/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/********************************************************
 * DFSClient can connect to a Hadoop Filesystem and 
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects 
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of 
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 ********************************************************/
package org.apache.hadoop.hdfs;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.*;
import org.apache.commons.logging.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.CRC32;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import javax.net.SocketFactory;
import javax.security.auth.login.LoginException;

public static class BlockReader extends FSInputChecker {

    private Socket dnSock; //for now just sending checksumOk.
    private DataInputStream in;
    private DataChecksum checksum;
    private long lastChunkOffset = -1;
    private long lastChunkLen = -1;
    private long lastSeqNo = -1;

    private long startOffset;
    private long firstChunkOffset;
    private int bytesPerChecksum;
    private int checksumSize;
    private boolean gotEOS = false;
    private boolean sentChecksumOk = false; //temp : to be removed in 0.20.0
    
    byte[] skipBuf = null;
    ByteBuffer checksumBytes = null;
    int dataLeft = 0;
    boolean isLastPacket = false;
    
    /* FSInputChecker interface */
    
    /* same interface as inputStream java.io.InputStream#read()
     * used by DFSInputStream#read()
     * This violates one rule when there is a checksum error:
     * "Read should not modify user buffer before successful read"
     * because it first reads the data to user buffer and then checks
     * the checksum.
     */
    @Override
    public synchronized int read(byte[] buf, int off, int len) 
                                 throws IOException {
      
      //for the first read, skip the extra bytes at the front.
      if (lastChunkLen < 0 && startOffset > firstChunkOffset && len > 0) {
        // Skip these bytes. But don't call this.skip()!
        int toSkip = (int)(startOffset - firstChunkOffset);
        if ( skipBuf == null ) {
          skipBuf = new byte[bytesPerChecksum];
        }
        if ( super.read(skipBuf, 0, toSkip) != toSkip ) {
          // should never happen
          throw new IOException("Could not skip required number of bytes");
        }
      }
      
      boolean eosBefore = gotEOS;
      int nRead = super.read(buf, off, len);
      
      // if gotEOS was set in the previous read and checksum is enabled :
      if (gotEOS && !eosBefore && nRead >= 0 && needChecksum()) {
        if (sentChecksumOk) {
           // this should not happen; log the error for the debugging purpose
           LOG.info(StringUtils.stringifyException(new IOException(
             "Checksum ok was sent and should not be sent again"))); 
        } else {
          //checksum is verified and there are no errors.
          checksumOk(dnSock);
          sentChecksumOk = true;
       }
      }
      return nRead;
    }

    @Override
    public synchronized long skip(long n) throws IOException {
      /* How can we make sure we don't throw a ChecksumException, at least
       * in majority of the cases?. This one throws. */  
      if ( skipBuf == null ) {
        skipBuf = new byte[bytesPerChecksum]; 
      }

      long nSkipped = 0;
      while ( nSkipped < n ) {
        int toSkip = (int)Math.min(n-nSkipped, skipBuf.length);
        int ret = read(skipBuf, 0, toSkip);
        if ( ret <= 0 ) {
          return nSkipped;
        }
        nSkipped += ret;
      }
      return nSkipped;
    }

    @Override
    public int read() throws IOException {
      throw new IOException("read() is not expected to be invoked. " +
                            "Use read(buf, off, len) instead.");
    }
    
    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      /* Checksum errors are handled outside the BlockReader. 
       * DFSInputStream does not always call 'seekToNewSource'. In the 
       * case of pread(), it just tries a different replica without seeking.
       */ 
      return false;
    }
    
    @Override
    public void seek(long pos) throws IOException {
      throw new IOException("Seek() is not supported in BlockInputChecker");
    }

    @Override
    protected long getChunkPosition(long pos) {
      throw new RuntimeException("getChunkPosition() is not supported, " +
                                 "since seek is not required");
    }
    
    /**
     * Makes sure that checksumBytes has enough capacity 
     * and limit is set to the number of checksum bytes needed 
     * to be read.
     */
    private void adjustChecksumBytes(int dataLen) {
      int requiredSize = 
        ((dataLen + bytesPerChecksum - 1)/bytesPerChecksum)*checksumSize;
      if (checksumBytes == null || requiredSize > checksumBytes.capacity()) {
        checksumBytes =  ByteBuffer.wrap(new byte[requiredSize]);
      } else {
        checksumBytes.clear();
      }
      checksumBytes.limit(requiredSize);
    }
    
    @Override
    protected synchronized int readChunk(long pos, byte[] buf, int offset, 
                                         int len, byte[] checksumBuf) 
                                         throws IOException {
      // Read one chunk.
      
      if ( gotEOS ) {
        if ( startOffset < 0 ) {
          //This is mainly for debugging. can be removed.
          throw new IOException( "BlockRead: already got EOS or an error" );
        }
        startOffset = -1;
        return -1;
      }
      
      // Read one DATA_CHUNK.
      long chunkOffset = lastChunkOffset;
      if ( lastChunkLen > 0 ) {
        chunkOffset += lastChunkLen;
      }
      
      if ( (pos + firstChunkOffset) != chunkOffset ) {
        throw new IOException("Mismatch in pos : " + pos + " + " + 
                              firstChunkOffset + " != " + chunkOffset);
      }

      // Read next packet if the previous packet has been read completely.
      if (dataLeft <= 0) {
        //Read packet headers.
        int packetLen = in.readInt();
        long offsetInBlock = in.readLong();
        long seqno = in.readLong();
        boolean lastPacketInBlock = in.readBoolean();
      
        if (LOG.isDebugEnabled()) {
          LOG.debug("DFSClient readChunk got seqno " + seqno +
                    " offsetInBlock " + offsetInBlock +
                    " lastPacketInBlock " + lastPacketInBlock +
                    " packetLen " + packetLen);
        }
        
        int dataLen = in.readInt();
      
        // Sanity check the lengths
        if ( dataLen < 0 || 
             ( (dataLen % bytesPerChecksum) != 0 && !lastPacketInBlock ) ||
             (seqno != (lastSeqNo + 1)) ) {
             throw new IOException("BlockReader: error in packet header" +
                                   "(chunkOffset : " + chunkOffset + 
                                   ", dataLen : " + dataLen +
                                   ", seqno : " + seqno + 
                                   " (last: " + lastSeqNo + "))");
        }
        
        lastSeqNo = seqno;
        isLastPacket = lastPacketInBlock;
        dataLeft = dataLen;
        adjustChecksumBytes(dataLen);
        if (dataLen > 0) {
          IOUtils.readFully(in, checksumBytes.array(), 0,
                            checksumBytes.limit());
        }
      }

      int chunkLen = Math.min(dataLeft, bytesPerChecksum);
      
      if ( chunkLen > 0 ) {
        // len should be >= chunkLen
        IOUtils.readFully(in, buf, offset, chunkLen);
        checksumBytes.get(checksumBuf, 0, checksumSize);
      }
      
      dataLeft -= chunkLen;
      lastChunkOffset = chunkOffset;
      lastChunkLen = chunkLen;
      
      if ((dataLeft == 0 && isLastPacket) || chunkLen == 0) {
        gotEOS = true;
      }
      if ( chunkLen == 0 ) {
        return -1;
      }
      
      return chunkLen;
    }
    
    private BlockReader( String file, long blockId, DataInputStream in, 
                         DataChecksum checksum, boolean verifyChecksum,
                         long startOffset, long firstChunkOffset, 
                         Socket dnSock ) {
      super(new Path("/blk_" + blockId + ":of:" + file)/*too non path-like?*/,
            1, verifyChecksum,
            checksum.getChecksumSize() > 0? checksum : null, 
            checksum.getBytesPerChecksum(),
            checksum.getChecksumSize());
      
      this.dnSock = dnSock;
      this.in = in;
      this.checksum = checksum;
      this.startOffset = Math.max( startOffset, 0 );

      this.firstChunkOffset = firstChunkOffset;
      lastChunkOffset = firstChunkOffset;
      lastChunkLen = -1;

      bytesPerChecksum = this.checksum.getBytesPerChecksum();
      checksumSize = this.checksum.getChecksumSize();
    }

    public static BlockReader newBlockReader(Socket sock, String file, long blockId, 
        long genStamp, long startOffset, long len, int bufferSize) throws IOException {
      return newBlockReader(sock, file, blockId, genStamp, startOffset, len, bufferSize,
          true);
    }

    /** Java Doc required */
    public static BlockReader newBlockReader( Socket sock, String file, long blockId, 
                                       long genStamp,
                                       long startOffset, long len,
                                       int bufferSize, boolean verifyChecksum)
                                       throws IOException {
      return newBlockReader(sock, file, blockId, genStamp, startOffset,
                            len, bufferSize, verifyChecksum, "");
    }

    public static BlockReader newBlockReader( Socket sock, String file,
                                       long blockId, 
                                       long genStamp,
                                       long startOffset, long len,
                                       int bufferSize, boolean verifyChecksum,
                                       String clientName)
                                       throws IOException {
      // in and out will be closed when sock is closed (by the caller)
      DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(NetUtils.getOutputStream(sock,HdfsConstants.WRITE_TIMEOUT)));

      //write the header.
      out.writeShort( DataTransferProtocol.DATA_TRANSFER_VERSION );
      out.write( DataTransferProtocol.OP_READ_BLOCK );
      out.writeLong( blockId );
      out.writeLong( genStamp );
      out.writeLong( startOffset );
      out.writeLong( len );
      Text.writeString(out, clientName);
      out.flush();
      
      //
      // Get bytes in block, set streams
      //

      DataInputStream in = new DataInputStream(
          new BufferedInputStream(NetUtils.getInputStream(sock), 
                                  bufferSize));
      
      if ( in.readShort() != DataTransferProtocol.OP_STATUS_SUCCESS ) {
        throw new IOException("Got error in response to OP_READ_BLOCK " +
                              "for file " + file + 
                              " for block " + blockId);
      }
      DataChecksum checksum = DataChecksum.newDataChecksum( in );
      //Warning when we get CHECKSUM_NULL?
      
      // Read the first chunk offset.
      long firstChunkOffset = in.readLong();
      
      if ( firstChunkOffset < 0 || firstChunkOffset > startOffset ||
          firstChunkOffset >= (startOffset + checksum.getBytesPerChecksum())) {
        throw new IOException("BlockReader: error in first chunk offset (" +
                              firstChunkOffset + ") startOffset is " + 
                              startOffset + " for file " + file);
      }

      return new BlockReader( file, blockId, in, checksum, verifyChecksum,
                              startOffset, firstChunkOffset, sock );
    }

    @Override
    public synchronized void close() throws IOException {
      startOffset = -1;
      checksum = null;
      // in will be closed when its Socket is closed.
    }
    
    /** kind of like readFully(). Only reads as much as possible.
     * And allows use of protected readFully().
     */
    public int readAll(byte[] buf, int offset, int len) throws IOException {
      return readFully(this, buf, offset, len);
    }
    
    /* When the reader reaches end of a block and there are no checksum
     * errors, we send OP_STATUS_CHECKSUM_OK to datanode to inform that 
     * checksum was verified and there was no error.
     */ 
    private void checksumOk(Socket sock) {
      try {
        OutputStream out = NetUtils.getOutputStream(sock, HdfsConstants.WRITE_TIMEOUT);
        byte buf[] = { (DataTransferProtocol.OP_STATUS_CHECKSUM_OK >>> 8) & 0xff,
                       (DataTransferProtocol.OP_STATUS_CHECKSUM_OK) & 0xff };
        out.write(buf);
        out.flush();
      } catch (IOException e) {
        // its ok not to be able to send this.
        LOG.debug("Could not write to datanode " + sock.getInetAddress() +
                  ": " + e.getMessage());
      }
    }
  }