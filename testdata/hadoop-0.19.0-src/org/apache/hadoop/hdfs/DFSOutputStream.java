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

class DFSOutputStream extends FSOutputSummer implements Syncable {
    private Socket s;
    boolean closed = false;
  
    private String src;
    private DataOutputStream blockStream;
    private DataInputStream blockReplyStream;
    private Block block;
    final private long blockSize;
    private DataChecksum checksum;
    private LinkedList<Packet> dataQueue = new LinkedList<Packet>();
    private LinkedList<Packet> ackQueue = new LinkedList<Packet>();
    private Packet currentPacket = null;
    private int maxPackets = 80; // each packet 64K, total 5MB
    // private int maxPackets = 1000; // each packet 64K, total 64MB
    private DataStreamer streamer = new DataStreamer();;
    private ResponseProcessor response = null;
    private long currentSeqno = 0;
    private long bytesCurBlock = 0; // bytes writen in current block
    private int packetSize = 0; // write packet size, including the header.
    private int chunksPerPacket = 0;
    private DatanodeInfo[] nodes = null; // list of targets for current block
    private volatile boolean hasError = false;
    private volatile int errorIndex = 0;
    private volatile IOException lastException = null;
    private long artificialSlowdown = 0;
    private long lastFlushOffset = -1; // offset when flush was invoked
    private boolean persistBlocks = false; // persist blocks on namenode
    private int recoveryErrorCount = 0; // number of times block recovery failed
    private int maxRecoveryErrorCount = 5; // try block recovery 5 times
    private volatile boolean appendChunk = false;   // appending to existing partial block

    private void setLastException(IOException e) {
      if (lastException == null) {
        lastException = e;
      }
    }
    
    private class Packet {
      ByteBuffer buffer;           // only one of buf and buffer is non-null
      byte[]  buf;
      long    seqno;               // sequencenumber of buffer in block
      long    offsetInBlock;       // offset in block
      boolean lastPacketInBlock;   // is this the last packet in block?
      int     numChunks;           // number of chunks currently in packet
      int     maxChunks;           // max chunks in packet
      int     dataStart;
      int     dataPos;
      int     checksumStart;
      int     checksumPos;      
  
      // create a new packet
      Packet(int pktSize, int chunksPerPkt, long offsetInBlock) {
        this.lastPacketInBlock = false;
        this.numChunks = 0;
        this.offsetInBlock = offsetInBlock;
        this.seqno = currentSeqno;
        currentSeqno++;
        
        buffer = null;
        buf = new byte[pktSize];
        
        checksumStart = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;
        checksumPos = checksumStart;
        dataStart = checksumStart + chunksPerPkt * checksum.getChecksumSize();
        dataPos = dataStart;
        maxChunks = chunksPerPkt;
      }

      void writeData(byte[] inarray, int off, int len) {
        if ( dataPos + len > buf.length) {
          throw new BufferOverflowException();
        }
        System.arraycopy(inarray, off, buf, dataPos, len);
        dataPos += len;
      }
  
      void  writeChecksum(byte[] inarray, int off, int len) {
        if (checksumPos + len > dataStart) {
          throw new BufferOverflowException();
        }
        System.arraycopy(inarray, off, buf, checksumPos, len);
        checksumPos += len;
      }
      
      /**
       * Returns ByteBuffer that contains one full packet, including header.
       */
      ByteBuffer getBuffer() {
        /* Once this is called, no more data can be added to the packet.
         * setting 'buf' to null ensures that.
         * This is called only when the packet is ready to be sent.
         */
        if (buffer != null) {
          return buffer;
        }
        
        //prepare the header and close any gap between checksum and data.
        
        int dataLen = dataPos - dataStart;
        int checksumLen = checksumPos - checksumStart;
        
        if (checksumPos != dataStart) {
          /* move the checksum to cover the gap.
           * This can happen for the last packet.
           */
          System.arraycopy(buf, checksumStart, buf, 
                           dataStart - checksumLen , checksumLen); 
        }
        
        int pktLen = SIZE_OF_INTEGER + dataLen + checksumLen;
        
        //normally dataStart == checksumPos, i.e., offset is zero.
        buffer = ByteBuffer.wrap(buf, dataStart - checksumPos,
                                 DataNode.PKT_HEADER_LEN + pktLen);
        buf = null;
        buffer.mark();
        
        /* write the header and data length.
         * The format is described in comment before DataNode.BlockSender
         */
        buffer.putInt(pktLen);  // pktSize
        buffer.putLong(offsetInBlock); 
        buffer.putLong(seqno);
        buffer.put((byte) ((lastPacketInBlock) ? 1 : 0));
        //end of pkt header
        buffer.putInt(dataLen); // actual data length, excluding checksum.
        
        buffer.reset();
        return buffer;
      }
    }
  
    //
    // The DataStreamer class is responsible for sending data packets to the
    // datanodes in the pipeline. It retrieves a new blockid and block locations
    // from the namenode, and starts streaming packets to the pipeline of
    // Datanodes. Every packet has a sequence number associated with
    // it. When all the packets for a block are sent out and acks for each
    // if them are received, the DataStreamer closes the current block.
    //
    private class DataStreamer extends Daemon {

      private volatile boolean closed = false;
  
      public void run() {

        while (!closed && clientRunning) {

          // if the Responder encountered an error, shutdown Responder
          if (hasError && response != null) {
            try {
              response.close();
              response.join();
              response = null;
            } catch (InterruptedException  e) {
            }
          }

          Packet one = null;
          synchronized (dataQueue) {

            // process IO errors if any
            boolean doSleep = processDatanodeError(hasError, false);

            // wait for a packet to be sent.
            while ((!closed && !hasError && clientRunning 
                   && dataQueue.size() == 0) || doSleep) {
              try {
                dataQueue.wait(1000);
              } catch (InterruptedException  e) {
              }
              doSleep = false;
            }
            if (closed || hasError || dataQueue.size() == 0 || !clientRunning) {
              continue;
            }

            try {
              // get packet to be sent.
              one = dataQueue.getFirst();
              long offsetInBlock = one.offsetInBlock;
  
              // get new block from namenode.
              if (blockStream == null) {
                LOG.debug("Allocating new block");
                nodes = nextBlockOutputStream(src); 
                this.setName("DataStreamer for file " + src +
                             " block " + block);
                response = new ResponseProcessor(nodes);
                response.start();
              }

              if (offsetInBlock >= blockSize) {
                throw new IOException("BlockSize " + blockSize +
                                      " is smaller than data size. " +
                                      " Offset of packet in block " + 
                                      offsetInBlock +
                                      " Aborting file " + src);
              }

              ByteBuffer buf = one.getBuffer();
              
              // move packet from dataQueue to ackQueue
              dataQueue.removeFirst();
              dataQueue.notifyAll();
              synchronized (ackQueue) {
                ackQueue.addLast(one);
                ackQueue.notifyAll();
              } 
              
              // write out data to remote datanode
              blockStream.write(buf.array(), buf.position(), buf.remaining());
              
              if (one.lastPacketInBlock) {
                blockStream.writeInt(0); // indicate end-of-block 
              }
              blockStream.flush();
              if (LOG.isDebugEnabled()) {
                LOG.debug("DataStreamer block " + block +
                          " wrote packet seqno:" + one.seqno +
                          " size:" + buf.remaining() +
                          " offsetInBlock:" + one.offsetInBlock + 
                          " lastPacketInBlock:" + one.lastPacketInBlock);
              }
            } catch (Throwable e) {
              LOG.warn("DataStreamer Exception: " + 
                       StringUtils.stringifyException(e));
              if (e instanceof IOException) {
                setLastException((IOException)e);
              }
              hasError = true;
            }
          }

          if (closed || hasError || !clientRunning) {
            continue;
          }

          // Is this block full?
          if (one.lastPacketInBlock) {
            synchronized (ackQueue) {
              while (!hasError && ackQueue.size() != 0 && clientRunning) {
                try {
                  ackQueue.wait();   // wait for acks to arrive from datanodes
                } catch (InterruptedException  e) {
                }
              }
            }
            LOG.debug("Closing old block " + block);
            this.setName("DataStreamer for file " + src);

            response.close();        // ignore all errors in Response
            try {
              response.join();
              response = null;
            } catch (InterruptedException  e) {
            }

            if (closed || hasError || !clientRunning) {
              continue;
            }

            synchronized (dataQueue) {
              try {
                blockStream.close();
                blockReplyStream.close();
              } catch (IOException e) {
              }
              nodes = null;
              response = null;
              blockStream = null;
              blockReplyStream = null;
            }
          }
          if (progress != null) { progress.progress(); }

          // This is used by unit test to trigger race conditions.
          if (artificialSlowdown != 0 && clientRunning) {
            try { 
              Thread.sleep(artificialSlowdown); 
            } catch (InterruptedException e) {}
          }
        }
      }

      // shutdown thread
      void close() {
        closed = true;
        synchronized (dataQueue) {
          dataQueue.notifyAll();
        }
        synchronized (ackQueue) {
          ackQueue.notifyAll();
        }
        this.interrupt();
      }
    }
                  
    //
    // Processes reponses from the datanodes.  A packet is removed 
    // from the ackQueue when its response arrives.
    //
    private class ResponseProcessor extends Thread {

      private volatile boolean closed = false;
      private DatanodeInfo[] targets = null;
      private boolean lastPacketInBlock = false;

      ResponseProcessor (DatanodeInfo[] targets) {
        this.targets = targets;
      }

      public void run() {

        this.setName("ResponseProcessor for block " + block);
  
        while (!closed && clientRunning && !lastPacketInBlock) {
          // process responses from datanodes.
          try {
            // verify seqno from datanode
            long seqno = blockReplyStream.readLong();
            LOG.debug("DFSClient received ack for seqno " + seqno);
            if (seqno == -1) {
              continue;
            } else if (seqno == -2) {
              // no nothing
            } else {
              Packet one = null;
              synchronized (ackQueue) {
                one = ackQueue.getFirst();
              }
              if (one.seqno != seqno) {
                throw new IOException("Responseprocessor: Expecting seqno " + 
                                      " for block " + block +
                                      one.seqno + " but received " + seqno);
              }
              lastPacketInBlock = one.lastPacketInBlock;
            }

            // processes response status from all datanodes.
            for (int i = 0; i < targets.length && clientRunning; i++) {
              short reply = blockReplyStream.readShort();
              if (reply != DataTransferProtocol.OP_STATUS_SUCCESS) {
                errorIndex = i; // first bad datanode
                throw new IOException("Bad response " + reply +
                                      " for block " + block +
                                      " from datanode " + 
                                      targets[i].getName());
              }
            }

            synchronized (ackQueue) {
              ackQueue.removeFirst();
              ackQueue.notifyAll();
            }
          } catch (Exception e) {
            if (!closed) {
              hasError = true;
              if (e instanceof IOException) {
                setLastException((IOException)e);
              }
              LOG.warn("DFSOutputStream ResponseProcessor exception " + 
                       " for block " + block +
                        StringUtils.stringifyException(e));
              closed = true;
            }
          }

          synchronized (dataQueue) {
            dataQueue.notifyAll();
          }
          synchronized (ackQueue) {
            ackQueue.notifyAll();
          }
        }
      }

      void close() {
        closed = true;
        this.interrupt();
      }
    }

    // If this stream has encountered any errors so far, shutdown 
    // threads and mark stream as closed. Returns true if we should
    // sleep for a while after returning from this call.
    //
    private boolean processDatanodeError(boolean hasError, boolean isAppend) {
      if (!hasError) {
        return false;
      }
      if (response != null) {
        LOG.info("Error Recovery for block " + block +
                 " waiting for responder to exit. ");
        return true;
      }
      if (errorIndex >= 0) {
        LOG.warn("Error Recovery for block " + block
            + " bad datanode[" + errorIndex + "] "
            + (nodes == null? "nodes == null": nodes[errorIndex].getName()));
      }

      if (blockStream != null) {
        try {
          blockStream.close();
          blockReplyStream.close();
        } catch (IOException e) {
        }
      }
      blockStream = null;
      blockReplyStream = null;

      // move packets from ack queue to front of the data queue
      synchronized (ackQueue) {
        dataQueue.addAll(0, ackQueue);
        ackQueue.clear();
      }

      boolean success = false;
      while (!success && clientRunning) {
        DatanodeInfo[] newnodes = null;
        if (nodes == null) {
          String msg = "Could not get block locations. Aborting...";
          LOG.warn(msg);
          setLastException(new IOException(msg));
          closed = true;
          if (streamer != null) streamer.close();
          return false;
        }
        StringBuilder pipelineMsg = new StringBuilder();
        for (int j = 0; j < nodes.length; j++) {
          pipelineMsg.append(nodes[j].getName());
          if (j < nodes.length - 1) {
            pipelineMsg.append(", ");
          }
        }
        // remove bad datanode from list of datanodes.
        // If errorIndex was not set (i.e. appends), then do not remove 
        // any datanodes
        // 
        if (errorIndex < 0) {
          newnodes = nodes;
        } else {
          if (nodes.length <= 1) {
            lastException = new IOException("All datanodes " + pipelineMsg + 
                                            " are bad. Aborting...");
            closed = true;
            if (streamer != null) streamer.close();
            return false;
          }
          LOG.warn("Error Recovery for block " + block +
                   " in pipeline " + pipelineMsg + 
                   ": bad datanode " + nodes[errorIndex].getName());
          newnodes =  new DatanodeInfo[nodes.length-1];
          System.arraycopy(nodes, 0, newnodes, 0, errorIndex);
          System.arraycopy(nodes, errorIndex+1, newnodes, errorIndex,
              newnodes.length-errorIndex);
        }

        // Tell the primary datanode to do error recovery 
        // by stamping appropriate generation stamps.
        //
        LocatedBlock newBlock = null;
        ClientDatanodeProtocol primary =  null;
        DatanodeInfo primaryNode = null;
        try {
          // Pick the "least" datanode as the primary datanode to avoid deadlock.
          primaryNode = Collections.min(Arrays.asList(newnodes));
          primary = createClientDatanodeProtocolProxy(primaryNode, conf);
          newBlock = primary.recoverBlock(block, isAppend, newnodes);
        } catch (IOException e) {
          recoveryErrorCount++;
          if (recoveryErrorCount > maxRecoveryErrorCount) {
            if (nodes.length > 1) {
              // if the primary datanode failed, remove it from the list.
              // The original bad datanode is left in the list because it is
              // conservative to remove only one datanode in one iteration.
              for (int j = 0; j < nodes.length; j++) {
                if (nodes[j] ==  primaryNode) {
                  errorIndex = j; // forget original bad node.
                }
              }
              LOG.warn("Error Recovery for block " + block + " failed " +
                       " because recovery from primary datanode " +
                       primaryNode + " failed " + recoveryErrorCount +
                       " times. Marking primary datanode as bad.");
              recoveryErrorCount = 0; 
              return true;          // sleep when we return from here
            }
            String emsg = "Error Recovery for block " + block + " failed " +
                          " because recovery from primary datanode " +
                          primaryNode + " failed " + recoveryErrorCount + 
                          " times. Aborting...";
            LOG.warn(emsg);
            lastException = new IOException(emsg);
            closed = true;
            if (streamer != null) streamer.close();
            return false;       // abort with IOexception
          } 
          LOG.warn("Error Recovery for block " + block + " failed " +
                   " because recovery from primary datanode " +
                   primaryNode + " failed " + recoveryErrorCount +
                   " times. Will retry...");
          return true;          // sleep when we return from here
        } finally {
          RPC.stopProxy(primary);
        }
        recoveryErrorCount = 0; // block recovery successful

        // If the block recovery generated a new generation stamp, use that
        // from now on.  Also, setup new pipeline
        //
        if (newBlock != null) {
          block = newBlock.getBlock();
          nodes = newBlock.getLocations();
        }

        this.hasError = false;
        lastException = null;
        errorIndex = 0;
        success = createBlockOutputStream(nodes, clientName, true);
      }

      response = new ResponseProcessor(nodes);
      response.start();
      return false; // do not sleep, continue processing
    }

    private void isClosed() throws IOException {
      if (closed) {
        if (lastException != null) {
          throw lastException;
        } else {
          throw new IOException("Stream closed.");
        }
      }
    }

    //
    // returns the list of targets, if any, that is being currently used.
    //
    DatanodeInfo[] getPipeline() {
      synchronized (dataQueue) {
        if (nodes == null) {
          return null;
        }
        DatanodeInfo[] value = new DatanodeInfo[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
          value[i] = nodes[i];
        }
        return value;
      }
    }

    private Progressable progress;

    private DFSOutputStream(String src, long blockSize, Progressable progress,
        int bytesPerChecksum) throws IOException {
      super(new CRC32(), bytesPerChecksum, 4);
      this.src = src;
      this.blockSize = blockSize;
      this.progress = progress;
      if (progress != null) {
        LOG.debug("Set non-null progress callback on DFSOutputStream "+src);
      }
      
      if ( bytesPerChecksum < 1 || blockSize % bytesPerChecksum != 0) {
        throw new IOException("io.bytes.per.checksum(" + bytesPerChecksum +
                              ") and blockSize(" + blockSize + 
                              ") do not match. " + "blockSize should be a " +
                              "multiple of io.bytes.per.checksum");
                              
      }
      checksum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, 
                                              bytesPerChecksum);
    }

    /**
     * Create a new output stream to the given DataNode.
     * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
     */
    DFSOutputStream(String src, FsPermission masked, boolean overwrite,
        short replication, long blockSize, Progressable progress,
        int buffersize, int bytesPerChecksum) throws IOException {
      this(src, blockSize, progress, bytesPerChecksum);

      computePacketChunkSize(writePacketSize, bytesPerChecksum);

      try {
        namenode.create(
            src, masked, clientName, overwrite, replication, blockSize);
      } catch(RemoteException re) {
        throw re.unwrapRemoteException(AccessControlException.class,
                                       QuotaExceededException.class);
      }
      streamer.start();
    }
  
    /**
     * Create a new output stream to the given DataNode.
     * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
     */
    DFSOutputStream(String src, int buffersize, Progressable progress,
        LocatedBlock lastBlock, FileStatus stat,
        int bytesPerChecksum) throws IOException {
      this(src, stat.getBlockSize(), progress, bytesPerChecksum);

      //
      // The last partial block of the file has to be filled.
      //
      if (lastBlock != null) {
        block = lastBlock.getBlock();
        long usedInLastBlock = stat.getLen() % blockSize;
        int freeInLastBlock = (int)(blockSize - usedInLastBlock);

        // calculate the amount of free space in the pre-existing 
        // last crc chunk
        int usedInCksum = (int)(stat.getLen() % bytesPerChecksum);
        int freeInCksum = bytesPerChecksum - usedInCksum;

        // if there is space in the last block, then we have to 
        // append to that block
        if (freeInLastBlock > blockSize) {
          throw new IOException("The last block for file " + 
                                src + " is full.");
        }

        // indicate that we are appending to an existing block
        bytesCurBlock = lastBlock.getBlockSize();

        if (usedInCksum > 0 && freeInCksum > 0) {
          // if there is space in the last partial chunk, then 
          // setup in such a way that the next packet will have only 
          // one chunk that fills up the partial chunk.
          //
          computePacketChunkSize(0, freeInCksum);
          resetChecksumChunk(freeInCksum);
          this.appendChunk = true;
        } else {
          // if the remaining space in the block is smaller than 
          // that expected size of of a packet, then create 
          // smaller size packet.
          //
          computePacketChunkSize(Math.min(writePacketSize, freeInLastBlock), 
                                 bytesPerChecksum);
        }

        // setup pipeline to append to the last block XXX retries??
        nodes = lastBlock.getLocations();
        errorIndex = -1;   // no errors yet.
        if (nodes.length < 1) {
          throw new IOException("Unable to retrieve blocks locations " +
                                " for last block " + block +
                                "of file " + src);
                        
        }
        processDatanodeError(true, true);
        streamer.start();
      }
      else {
        computePacketChunkSize(writePacketSize, bytesPerChecksum);
        streamer.start();
      }
    }

    private void computePacketChunkSize(int psize, int csize) {
      int chunkSize = csize + checksum.getChecksumSize();
      int n = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER;
      chunksPerPacket = Math.max((psize - n + chunkSize-1)/chunkSize, 1);
      packetSize = n + chunkSize*chunksPerPacket;
      if (LOG.isDebugEnabled()) {
        LOG.debug("computePacketChunkSize: src=" + src +
                  ", chunkSize=" + chunkSize +
                  ", chunksPerPacket=" + chunksPerPacket +
                  ", packetSize=" + packetSize);
      }
    }

    /**
     * Open a DataOutputStream to a DataNode so that it can be written to.
     * This happens when a file is created and each time a new block is allocated.
     * Must get block ID and the IDs of the destinations from the namenode.
     * Returns the list of target datanodes.
     */
    private DatanodeInfo[] nextBlockOutputStream(String client) throws IOException {
      LocatedBlock lb = null;
      boolean retry = false;
      DatanodeInfo[] nodes;
      int count = conf.getInt("dfs.client.block.write.retries", 3);
      boolean success;
      do {
        hasError = false;
        lastException = null;
        errorIndex = 0;
        retry = false;
        nodes = null;
        success = false;
                
        long startTime = System.currentTimeMillis();
        lb = locateFollowingBlock(startTime);
        block = lb.getBlock();
        nodes = lb.getLocations();
  
        //
        // Connect to first DataNode in the list.
        //
        success = createBlockOutputStream(nodes, clientName, false);

        if (!success) {
          LOG.info("Abandoning block " + block);
          namenode.abandonBlock(block, src, clientName);

          // Connection failed.  Let's wait a little bit and retry
          retry = true;
          try {
            if (System.currentTimeMillis() - startTime > 5000) {
              LOG.info("Waiting to find target node: " + nodes[0].getName());
            }
            Thread.sleep(6000);
          } catch (InterruptedException iex) {
          }
        }
      } while (retry && --count >= 0);

      if (!success) {
        throw new IOException("Unable to create new block.");
      }
      return nodes;
    }

    // connects to the first datanode in the pipeline
    // Returns true if success, otherwise return failure.
    //
    private boolean createBlockOutputStream(DatanodeInfo[] nodes, String client,
                    boolean recoveryFlag) {
      String firstBadLink = "";
      if (LOG.isDebugEnabled()) {
        for (int i = 0; i < nodes.length; i++) {
          LOG.debug("pipeline = " + nodes[i].getName());
        }
      }

      // persist blocks on namenode on next flush
      persistBlocks = true;

      try {
        LOG.debug("Connecting to " + nodes[0].getName());
        InetSocketAddress target = NetUtils.createSocketAddr(nodes[0].getName());
        s = socketFactory.createSocket();
        int timeoutValue = 3000 * nodes.length + socketTimeout;
        s.connect(target, timeoutValue);
        s.setSoTimeout(timeoutValue);
        s.setSendBufferSize(DEFAULT_DATA_SOCKET_SIZE);
        LOG.debug("Send buf size " + s.getSendBufferSize());
        long writeTimeout = HdfsConstants.WRITE_TIMEOUT_EXTENSION * nodes.length +
                            datanodeWriteTimeout;

        //
        // Xmit header info to datanode
        //
        DataOutputStream out = new DataOutputStream(
            new BufferedOutputStream(NetUtils.getOutputStream(s, writeTimeout), 
                                     DataNode.SMALL_BUFFER_SIZE));
        blockReplyStream = new DataInputStream(NetUtils.getInputStream(s));

        out.writeShort( DataTransferProtocol.DATA_TRANSFER_VERSION );
        out.write( DataTransferProtocol.OP_WRITE_BLOCK );
        out.writeLong( block.getBlockId() );
        out.writeLong( block.getGenerationStamp() );
        out.writeInt( nodes.length );
        out.writeBoolean( recoveryFlag );       // recovery flag
        Text.writeString( out, client );
        out.writeBoolean(false); // Not sending src node information
        out.writeInt( nodes.length - 1 );
        for (int i = 1; i < nodes.length; i++) {
          nodes[i].write(out);
        }
        checksum.writeHeader( out );
        out.flush();

        // receive ack for connect
        firstBadLink = Text.readString(blockReplyStream);
        if (firstBadLink.length() != 0) {
          throw new IOException("Bad connect ack with firstBadLink " + firstBadLink);
        }

        blockStream = out;
        return true;     // success

      } catch (IOException ie) {

        LOG.info("Exception in createBlockOutputStream " + ie);

        // find the datanode that matches
        if (firstBadLink.length() != 0) {
          for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].getName().equals(firstBadLink)) {
              errorIndex = i;
              break;
            }
          }
        }
        hasError = true;
        setLastException(ie);
        blockReplyStream = null;
        return false;  // error
      }
    }
  
    private LocatedBlock locateFollowingBlock(long start
                                              ) throws IOException {     
      int retries = 5;
      long sleeptime = 400;
      while (true) {
        long localstart = System.currentTimeMillis();
        while (true) {
          try {
            return namenode.addBlock(src, clientName);
          } catch (RemoteException e) {
            IOException ue = 
              e.unwrapRemoteException(FileNotFoundException.class,
                                      AccessControlException.class,
                                      QuotaExceededException.class);
            if (ue != e) { 
              throw ue; // no need to retry these exceptions
            }
            
            if (--retries == 0 && 
                !NotReplicatedYetException.class.getName().
                equals(e.getClassName())) {
              throw e;
            } else {
              LOG.info(StringUtils.stringifyException(e));
              if (System.currentTimeMillis() - localstart > 5000) {
                LOG.info("Waiting for replication for " + 
                         (System.currentTimeMillis() - localstart)/1000 + 
                         " seconds");
              }
              try {
                LOG.warn("NotReplicatedYetException sleeping " + src +
                          " retries left " + retries);
                Thread.sleep(sleeptime);
                sleeptime *= 2;
              } catch (InterruptedException ie) {
              }
            }                
          }
        }
      } 
    }
  
    // @see FSOutputSummer#writeChunk()
    @Override
    protected synchronized void writeChunk(byte[] b, int offset, int len, byte[] checksum) 
                                                          throws IOException {
      checkOpen();
      isClosed();
  
      int cklen = checksum.length;
      int bytesPerChecksum = this.checksum.getBytesPerChecksum(); 
      if (len > bytesPerChecksum) {
        throw new IOException("writeChunk() buffer size is " + len +
                              " is larger than supported  bytesPerChecksum " +
                              bytesPerChecksum);
      }
      if (checksum.length != this.checksum.getChecksumSize()) {
        throw new IOException("writeChunk() checksum size is supposed to be " +
                              this.checksum.getChecksumSize() + 
                              " but found to be " + checksum.length);
      }

      synchronized (dataQueue) {
  
        // If queue is full, then wait till we can create  enough space
        while (!closed && dataQueue.size() + ackQueue.size()  > maxPackets) {
          try {
            dataQueue.wait();
          } catch (InterruptedException  e) {
          }
        }
        isClosed();
  
        if (currentPacket == null) {
          currentPacket = new Packet(packetSize, chunksPerPacket, 
                                     bytesCurBlock);
          if (LOG.isDebugEnabled()) {
            LOG.debug("DFSClient writeChunk allocating new packet seqno=" + 
                      currentPacket.seqno +
                      ", src=" + src +
                      ", packetSize=" + packetSize +
                      ", chunksPerPacket=" + chunksPerPacket +
                      ", bytesCurBlock=" + bytesCurBlock);
          }
        }

        currentPacket.writeChecksum(checksum, 0, cklen);
        currentPacket.writeData(b, offset, len);
        currentPacket.numChunks++;
        bytesCurBlock += len;

        // If packet is full, enqueue it for transmission
        //
        if (currentPacket.numChunks == currentPacket.maxChunks ||
            bytesCurBlock == blockSize) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("DFSClient writeChunk packet full seqno=" +
                      currentPacket.seqno +
                      ", src=" + src +
                      ", bytesCurBlock=" + bytesCurBlock +
                      ", blockSize=" + blockSize +
                      ", appendChunk=" + appendChunk);
          }
          //
          // if we allocated a new packet because we encountered a block
          // boundary, reset bytesCurBlock.
          //
          if (bytesCurBlock == blockSize) {
            currentPacket.lastPacketInBlock = true;
            bytesCurBlock = 0;
            lastFlushOffset = -1;
          }
          dataQueue.addLast(currentPacket);
          dataQueue.notifyAll();
          currentPacket = null;
 
          // If this was the first write after reopening a file, then the above
          // write filled up any partial chunk. Tell the summer to generate full 
          // crc chunks from now on.
          if (appendChunk) {
            appendChunk = false;
            resetChecksumChunk(bytesPerChecksum);
          }
          int psize = Math.min((int)(blockSize-bytesCurBlock), writePacketSize);
          computePacketChunkSize(psize, bytesPerChecksum);
        }
      }
      //LOG.debug("DFSClient writeChunk done length " + len +
      //          " checksum length " + cklen);
    }
  
    /**
     * All data is written out to datanodes. It is not guaranteed 
     * that data has been flushed to persistent store on the 
     * datanode. Block allocations are persisted on namenode.
     */
    public synchronized void sync() throws IOException {
      try {
        /* Record current blockOffset. This might be changed inside
         * flushBuffer() where a partial checksum chunk might be flushed.
         * After the flush, reset the bytesCurBlock back to its previous value,
         * any partial checksum chunk will be sent now and in next packet.
         */
        long saveOffset = bytesCurBlock;

        // flush checksum buffer, but keep checksum buffer intact
        flushBuffer(true);

        LOG.debug("DFSClient flush() : saveOffset " + saveOffset +  
                  " bytesCurBlock " + bytesCurBlock +
                  " lastFlushOffset " + lastFlushOffset);
        
        // Flush only if we haven't already flushed till this offset.
        if (lastFlushOffset != bytesCurBlock) {

          // record the valid offset of this flush
          lastFlushOffset = bytesCurBlock;

          // wait for all packets to be sent and acknowledged
          flushInternal();
        } else {
          // just discard the current packet since it is already been sent.
          currentPacket = null;
        }
        
        // Restore state of stream. Record the last flush offset 
        // of the last full chunk that was flushed.
        //
        bytesCurBlock = saveOffset;

        // If any new blocks were allocated since the last flush, 
        // then persist block locations on namenode. 
        //
        if (persistBlocks) {
          namenode.fsync(src, clientName);
          persistBlocks = false;
        }
      } catch (IOException e) {
          lastException = new IOException("IOException flush:" + e);
          closed = true;
          closeThreads();
          throw e;
      }
    }

    /**
     * Waits till all existing data is flushed and confirmations 
     * received from datanodes. 
     */
    private synchronized void flushInternal() throws IOException {
      checkOpen();
      isClosed();

      while (!closed) {
        synchronized (dataQueue) {
          isClosed();
          //
          // If there is data in the current buffer, send it across
          //
          if (currentPacket != null) {
            dataQueue.addLast(currentPacket);
            dataQueue.notifyAll();
            currentPacket = null;
          }

          // wait for all buffers to be flushed to datanodes
          if (!closed && dataQueue.size() != 0) {
            try {
              dataQueue.wait();
            } catch (InterruptedException e) {
            }
            continue;
          }
        }

        // wait for all acks to be received back from datanodes
        synchronized (ackQueue) {
          if (!closed && ackQueue.size() != 0) {
            try {
              ackQueue.wait();
            } catch (InterruptedException e) {
            }
            continue;
          }
        }

        // acquire both the locks and verify that we are
        // *really done*. In the case of error recovery, 
        // packets might move back from ackQueue to dataQueue.
        //
        synchronized (dataQueue) {
          synchronized (ackQueue) {
            if (dataQueue.size() + ackQueue.size() == 0) {
              break;       // we are done
            }
          }
        }
      }
    }
  
    /**
     * Closes this output stream and releases any system 
     * resources associated with this stream.
     */
    @Override
    public void close() throws IOException {
      closeInternal();
      leasechecker.remove(src);
      
      if (s != null) {
        s.close();
        s = null;
      }
    }
 
    // shutdown datastreamer and responseprocessor threads.
    private void closeThreads() throws IOException {
      try {
        streamer.close();
        streamer.join();
        
        // shutdown response after streamer has exited.
        if (response != null) {
          response.close();
          response.join();
          response = null;
        }
      } catch (InterruptedException e) {
        throw new IOException("Failed to shutdown response thread");
      }
    }
    
    /**
     * Closes this output stream and releases any system 
     * resources associated with this stream.
     */
    private synchronized void closeInternal() throws IOException {
      checkOpen();
      isClosed();

      try {
          flushBuffer();       // flush from all upper layers
      
          // Mark that this packet is the last packet in block.
          // If there are no outstanding packets and the last packet
          // was not the last one in the current block, then create a
          // packet with empty payload.
          synchronized (dataQueue) {
            if (currentPacket == null && bytesCurBlock != 0) {
              currentPacket = new Packet(packetSize, chunksPerPacket,
                                         bytesCurBlock);
            }
            if (currentPacket != null) { 
              currentPacket.lastPacketInBlock = true;
            }
          }

        flushInternal();             // flush all data to Datanodes
        isClosed(); // check to see if flushInternal had any exceptions
        closed = true; // allow closeThreads() to showdown threads

        closeThreads();
        
        synchronized (dataQueue) {
          if (blockStream != null) {
            blockStream.writeInt(0); // indicate end-of-block to datanode
            blockStream.close();
            blockReplyStream.close();
          }
          if (s != null) {
            s.close();
            s = null;
          }
        }

        streamer = null;
        blockStream = null;
        blockReplyStream = null;

        long localstart = System.currentTimeMillis();
        boolean fileComplete = false;
        while (!fileComplete) {
          fileComplete = namenode.complete(src, clientName);
          if (!fileComplete) {
            try {
              Thread.sleep(400);
              if (System.currentTimeMillis() - localstart > 5000) {
                LOG.info("Could not complete file " + src + " retrying...");
              }
            } catch (InterruptedException ie) {
            }
          }
        }
      } finally {
        closed = true;
      }
    }

    void setArtificialSlowdown(long period) {
      artificialSlowdown = period;
    }

    synchronized void setChunksPerPacket(int value) {
      chunksPerPacket = Math.min(chunksPerPacket, value);
      packetSize = DataNode.PKT_HEADER_LEN + SIZE_OF_INTEGER +
                   (checksum.getBytesPerChecksum() + 
                    checksum.getChecksumSize()) * chunksPerPacket;
    }

    synchronized void setTestFilename(String newname) {
      src = newname;
    }
  }