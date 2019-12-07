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