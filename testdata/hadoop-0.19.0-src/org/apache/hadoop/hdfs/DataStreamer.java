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