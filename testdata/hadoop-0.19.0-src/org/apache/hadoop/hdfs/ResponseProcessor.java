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