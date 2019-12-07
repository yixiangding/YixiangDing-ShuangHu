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
/***************************************************
 * PendingReplicationBlocks does the bookkeeping of all
 * blocks that are getting replicated.
 *
 * It does the following:
 * 1)  record blocks that are getting replicated at this instant.
 * 2)  a coarse grain timer to track age of replication request
 * 3)  a thread that periodically identifies replication-requests
 *     that never made it.
 *
 ***************************************************/
package org.apache.hadoop.hdfs.server.namenode;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.*;
import java.io.*;
import java.util.*;
import java.sql.Time;

class PendingReplicationMonitor implements Runnable {
    public void run() {
      while (fsRunning) {
        long period = Math.min(defaultRecheckInterval, timeout);
        try {
          pendingReplicationCheck();
          Thread.sleep(period);
        } catch (InterruptedException ie) {
          FSNamesystem.LOG.debug(
                "PendingReplicationMonitor thread received exception. " + ie);
        }
      }
    }

    /**
     * Iterate through all items and detect timed-out items
     */
    void pendingReplicationCheck() {
      synchronized (pendingReplications) {
        Iterator iter = pendingReplications.entrySet().iterator();
        long now = FSNamesystem.now();
        FSNamesystem.LOG.debug("PendingReplicationMonitor checking Q");
        while (iter.hasNext()) {
          Map.Entry entry = (Map.Entry) iter.next();
          PendingBlockInfo pendingBlock = (PendingBlockInfo) entry.getValue();
          if (now > pendingBlock.getTimeStamp() + timeout) {
            Block block = (Block) entry.getKey();
            synchronized (timedOutItems) {
              timedOutItems.add(block);
            }
            FSNamesystem.LOG.warn(
                "PendingReplicationMonitor timed out block " + block);
            iter.remove();
          }
        }
      }
    }
  }