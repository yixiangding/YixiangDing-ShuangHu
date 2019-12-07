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

static class PendingBlockInfo {
    private long timeStamp;
    private int numReplicasInProgress;

    PendingBlockInfo(int numReplicas) {
      this.timeStamp = FSNamesystem.now();
      this.numReplicasInProgress = numReplicas;
    }

    long getTimeStamp() {
      return timeStamp;
    }

    void setTimeStamp() {
      timeStamp = FSNamesystem.now();
    }

    void incrementReplicas(int increment) {
      numReplicasInProgress += increment;
    }

    void decrementReplicas() {
      numReplicasInProgress--;
      assert(numReplicasInProgress >= 0);
    }

    int getNumReplicas() {
      return numReplicasInProgress;
    }
  }