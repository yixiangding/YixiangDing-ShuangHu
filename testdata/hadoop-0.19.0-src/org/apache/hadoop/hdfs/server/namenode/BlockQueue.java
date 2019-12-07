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
/**************************************************
 * DatanodeDescriptor tracks stats on a given DataNode,
 * such as available storage capacity, last update time, etc.,
 * and maintains a set of blocks stored on the datanode. 
 *
 * This data structure is a data structure that is internal
 * to the namenode. It is *not* sent over-the-wire to the Client
 * or the Datnodes. Neither is it stored persistently in the
 * fsImage.

 **************************************************/
package org.apache.hadoop.hdfs.server.namenode;
import java.io.DataInput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.protocol.BlockCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableUtils;

private static class BlockQueue {
    private final Queue<BlockTargetPair> blockq = new LinkedList<BlockTargetPair>();

    /** Size of the queue */
    synchronized int size() {return blockq.size();}

    /** Enqueue */
    synchronized boolean offer(Block block, DatanodeDescriptor[] targets) { 
      return blockq.offer(new BlockTargetPair(block, targets));
    }

    /** Dequeue */
    synchronized List<BlockTargetPair> poll(int numTargets) {
      if (numTargets <= 0 || blockq.isEmpty()) {
        return null;
      }
      else {
        List<BlockTargetPair> results = new ArrayList<BlockTargetPair>();
        for(; !blockq.isEmpty() && numTargets > 0; ) {
          numTargets -= blockq.peek().targets.length; 
          if (numTargets >= 0) {
            results.add(blockq.poll());
          }
        }
        return results;
      }
    }
  }