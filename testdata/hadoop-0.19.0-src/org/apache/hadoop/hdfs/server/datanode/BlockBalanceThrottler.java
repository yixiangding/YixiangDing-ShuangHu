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
/**
 * Server used for receiving/sending a block of data.
 * This is created to listen for requests from clients or 
 * other DataNodes.  This small server does not use the 
 * Hadoop IPC mechanism.
 */
package org.apache.hadoop.hdfs.server.datanode;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.balancer.Balancer;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;

static class BlockBalanceThrottler extends BlockTransferThrottler {
   private int numThreads;
   
   /**Constructor
    * 
    * @param bandwidth Total amount of bandwidth can be used for balancing 
    */
   private BlockBalanceThrottler(long bandwidth) {
     super(bandwidth);
     LOG.info("Balancing bandwith is "+ bandwidth + " bytes/s");
   }
   
   /** Check if the block move can start. 
    * 
    * Return true if the thread quota is not exceeded and 
    * the counter is incremented; False otherwise.
    */
   synchronized boolean acquire() {
     if (numThreads >= Balancer.MAX_NUM_CONCURRENT_MOVES) {
       return false;
     }
     numThreads++;
     return true;
   }
   
   /** Mark that the move is completed. The thread counter is decremented. */
   synchronized void release() {
     numThreads--;
   }
  }