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
/** <p>The balancer is a tool that balances disk space usage on an HDFS cluster
 * when some datanodes become full or when new empty nodes join the cluster.
 * The tool is deployed as an application program that can be run by the 
 * cluster administrator on a live HDFS cluster while applications
 * adding and deleting files.
 * 
 * <p>SYNOPSIS
 * <pre>
 * To start:
 *      bin/start-balancer.sh [-threshold <threshold>]
 *      Example: bin/ start-balancer.sh 
 *                     start the balancer with a default threshold of 10%
 *               bin/ start-balancer.sh -threshold 5
 *                     start the balancer with a threshold of 5%
 * To stop:
 *      bin/ stop-balancer.sh
 * </pre>
 * 
 * <p>DESCRIPTION
 * <p>The threshold parameter is a fraction in the range of (0%, 100%) with a 
 * default value of 10%. The threshold sets a target for whether the cluster 
 * is balanced. A cluster is balanced if for each datanode, the utilization 
 * of the node (ratio of used space at the node to total capacity of the node) 
 * differs from the utilization of the (ratio of used space in the cluster 
 * to total capacity of the cluster) by no more than the threshold value. 
 * The smaller the threshold, the more balanced a cluster will become. 
 * It takes more time to run the balancer for small threshold values. 
 * Also for a very small threshold the cluster may not be able to reach the 
 * balanced state when applications write and delete files concurrently.
 * 
 * <p>The tool moves blocks from highly utilized datanodes to poorly 
 * utilized datanodes iteratively. In each iteration a datanode moves or 
 * receives no more than the lesser of 10G bytes or the threshold fraction 
 * of its capacity. Each iteration runs no more than 20 minutes.
 * At the end of each iteration, the balancer obtains updated datanodes
 * information from the namenode.
 * 
 * <p>A system property that limits the balancer's use of bandwidth is 
 * defined in the default configuration file:
 * <pre>
 * <property>
 *   <name>dfs.balance.bandwidthPerSec</name>
 *   <value>1048576</value>
 * <description>  Specifies the maximum bandwidth that each datanode 
 * can utilize for the balancing purpose in term of the number of bytes 
 * per second. </description>
 * </property>
 * </pre>
 * 
 * <p>This property determines the maximum speed at which a block will be 
 * moved from one datanode to another. The default value is 1MB/s. The higher 
 * the bandwidth, the faster a cluster can reach the balanced state, 
 * but with greater competition with application processes. If an 
 * administrator changes the value of this property in the configuration 
 * file, the change is observed when HDFS is next restarted.
 * 
 * <p>MONITERING BALANCER PROGRESS
 * <p>After the balancer is started, an output file name where the balancer 
 * progress will be recorded is printed on the screen.  The administrator 
 * can monitor the running of the balancer by reading the output file. 
 * The output shows the balancer's status iteration by iteration. In each 
 * iteration it prints the starting time, the iteration number, the total 
 * number of bytes that have been moved in the previous iterations, 
 * the total number of bytes that are left to move in order for the cluster 
 * to be balanced, and the number of bytes that are being moved in this 
 * iteration. Normally "Bytes Already Moved" is increasing while "Bytes Left 
 * To Move" is decreasing.
 * 
 * <p>Running multiple instances of the balancer in an HDFS cluster is 
 * prohibited by the tool.
 * 
 * <p>The balancer automatically exits when any of the following five 
 * conditions is satisfied:
 * <ol>
 * <li>The cluster is balanced;
 * <li>No block can be moved;
 * <li>No block has been moved for five consecutive iterations;
 * <li>An IOException occurs while communicating with the namenode;
 * <li>Another balancer is running.
 * </ol>
 * 
 * <p>Upon exit, a balancer returns an exit code and prints one of the 
 * following messages to the output file in corresponding to the above exit 
 * reasons:
 * <ol>
 * <li>The cluster is balanced. Exiting
 * <li>No block can be moved. Exiting...
 * <li>No block has been moved for 3 iterations. Exiting...
 * <li>Received an IO exception: failure reason. Exiting...
 * <li>Another balancer is running. Exiting...
 * </ol>
 * 
 * <p>The administrator can interrupt the execution of the balancer at any 
 * time by running the command "stop-balancer.sh" on the machine where the 
 * balancer is running.
 */
package org.apache.hadoop.hdfs.server.balancer;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Formatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

private class Source extends BalancerDatanode {
    
    /* A thread that initiates a block move 
     * and waits for block move to complete */
    private class BlockMoveDispatcher implements Runnable {
      public void run() {
        dispatchBlocks();
      }
    }
    
    private ArrayList<NodeTask> nodeTasks = new ArrayList<NodeTask>(2);
    private long blocksToReceive = 0L;
    /* source blocks point to balancerBlocks in the global list because
     * we want to keep one copy of a block in balancer and be aware that
     * the locations are changing over time.
     */
    private List<BalancerBlock> srcBlockList
            = new ArrayList<BalancerBlock>();
    
    /* constructor */
    private Source(DatanodeInfo node, double avgUtil, double threshold) {
      super(node, avgUtil, threshold);
    }
    
    /** Add a node task */
    private void addNodeTask(NodeTask task) {
      assert (task.datanode != this) :
        "Source and target are the same " + datanode.getName();
      incScheduledSize(task.getSize());
      nodeTasks.add(task);
    }
    
    /* Return an iterator to this source's blocks */
    private Iterator<BalancerBlock> getBlockIterator() {
      return srcBlockList.iterator();
    }
    
    /* fetch new blocks of this source from namenode and
     * update this source's block list & the global block list
     * Return the total size of the received blocks in the number of bytes.
     */
    private long getBlockList() throws IOException {
      BlockWithLocations[] newBlocks = namenode.getBlocks(datanode, 
        (long)Math.min(MAX_BLOCKS_SIZE_TO_FETCH, blocksToReceive)).getBlocks();
      long bytesReceived = 0;
      for (BlockWithLocations blk : newBlocks) {
        bytesReceived += blk.getBlock().getNumBytes();
        BalancerBlock block;
        synchronized(globalBlockList) {
          block = globalBlockList.get(blk.getBlock());
          if (block==null) {
            block = new BalancerBlock(blk.getBlock());
            globalBlockList.put(blk.getBlock(), block);
          } else {
            block.clearLocations();
          }
        
          synchronized (block) {
            // update locations
            for ( String location : blk.getDatanodes() ) {
              BalancerDatanode datanode = datanodes.get(location);
              if (datanode != null) { // not an unknown datanode
                block.addLocation(datanode);
              }
            }
          }
          if (!srcBlockList.contains(block) && isGoodBlockCandidate(block)) {
            // filter bad candidates
            srcBlockList.add(block);
          }
        }
      }
      return bytesReceived;
    }

    /* Decide if the given block is a good candidate to move or not */
    private boolean isGoodBlockCandidate(BalancerBlock block) {
      for (NodeTask nodeTask : nodeTasks) {
        if (Balancer.this.isGoodBlockCandidate(this, nodeTask.datanode, block)) {
          return true;
        }
      }
      return false;
    }

    /* Return a block that's good for the source thread to dispatch immediately
     * The block's source, target, and proxy source are determined too.
     * When choosing proxy and target, source & target throttling
     * has been considered. They are chosen only when they have the capacity
     * to support this block move.
     * The block should be dispatched immediately after this method is returned.
     */
    private PendingBlockMove chooseNextBlockToMove() {
      for ( Iterator<NodeTask> tasks=nodeTasks.iterator(); tasks.hasNext(); ) {
        NodeTask task = tasks.next();
        BalancerDatanode target = task.getDatanode();
        PendingBlockMove pendingBlock = new PendingBlockMove();
        if ( target.addPendingBlock(pendingBlock) ) { 
          // target is not busy, so do a tentative block allocation
          pendingBlock.source = this;
          pendingBlock.target = target;
          if ( pendingBlock.chooseBlockAndProxy() ) {
            long blockSize = pendingBlock.block.getNumBytes(); 
            scheduledSize -= blockSize;
            task.size -= blockSize;
            if (task.size == 0) {
              tasks.remove();
            }
            return pendingBlock;
          } else {
            // cancel the tentative move
            target.removePendingBlock(pendingBlock);
          }
        }
      }
      return null;
    }

    /* iterate all source's blocks to remove moved ones */    
    private void filterMovedBlocks() {
      for (Iterator<BalancerBlock> blocks=getBlockIterator();
            blocks.hasNext();) {
        if (isMoved(blocks.next())) {
          blocks.remove();
        }
      }
    }
    
    private static final int SOURCE_BLOCK_LIST_MIN_SIZE=5;
    /* Return if should fetch more blocks from namenode */
    private boolean shouldFetchMoreBlocks() {
      return srcBlockList.size()<SOURCE_BLOCK_LIST_MIN_SIZE &&
                 blocksToReceive>0;
    }
    
    /* This method iteratively does the following:
     * it first selects a block to move,
     * then sends a request to the proxy source to start the block move
     * when the source's block list falls below a threshold, it asks
     * the namenode for more blocks.
     * It terminates when it has dispatch enough block move tasks or
     * it has received enough blocks from the namenode, or 
     * the elapsed time of the iteration has exceeded the max time limit.
     */ 
    private static final long MAX_ITERATION_TIME = 20*60*1000L; //20 mins
    private void dispatchBlocks() {
      long startTime = Util.now();
      this.blocksToReceive = 2*scheduledSize;
      boolean isTimeUp = false;
      while(!isTimeUp && scheduledSize>0 &&
          (!srcBlockList.isEmpty() || blocksToReceive>0)) {
        PendingBlockMove pendingBlock = chooseNextBlockToMove();
        if (pendingBlock != null) {
          // move the block
          pendingBlock.scheduleBlockMove();
          continue;
        }
        
        /* Since we can not schedule any block to move,
         * filter any moved blocks from the source block list and
         * check if we should fetch more blocks from the namenode
         */
        filterMovedBlocks(); // filter already moved blocks
        if (shouldFetchMoreBlocks()) {
          // fetch new blocks
          try {
            blocksToReceive -= getBlockList();
            continue;
          } catch (IOException e) {
            LOG.warn(StringUtils.stringifyException(e));
            return;
          }
        } 
        
        // check if time is up or not
        if (Util.now()-startTime > MAX_ITERATION_TIME) {
          isTimeUp = true;
          continue;
        }
        
        /* Now we can not schedule any block to move and there are
         * no new blocks added to the source block list, so we wait. 
         */
        try {
          synchronized(Balancer.this) {
            Balancer.this.wait(1000);  // wait for targets/sources to be idle
          }
        } catch (InterruptedException ignored) {
        }
      }
    }
  }