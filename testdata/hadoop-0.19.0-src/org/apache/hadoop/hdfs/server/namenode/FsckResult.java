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
 * This class provides rudimentary checking of DFS volumes for errors and
 * sub-optimal conditions.
 * <p>The tool scans all files and directories, starting from an indicated
 *  root path. The following abnormal conditions are detected and handled:</p>
 * <ul>
 * <li>files with blocks that are completely missing from all datanodes.<br/>
 * In this case the tool can perform one of the following actions:
 *  <ul>
 *      <li>none ({@link #FIXING_NONE})</li>
 *      <li>move corrupted files to /lost+found directory on DFS
 *      ({@link #FIXING_MOVE}). Remaining data blocks are saved as a
 *      block chains, representing longest consecutive series of valid blocks.</li>
 *      <li>delete corrupted files ({@link #FIXING_DELETE})</li>
 *  </ul>
 *  </li>
 *  <li>detect files with under-replicated or over-replicated blocks</li>
 *  </ul>
 *  Additionally, the tool collects a detailed overall DFS statistics, and
 *  optionally can print detailed statistics on block locations and replication
 *  factors of each file.
 */
package org.apache.hadoop.hdfs.server.namenode;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.PermissionStatus;

public static class FsckResult {
    private ArrayList<String> missingIds = new ArrayList<String>();
    private long missingSize = 0L;
    private long corruptFiles = 0L;
    private long corruptBlocks = 0L;
    private long excessiveReplicas = 0L;
    private long missingReplicas = 0L;
    private long numOverReplicatedBlocks = 0L;
    private long numUnderReplicatedBlocks = 0L;
    private long numMisReplicatedBlocks = 0L;  // blocks that do not satisfy block placement policy
    private long numMinReplicatedBlocks = 0L;  // minimally replicatedblocks
    private int replication = 0;
    private long totalBlocks = 0L;
    private long totalOpenFilesBlocks = 0L;
    private long totalFiles = 0L;
    private long totalOpenFiles = 0L;
    private long totalDirs = 0L;
    private long totalSize = 0L;
    private long totalOpenFilesSize = 0L;
    private long totalReplicas = 0L;
    private int totalDatanodes = 0;
    private int totalRacks = 0;
    
    /**
     * DFS is considered healthy if there are no missing blocks.
     */
    public boolean isHealthy() {
      return ((missingIds.size() == 0) && (corruptBlocks == 0));
    }
    
    /** Add a missing block name, plus its size. */
    public void addMissing(String id, long size) {
      missingIds.add(id);
      missingSize += size;
    }
    
    /** Return a list of missing block names (as list of Strings). */
    public ArrayList<String> getMissingIds() {
      return missingIds;
    }
    
    /** Return total size of missing data, in bytes. */
    public long getMissingSize() {
      return missingSize;
    }

    public void setMissingSize(long missingSize) {
      this.missingSize = missingSize;
    }
    
    /** Return the number of over-replicated blocks. */
    public long getExcessiveReplicas() {
      return excessiveReplicas;
    }
    
    public void setExcessiveReplicas(long overReplicatedBlocks) {
      this.excessiveReplicas = overReplicatedBlocks;
    }
    
    /** Return the actual replication factor. */
    public float getReplicationFactor() {
      if (totalBlocks == 0)
        return 0.0f;
      return (float) (totalReplicas) / (float) totalBlocks;
    }
    
    /** Return the number of under-replicated blocks. Note: missing blocks are not counted here.*/
    public long getMissingReplicas() {
      return missingReplicas;
    }
    
    public void setMissingReplicas(long underReplicatedBlocks) {
      this.missingReplicas = underReplicatedBlocks;
    }
    
    /** Return total number of directories encountered during this scan. */
    public long getTotalDirs() {
      return totalDirs;
    }
    
    public void setTotalDirs(long totalDirs) {
      this.totalDirs = totalDirs;
    }
    
    /** Return total number of files encountered during this scan. */
    public long getTotalFiles() {
      return totalFiles;
    }
    
    public void setTotalFiles(long totalFiles) {
      this.totalFiles = totalFiles;
    }
    
    /** Return total number of files opened for write encountered during this scan. */
    public long getTotalOpenFiles() {
      return totalOpenFiles;
    }

    /** Set total number of open files encountered during this scan. */
    public void setTotalOpenFiles(long totalOpenFiles) {
      this.totalOpenFiles = totalOpenFiles;
    }
    
    /** Return total size of scanned data, in bytes. */
    public long getTotalSize() {
      return totalSize;
    }
    
    public void setTotalSize(long totalSize) {
      this.totalSize = totalSize;
    }
    
    /** Return total size of open files data, in bytes. */
    public long getTotalOpenFilesSize() {
      return totalOpenFilesSize;
    }
    
    public void setTotalOpenFilesSize(long totalOpenFilesSize) {
      this.totalOpenFilesSize = totalOpenFilesSize;
    }
    
    /** Return the intended replication factor, against which the over/under-
     * replicated blocks are counted. Note: this values comes from the current
     * Configuration supplied for the tool, so it may be different from the
     * value in DFS Configuration.
     */
    public int getReplication() {
      return replication;
    }
    
    public void setReplication(int replication) {
      this.replication = replication;
    }
    
    /** Return the total number of blocks in the scanned area. */
    public long getTotalBlocks() {
      return totalBlocks;
    }
    
    public void setTotalBlocks(long totalBlocks) {
      this.totalBlocks = totalBlocks;
    }
    
    /** Return the total number of blocks held by open files. */
    public long getTotalOpenFilesBlocks() {
      return totalOpenFilesBlocks;
    }
    
    public void setTotalOpenFilesBlocks(long totalOpenFilesBlocks) {
      this.totalOpenFilesBlocks = totalOpenFilesBlocks;
    }
    
    public String toString() {
      StringBuffer res = new StringBuffer();
      res.append("Status: " + (isHealthy() ? "HEALTHY" : "CORRUPT"));
      res.append("\n Total size:\t" + totalSize + " B");
      if (totalOpenFilesSize != 0) 
        res.append(" (Total open files size: " + totalOpenFilesSize + " B)");
      res.append("\n Total dirs:\t" + totalDirs);
      res.append("\n Total files:\t" + totalFiles);
      if (totalOpenFiles != 0)
        res.append(" (Files currently being written: " + 
                   totalOpenFiles + ")");
      res.append("\n Total blocks (validated):\t" + totalBlocks);
      if (totalBlocks > 0) res.append(" (avg. block size "
                                      + (totalSize / totalBlocks) + " B)");
      if (totalOpenFilesBlocks != 0)
        res.append(" (Total open file blocks (not validated): " + 
                   totalOpenFilesBlocks + ")");
      if (corruptFiles > 0) { 
        res.append("\n  ********************************");
        res.append("\n  CORRUPT FILES:\t" + corruptFiles);
        if (missingSize > 0) {
          res.append("\n  MISSING BLOCKS:\t" + missingIds.size());
          res.append("\n  MISSING SIZE:\t\t" + missingSize + " B");
        }
        if (corruptBlocks > 0) {
          res.append("\n  CORRUPT BLOCKS: \t" + corruptBlocks);
        }
        res.append("\n  ********************************");
      }
      res.append("\n Minimally replicated blocks:\t" + numMinReplicatedBlocks);
      if (totalBlocks > 0)        res.append(" (" + ((float) (numMinReplicatedBlocks * 100) / (float) totalBlocks) + " %)");
      res.append("\n Over-replicated blocks:\t" + numOverReplicatedBlocks);
      if (totalBlocks > 0)        res.append(" (" + ((float) (numOverReplicatedBlocks * 100) / (float) totalBlocks) + " %)");
      res.append("\n Under-replicated blocks:\t" + numUnderReplicatedBlocks);
      if (totalBlocks > 0)        res.append(" (" + ((float) (numUnderReplicatedBlocks * 100) / (float) totalBlocks) + " %)");
      res.append("\n Mis-replicated blocks:\t\t" + numMisReplicatedBlocks);
      if (totalBlocks > 0)        res.append(" (" + ((float) (numMisReplicatedBlocks * 100) / (float) totalBlocks) + " %)");
      res.append("\n Default replication factor:\t" + replication);
      res.append("\n Average block replication:\t" + getReplicationFactor());
      res.append("\n Corrupt blocks:\t\t" + corruptBlocks);
      res.append("\n Missing replicas:\t\t" + missingReplicas);
      if (totalReplicas > 0)        res.append(" (" + ((float) (missingReplicas * 100) / (float) totalReplicas) + " %)");
      res.append("\n Number of data-nodes:\t\t" + totalDatanodes);
      res.append("\n Number of racks:\t\t" + totalRacks);
      return res.toString();
    }
    
    /** Return the number of currupted files. */
    public long getCorruptFiles() {
      return corruptFiles;
    }
    
    public void setCorruptFiles(long corruptFiles) {
      this.corruptFiles = corruptFiles;
    }
  }