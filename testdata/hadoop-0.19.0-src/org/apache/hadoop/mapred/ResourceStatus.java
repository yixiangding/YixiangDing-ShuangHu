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
 * A TaskTrackerStatus is a MapReduce primitive.  Keeps
 * info on a TaskTracker.  The JobTracker maintains a set
 * of the most recent TaskTrackerStatus objects for each
 * unique TaskTracker it knows about.
 *
 **************************************************/
package org.apache.hadoop.mapred;
import org.apache.hadoop.io.*;
import java.io.*;
import java.util.*;

static class ResourceStatus implements Writable {
    
    private long freeVirtualMemory;
    private long totalMemory;
    private long availableSpace;
    
    ResourceStatus() {
      freeVirtualMemory = JobConf.DISABLED_VIRTUAL_MEMORY_LIMIT;
      totalMemory = JobConf.DISABLED_VIRTUAL_MEMORY_LIMIT;
      availableSpace = Long.MAX_VALUE;
    }
    
    /**
     * Set the amount of free virtual memory that is available for running
     * a new task
     * @param freeVMem amount of free virtual memory in kilobytes
     */
    void setFreeVirtualMemory(long freeVmem) {
      freeVirtualMemory = freeVmem;
    }

    /**
     * Get the amount of free virtual memory that will be available for
     * running a new task. 
     * 
     * If this is {@link JobConf.DISABLED_VIRTUAL_MEMORY_LIMIT}, it should 
     * be ignored and not used in computation.
     * 
     *@return amount of free virtual memory in kilobytes.
     */
    long getFreeVirtualMemory() {
      return freeVirtualMemory;
    }

    /**
     * Set the maximum amount of virtual memory on the tasktracker.
     * @param vmem maximum amount of virtual memory on the tasktracker in kilobytes.
     */
    void setTotalMemory(long totalMem) {
      totalMemory = totalMem;
    }
    
    /**
     * Get the maximum amount of virtual memory on the tasktracker.
     * 
     * If this is
     * {@link JobConf.DISABLED_VIRTUAL_MEMORY_LIMIT}, it should be ignored 
     * and not used in any computation.
     * 
     * @return maximum amount of virtual memory on the tasktracker in kilobytes. 
     */    
    long getTotalMemory() {
      return totalMemory;
    }
    
    void setAvailableSpace(long availSpace) {
      availableSpace = availSpace;
    }
    
    /**
     * Will return LONG_MAX if space hasn't been measured yet.
     * @return bytes of available local disk space on this tasktracker.
     */    
    long getAvailableSpace() {
      return availableSpace;
    }
    
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVLong(out, freeVirtualMemory);
      WritableUtils.writeVLong(out, totalMemory);
      WritableUtils.writeVLong(out, availableSpace);
    }
    
    public void readFields(DataInput in) throws IOException {
      freeVirtualMemory = WritableUtils.readVLong(in);;
      totalMemory = WritableUtils.readVLong(in);;
      availableSpace = WritableUtils.readVLong(in);;
    }
  }