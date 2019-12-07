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
 * A {@link JobInProgressListener} that maintains the jobs being managed in
 * a queue. By default the queue is FIFO, but it is possible to use custom
 * queue ordering by using the
 * {@link #JobQueueJobInProgressListener(Collection)} constructor.
 */
package org.apache.hadoop.mapred;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;

static class JobSchedulingInfo {
    private JobPriority priority;
    private long startTime;
    private JobID id;
    
    public JobSchedulingInfo(JobInProgress jip) {
      this(jip.getStatus());
    }
    
    public JobSchedulingInfo(JobStatus status) {
      priority = status.getJobPriority();
      startTime = status.getStartTime();
      id = status.getJobID();
    }
    
    JobPriority getPriority() {return priority;}
    long getStartTime() {return startTime;}
    JobID getJobID() {return id;}
  }