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
 * Default parser for job history files. It creates object model from 
 * job history file. 
 * 
 */
package org.apache.hadoop.mapred;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.Values;

abstract static class NodesFilter implements JobHistory.Listener {
    private Map<String, Set<String>> badNodesToNumFailedTasks =
      new HashMap<String, Set<String>>();
    
    Map<String, Set<String>> getValues(){
      return badNodesToNumFailedTasks; 
    }
    String failureType;
    
    public void handle(JobHistory.RecordTypes recType, Map<Keys, String> values)
      throws IOException {
      if (recType.equals(JobHistory.RecordTypes.MapAttempt) || 
          recType.equals(JobHistory.RecordTypes.ReduceAttempt)) {
        if (failureType.equals(values.get(Keys.TASK_STATUS)) ) {
          String hostName = values.get(Keys.HOSTNAME);
          String taskid = values.get(Keys.TASKID); 
          Set<String> tasks = badNodesToNumFailedTasks.get(hostName); 
          if (null == tasks ){
            tasks = new TreeSet<String>(); 
            tasks.add(taskid);
            badNodesToNumFailedTasks.put(hostName, tasks);
          }else{
            tasks.add(taskid);
          }
        }
      }      
    }
    abstract void setFailureType();
    String getFailureType() {
      return failureType;
    }
    NodesFilter() {
      setFailureType();
    }
  }