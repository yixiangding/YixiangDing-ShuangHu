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
/*******************************************************
 * JobTracker is the central location for submitting and 
 * tracking MR jobs in a network environment.
 *
 *******************************************************/
package org.apache.hadoop.mapred;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.Listener;
import org.apache.hadoop.mapred.JobHistory.Values;
import org.apache.hadoop.mapred.JobStatusChangeEvent.EventType;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

class JobRecoveryListener implements Listener {
      // The owner job
      private JobInProgress jip;
      
      private JobHistory.JobInfo job; // current job's info object
      
      // Maintain the count of the (attempt) events recovered
      private int numEventsRecovered = 0;
      
      // Maintains open transactions
      private Map<String, String> hangingAttempts = 
        new HashMap<String, String>();
      
      // Whether there are any updates for this job
      private boolean hasUpdates = false;
      
      public JobRecoveryListener(JobInProgress jip) {
        this.jip = jip;
        this.job = new JobHistory.JobInfo(jip.getJobID().toString());
      }

      /**
       * Process a task. Note that a task might commit a previously pending 
       * transaction.
       */
      private void processTask(String taskId, JobHistory.Task task) {
        // Any TASK info commits the previous transaction
        boolean hasHanging = hangingAttempts.remove(taskId) != null;
        if (hasHanging) {
          numEventsRecovered += 2;
        }
        
        TaskID id = TaskID.forName(taskId);
        TaskInProgress tip = getTip(id);
        
        updateTip(tip, task);
      }

      /**
       * Adds a task-attempt in the listener
       */
      private void processTaskAttempt(String taskAttemptId, 
                                      JobHistory.TaskAttempt attempt) {
        TaskAttemptID id = TaskAttemptID.forName(taskAttemptId);
        
        // Check if the transaction for this attempt can be committed
        String taskStatus = attempt.get(Keys.TASK_STATUS);
        
        if (taskStatus.length() > 0) {
          // This means this is an update event
          if (taskStatus.equals(Values.SUCCESS.name())) {
            // Mark this attempt as hanging
            hangingAttempts.put(id.getTaskID().toString(), taskAttemptId);
            addSuccessfulAttempt(jip, id, attempt);
          } else {
            addUnsuccessfulAttempt(jip, id, attempt);
            numEventsRecovered += 2;
          }
        } else {
          createTaskAttempt(jip, id, attempt);
        }
      }

      public void handle(JobHistory.RecordTypes recType, Map<Keys, 
                         String> values) throws IOException {
        if (recType == JobHistory.RecordTypes.Job) {
          // Update the meta-level job information
          job.handle(values);
          
          // Forcefully init the job as we have some updates for it
          checkAndInit();
        } else if (recType.equals(JobHistory.RecordTypes.Task)) {
          String taskId = values.get(Keys.TASKID);
          
          // Create a task
          JobHistory.Task task = new JobHistory.Task();
          task.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(task)) {
            return;
          }
            
          // Process the task i.e update the tip state
          processTask(taskId, task);
        } else if (recType.equals(JobHistory.RecordTypes.MapAttempt)) {
          String attemptId = values.get(Keys.TASK_ATTEMPT_ID);
          
          // Create a task attempt
          JobHistory.MapAttempt attempt = new JobHistory.MapAttempt();
          attempt.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(attempt)) {
            return;
          }
          
          // Process the attempt i.e update the attempt state via job
          processTaskAttempt(attemptId, attempt);
        } else if (recType.equals(JobHistory.RecordTypes.ReduceAttempt)) {
          String attemptId = values.get(Keys.TASK_ATTEMPT_ID);
          
          // Create a task attempt
          JobHistory.ReduceAttempt attempt = new JobHistory.ReduceAttempt();
          attempt.handle(values);
          
          // Ignore if its a cleanup task
          if (isCleanup(attempt)) {
            return;
          }
          
          // Process the attempt i.e update the job state via job
          processTaskAttempt(attemptId, attempt);
        }
      }

      // Check if the task is of type CLEANUP
      private boolean isCleanup(JobHistory.Task task) {
        String taskType = task.get(Keys.TASK_TYPE);
        return Values.CLEANUP.name().equals(taskType);
      }
      
      // Init the job if its ready for init. Also make sure that the scheduler
      // is updated
      private void checkAndInit() throws IOException {
        String jobStatus = this.job.get(Keys.JOB_STATUS);
        if (Values.PREP.name().equals(jobStatus)) {
          hasUpdates = true;
          LOG.info("Calling init from RM for job " + jip.getJobID().toString());
          jip.initTasks();
        }
      }
      
      void close() {
        if (hasUpdates) {
          // Apply the final (job-level) updates
          JobStatusChangeEvent event = updateJob(jip, job);
          
          // Update the job listeners
          updateJobInProgressListeners(event);
        }
      }
      
      public int getNumEventsRecovered() {
        return numEventsRecovered;
      }

    }