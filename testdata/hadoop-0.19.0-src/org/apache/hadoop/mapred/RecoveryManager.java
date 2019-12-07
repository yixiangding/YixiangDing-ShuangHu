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

class RecoveryManager {
    Set<JobID> jobsToRecover; // set of jobs to be recovered
    
    private int totalEventsRecovered = 0;
    
    /** A custom listener that replays the events in the order in which the 
     * events (task attempts) occurred. 
     */
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
    
    public RecoveryManager() {
      jobsToRecover = new TreeSet<JobID>();
    }

    public boolean contains(JobID id) {
      return jobsToRecover.contains(id);
    }

    void addJobForRecovery(JobID id) {
      jobsToRecover.add(id);
    }

    public boolean shouldRecover() {
      return jobsToRecover.size() != 0;
    }

    /** Check if the given string represents a job-id or not 
     */
    private boolean isJobNameValid(String str) {
      if(str == null) {
        return false;
      }
      String[] parts = str.split("_");
      if(parts.length == 3) {
        if(parts[0].equals("job")) {
            // other 2 parts should be parseable
            return JobTracker.validateIdentifier(parts[1])
                   && JobTracker.validateJobNumber(parts[2]);
        }
      }
      return false;
    }
    
    // checks if the job dir has the required files
    public void checkAndAddJob(FileStatus status) throws IOException {
      String jobName = status.getPath().getName();
      if (isJobNameValid(jobName)) {
        if (JobClient.isJobDirValid(status.getPath(), fs)) {
          recoveryManager.addJobForRecovery(JobID.forName(jobName));
        } else {
          LOG.info("Found an incomplete job directory " + jobName + "." 
                   + " Deleting it!!");
          fs.delete(status.getPath(), true);
        }
      } else {
        LOG.info("Deleting " + status.getPath());
        fs.delete(status.getPath(), true);
      }
    }
    
    private JobStatusChangeEvent updateJob(JobInProgress jip, 
                                           JobHistory.JobInfo job) {
      // Change the job priority
      String jobpriority = job.get(Keys.JOB_PRIORITY);
      JobPriority priority = JobPriority.valueOf(jobpriority);
      // It's important to update this via the jobtracker's api as it will 
      // take care of updating the event listeners too
      setJobPriority(jip.getJobID(), priority);

      // Save the previous job status
      JobStatus oldStatus = (JobStatus)jip.getStatus().clone();
      
      // Set the start/launch time only if there are recovered tasks
      // Increment the job's restart count
      jip.updateJobInfo(job.getLong(JobHistory.Keys.SUBMIT_TIME), 
                        job.getLong(JobHistory.Keys.LAUNCH_TIME),
                        job.getInt(Keys.RESTART_COUNT) + 1);

      // Save the new job status
      JobStatus newStatus = (JobStatus)jip.getStatus().clone();
      
      return new JobStatusChangeEvent(jip, EventType.START_TIME_CHANGED, oldStatus, 
                                      newStatus);
    }
    
    private void updateTip(TaskInProgress tip, JobHistory.Task task) {
      long startTime = task.getLong(Keys.START_TIME);
      if (startTime != 0) {
        tip.setExecStartTime(startTime);
      }
      
      long finishTime = task.getLong(Keys.FINISH_TIME);
      // For failed tasks finish-time will be missing
      if (finishTime != 0) {
        tip.setExecFinishTime(finishTime);
      }
      
      String cause = task.get(Keys.TASK_ATTEMPT_ID);
      if (cause.length() > 0) {
        // This means that the this is a FAILED events
        TaskAttemptID id = TaskAttemptID.forName(cause);
        TaskStatus status = tip.getTaskStatus(id);
        // This will add the tip failed event in the new log
        tip.getJob().failedTask(tip, id, status.getDiagnosticInfo(), 
                                status.getPhase(), status.getRunState(), 
                                status.getTaskTracker(), myInstrumentation);
      }
    }
    
    private void createTaskAttempt(JobInProgress job, 
                                   TaskAttemptID attemptId, 
                                   JobHistory.TaskAttempt attempt) {
      TaskID id = attemptId.getTaskID();
      String type = attempt.get(Keys.TASK_TYPE);
      TaskInProgress tip = job.getTaskInProgress(id);
      
      //    I. Get the required info
      TaskStatus taskStatus = null;
      String trackerName = attempt.get(Keys.TRACKER_NAME);
      String trackerHostName = 
        JobInProgress.convertTrackerNameToHostName(trackerName);
      int index = trackerHostName.indexOf("_");
      trackerHostName = 
        trackerHostName.substring(index + 1, trackerHostName.length());
      int port = attempt.getInt(Keys.HTTP_PORT);
      
      long attemptStartTime = attempt.getLong(Keys.START_TIME);

      // II. Create the (appropriate) task status
      if (type.equals(Values.MAP.name())) {
        taskStatus = 
          new MapTaskStatus(attemptId, 0.0f, TaskStatus.State.RUNNING, 
                            "", "", trackerName, TaskStatus.Phase.MAP, 
                            new Counters());
      } else {
        taskStatus = 
          new ReduceTaskStatus(attemptId, 0.0f, TaskStatus.State.RUNNING, 
                               "", "", trackerName, TaskStatus.Phase.REDUCE, 
                               new Counters());
      }

      // Set the start time
      taskStatus.setStartTime(attemptStartTime);

      List<TaskStatus> ttStatusList = new ArrayList<TaskStatus>();
      ttStatusList.add(taskStatus);
      
      // III. Create the dummy tasktracker status
      TaskTrackerStatus ttStatus = 
        new TaskTrackerStatus(trackerName, trackerHostName, port, ttStatusList, 
                              0 , 0, 0);
      ttStatus.setLastSeen(System.currentTimeMillis());

      // IV. Register a new tracker
      boolean isTrackerRegistered = getTaskTracker(trackerName) != null;
      if (!isTrackerRegistered) {
        addNewTracker(ttStatus);
      }
      
      // V. Update the tracker status
      //    This will update the meta info of the jobtracker and also add the
      //    tracker status if missing i.e register it
      updateTaskTrackerStatus(trackerName, ttStatus);
      
      // VI. Register the attempt
      //   a) In the job
      job.addRunningTaskToTIP(tip, attemptId, ttStatus, false);
      //   b) In the tip
      tip.updateStatus(taskStatus);
      
      // VII. Make an entry in the launched tasks
      expireLaunchingTasks.addNewTask(attemptId);
    }
    
    private void addSuccessfulAttempt(JobInProgress job, 
                                      TaskAttemptID attemptId, 
                                      JobHistory.TaskAttempt attempt) {
      // I. Get the required info
      TaskID taskId = attemptId.getTaskID();
      String type = attempt.get(Keys.TASK_TYPE);

      TaskInProgress tip = job.getTaskInProgress(taskId);
      long attemptFinishTime = attempt.getLong(Keys.FINISH_TIME);

      // Get the task status and the tracker name and make a copy of it
      TaskStatus taskStatus = (TaskStatus)tip.getTaskStatus(attemptId).clone();
      taskStatus.setFinishTime(attemptFinishTime);

      String stateString = attempt.get(Keys.STATE_STRING);

      // Update the basic values
      taskStatus.setStateString(stateString);
      taskStatus.setProgress(1.0f);
      taskStatus.setRunState(TaskStatus.State.SUCCEEDED);

      // Set the shuffle/sort finished times
      if (type.equals(Values.REDUCE.name())) {
        long shuffleTime = 
          Long.parseLong(attempt.get(Keys.SHUFFLE_FINISHED));
        long sortTime = 
          Long.parseLong(attempt.get(Keys.SORT_FINISHED));
        taskStatus.setShuffleFinishTime(shuffleTime);
        taskStatus.setSortFinishTime(sortTime);
      }

      // Add the counters
      String counterString = attempt.get(Keys.COUNTERS);
      Counters counter = null;
      //TODO Check if an exception should be thrown
      try {
        counter = Counters.fromEscapedCompactString(counterString);
      } catch (ParseException pe) { 
        counter = new Counters(); // Set it to empty counter
      }
      taskStatus.setCounters(counter);
      
      // II. Replay the status
      job.updateTaskStatus(tip, taskStatus, myInstrumentation);
      
      // III. Prevent the task from expiry
      expireLaunchingTasks.removeTask(attemptId);
    }
    
    private void addUnsuccessfulAttempt(JobInProgress job,
                                        TaskAttemptID attemptId,
                                        JobHistory.TaskAttempt attempt) {
      // I. Get the required info
      TaskID taskId = attemptId.getTaskID();
      TaskInProgress tip = job.getTaskInProgress(taskId);
      long attemptFinishTime = attempt.getLong(Keys.FINISH_TIME);

      TaskStatus taskStatus = (TaskStatus)tip.getTaskStatus(attemptId).clone();
      taskStatus.setFinishTime(attemptFinishTime);

      // Reset the progress
      taskStatus.setProgress(0.0f);
      
      String stateString = attempt.get(Keys.STATE_STRING);
      taskStatus.setStateString(stateString);

      boolean hasFailed = 
        attempt.get(Keys.TASK_STATUS).equals(Values.FAILED.name());
      // Set the state failed/killed
      if (hasFailed) {
        taskStatus.setRunState(TaskStatus.State.FAILED);
      } else {
        taskStatus.setRunState(TaskStatus.State.KILLED);
      }

      // Get/Set the error msg
      String diagInfo = attempt.get(Keys.ERROR);
      taskStatus.setDiagnosticInfo(diagInfo); // diag info

      // II. Update the task status
     job.updateTaskStatus(tip, taskStatus, myInstrumentation);

     // III. Prevent the task from expiry
     expireLaunchingTasks.removeTask(attemptId);
    }
  
    public void recover() throws IOException {
      // I. Init the jobs and cache the recovered job history filenames
      Map<JobID, Path> jobHistoryFilenameMap = new HashMap<JobID, Path>();
      for (JobID id : jobsToRecover) {
        // 1. Create the job object
        JobInProgress job = new JobInProgress(id, JobTracker.this, conf);
        
        // 2. Get the log file and the file path
        String logFileName = 
          JobHistory.JobInfo.getJobHistoryFileName(job.getJobConf(), id);
        Path jobHistoryFilePath = 
          JobHistory.JobInfo.getJobHistoryLogLocation(logFileName);
        
        // 3. Recover the history file. This involved
        //     - deleting file.recover if file exists
        //     - renaming file.recover to file if file doesnt exist
        // This makes sure that the (master) file exists
        JobHistory.JobInfo.recoverJobHistoryFile(job.getJobConf(), 
                                                 jobHistoryFilePath);

        // 4. Cache the history file name as it costs one dfs access
        jobHistoryFilenameMap.put(job.getJobID(), jobHistoryFilePath);

        // 5. Sumbit the job to the jobtracker
        addJob(id, job);
      }

      long recoveryStartTime = System.currentTimeMillis();

      // II. Recover each job
      for (JobID id : jobsToRecover) {
        JobInProgress pJob = getJob(id);

        // 1. Get the required info
        // Get the recovered history file
        Path jobHistoryFilePath = jobHistoryFilenameMap.get(pJob.getJobID());
        String logFileName = jobHistoryFilePath.getName();

        FileSystem fs = jobHistoryFilePath.getFileSystem(conf);

        // 2. Parse the history file
        // Note that this also involves job update
        JobRecoveryListener listener = new JobRecoveryListener(pJob);
        try {
          JobHistory.parseHistoryFromFS(jobHistoryFilePath.toString(), 
                                        listener, fs);
        } catch (IOException e) {
          LOG.info("JobTracker failed to recover job " + pJob + "."
                   + " Ignoring it.", e);
          continue;
        }

        // 3. Close the listener
        listener.close();

        // 4. Update the recovery metric
        totalEventsRecovered += listener.getNumEventsRecovered();

        // 5. Cleanup history
        // Delete the master log file as an indication that the new file
        // should be used in future
        synchronized (pJob) {
          JobHistory.JobInfo.checkpointRecovery(logFileName, 
              pJob.getJobConf());
        }

        // 6. Inform the jobtracker as to how much of the data is recovered.
        // This is done so that TT should rollback to account for lost
        // updates
        lastSeenEventMapOnRestart.put(pJob.getStatus().getJobID(), 
                                      pJob.getNumTaskCompletionEvents());
      }

      recoveryDuration = System.currentTimeMillis() - recoveryStartTime;
      hasRecovered = true;

      // III. Finalize the recovery
      // Make sure that the tracker statuses in the expiry-tracker queue
      // are updated
      long now = System.currentTimeMillis();
      int size = trackerExpiryQueue.size();
      for (int i = 0; i < size ; ++i) {
        // Get the first status
        TaskTrackerStatus status = trackerExpiryQueue.first();

        // Remove it
        trackerExpiryQueue.remove(status);

        // Set the new time
        status.setLastSeen(now);

        // Add back to get the sorted list
        trackerExpiryQueue.add(status);
      }

      // IV. Cleanup
      jobsToRecover.clear();
      LOG.info("Restoration complete");
    }
    
    int totalEventsRecovered() {
      return totalEventsRecovered;
    }
  }