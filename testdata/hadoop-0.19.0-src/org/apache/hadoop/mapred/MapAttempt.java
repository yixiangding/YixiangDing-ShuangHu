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
 * Provides methods for writing to and reading from job history. 
 * Job History works in an append mode, JobHistory and its inner classes provide methods 
 * to log job events. 
 * 
 * JobHistory is split into multiple files, format of each file is plain text where each line 
 * is of the format [type (key=value)*], where type identifies the type of the record. 
 * Type maps to UID of one of the inner classes of this class. 
 * 
 * Job history is maintained in a master index which contains star/stop times of all jobs with
 * a few other job level properties. Apart from this each job's history is maintained in a seperate history 
 * file. name of job history files follows the format jobtrackerId_jobid
 *  
 * For parsing the job history it supports a listener based interface where each line is parsed
 * and passed to listener. The listener can create an object model of history or look for specific 
 * events and discard rest of the history.  
 * 
 * CHANGE LOG :
 * Version 0 : The history has the following format : 
 *             TAG KEY1="VALUE1" KEY2="VALUE2" and so on. 
               TAG can be Job, Task, MapAttempt or ReduceAttempt. 
               Note that a '"' is the line delimiter.
 * Version 1 : Changes the line delimiter to '.'
               Values are now escaped for unambiguous parsing. 
               Added the Meta tag to store version info.
 */
package org.apache.hadoop.mapred;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.StringUtils;

public static class MapAttempt extends TaskAttempt{
    /**
     * Log start time of this map task attempt. 
     * @param taskAttemptId task attempt id
     * @param startTime start time of task attempt as reported by task tracker. 
     * @param hostName host name of the task attempt. 
     * @deprecated Use 
     *             {@link #logStarted(TaskAttemptID, long, String, int, String)}
     */
    @Deprecated
    public static void logStarted(TaskAttemptID taskAttemptId, long startTime, String hostName){
      logStarted(taskAttemptId, startTime, hostName, -1, Values.MAP.name());
    }
    
    /**
     * Log start time of this map task attempt.
     *  
     * @param taskAttemptId task attempt id
     * @param startTime start time of task attempt as reported by task tracker. 
     * @param trackerName name of the tracker executing the task attempt.
     * @param httpPort http port of the task tracker executing the task attempt
     * @param taskType Whether the attempt is cleanup or setup or map 
     */
    public static void logStarted(TaskAttemptID taskAttemptId, long startTime,
                                  String trackerName, int httpPort, 
                                  String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.START_TIME, 
                                     Keys.TRACKER_NAME, Keys.HTTP_PORT},
                         new String[]{taskType,
                                      taskAttemptId.getTaskID().toString(), 
                                      taskAttemptId.toString(), 
                                      String.valueOf(startTime), trackerName,
                                      String.valueOf(httpPort)}); 
        }
      }
    }
    
    /**
     * Log finish time of map task attempt. 
     * @param taskAttemptId task attempt id 
     * @param finishTime finish time
     * @param hostName host name 
     * @deprecated Use 
     * {@link #logFinished(TaskAttemptID, long, String, String, String, Counters)}
     */
    @Deprecated
    public static void logFinished(TaskAttemptID taskAttemptId, long finishTime, 
                                   String hostName){
      logFinished(taskAttemptId, finishTime, hostName, Values.MAP.name(), "", 
                  new Counters());
    }

    /**
     * Log finish time of map task attempt. 
     * 
     * @param taskAttemptId task attempt id 
     * @param finishTime finish time
     * @param hostName host name 
     * @param taskType Whether the attempt is cleanup or setup or map 
     * @param stateString state string of the task attempt
     * @param counter counters of the task attempt
     */
    public static void logFinished(TaskAttemptID taskAttemptId, 
                                   long finishTime, 
                                   String hostName,
                                   String taskType,
                                   String stateString, 
                                   Counters counter) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{ Keys.TASK_TYPE, Keys.TASKID, 
                                     Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                     Keys.FINISH_TIME, Keys.HOSTNAME, 
                                     Keys.STATE_STRING, Keys.COUNTERS},
                         new String[]{taskType, 
                                      taskAttemptId.getTaskID().toString(),
                                      taskAttemptId.toString(), 
                                      Values.SUCCESS.name(),  
                                      String.valueOf(finishTime), hostName, 
                                      stateString, 
                                      counter.makeEscapedCompactString()}); 
        }
      }
    }

    /**
     * Log task attempt failed event.  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt.
     * @deprecated Use
     * {@link #logFailed(TaskAttemptID, long, String, String, String)} 
     */
    @Deprecated
    public static void logFailed(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, 
                                 String error) {
      logFailed(taskAttemptId, timestamp, hostName, error, Values.MAP.name());
    }

    /**
     * Log task attempt failed event. 
     *  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @param taskType Whether the attempt is cleanup or setup or map 
     */
    public static void logFailed(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, 
                                 String error, String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{Keys.TASK_TYPE, Keys.TASKID, 
                                    Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                    Keys.FINISH_TIME, Keys.HOSTNAME, Keys.ERROR},
                         new String[]{ taskType, 
                                       taskAttemptId.getTaskID().toString(),
                                       taskAttemptId.toString(), 
                                       Values.FAILED.name(),
                                       String.valueOf(timestamp), 
                                       hostName, error}); 
        }
      }
    }
    
    /**
     * Log task attempt killed event.  
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @deprecated Use 
     * {@link #logKilled(TaskAttemptID, long, String, String, String)}
     */
    @Deprecated
    public static void logKilled(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName, String error){
      logKilled(taskAttemptId, timestamp, hostName, error, Values.MAP.name());
    } 
    
    /**
     * Log task attempt killed event.  
     * 
     * @param taskAttemptId task attempt id
     * @param timestamp timestamp
     * @param hostName hostname of this task attempt.
     * @param error error message if any for this task attempt. 
     * @param taskType Whether the attempt is cleanup or setup or map 
     */
    public static void logKilled(TaskAttemptID taskAttemptId, 
                                 long timestamp, String hostName,
                                 String error, String taskType) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                   + taskAttemptId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.MapAttempt, 
                         new Keys[]{Keys.TASK_TYPE, Keys.TASKID,
                                    Keys.TASK_ATTEMPT_ID, Keys.TASK_STATUS, 
                                    Keys.FINISH_TIME, Keys.HOSTNAME,
                                    Keys.ERROR},
                         new String[]{ taskType, 
                                       taskAttemptId.getTaskID().toString(), 
                                       taskAttemptId.toString(),
                                       Values.KILLED.name(),
                                       String.valueOf(timestamp), 
                                       hostName, error}); 
        }
      }
    } 
  }