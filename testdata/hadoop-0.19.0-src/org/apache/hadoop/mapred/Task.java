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

public static class Task extends KeyValuePair{
    private Map <String, TaskAttempt> taskAttempts = new TreeMap<String, TaskAttempt>(); 

    /**
     * Log start time of task (TIP).
     * @param taskId task id
     * @param taskType MAP or REDUCE
     * @param startTime startTime of tip. 
     */
    public static void logStarted(TaskID taskId, String taskType, 
                                  long startTime, String splitLocations) {
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Task, 
                         new Keys[]{Keys.TASKID, Keys.TASK_TYPE ,
                                    Keys.START_TIME, Keys.SPLITS}, 
                         new String[]{taskId.toString(), taskType,
                                      String.valueOf(startTime),
                                      splitLocations});
        }
      }
    }
    /**
     * Log finish time of task. 
     * @param taskId task id
     * @param taskType MAP or REDUCE
     * @param finishTime finish timeof task in ms
     */
    public static void logFinished(TaskID taskId, String taskType, 
                                   long finishTime, Counters counters){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskId.getJobID()); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Task, 
                         new Keys[]{Keys.TASKID, Keys.TASK_TYPE, 
                                    Keys.TASK_STATUS, Keys.FINISH_TIME,
                                    Keys.COUNTERS}, 
                         new String[]{ taskId.toString(), taskType, Values.SUCCESS.name(), 
                                       String.valueOf(finishTime),
                                       counters.makeEscapedCompactString()});
        }
      }
    }
    /**
     * Log job failed event.
     * @param taskId task id
     * @param taskType MAP or REDUCE.
     * @param time timestamp when job failed detected. 
     * @param error error message for failure. 
     */
    public static void logFailed(TaskID taskId, String taskType, long time, String error){
      logFailed(taskId, taskType, time, error, null);
    }
    
    /**
     * @param failedDueToAttempt The attempt that caused the failure, if any
     */
    public static void logFailed(TaskID taskId, String taskType, long time,
                                 String error, 
                                 TaskAttemptID failedDueToAttempt){
      if (!disableHistory){
        ArrayList<PrintWriter> writer = openJobs.get(JOBTRACKER_UNIQUE_STRING 
                                                     + taskId.getJobID()); 

        if (null != writer){
          String failedAttempt = failedDueToAttempt == null
                                 ? ""
                                 : failedDueToAttempt.toString();
          JobHistory.log(writer, RecordTypes.Task, 
                         new Keys[]{Keys.TASKID, Keys.TASK_TYPE, 
                                    Keys.TASK_STATUS, Keys.FINISH_TIME, 
                                    Keys.ERROR, Keys.TASK_ATTEMPT_ID}, 
                         new String[]{ taskId.toString(),  taskType, 
                                      Values.FAILED.name(), 
                                      String.valueOf(time) , error, 
                                      failedAttempt});
        }
      }
    }
    /**
     * Returns all task attempts for this task. <task attempt id - TaskAttempt>
     */
    public Map<String, TaskAttempt> getTaskAttempts(){
      return this.taskAttempts;
    }
  }