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

public static class JobInfo extends KeyValuePair{
    
    private Map<String, Task> allTasks = new TreeMap<String, Task>();
    
    /** Create new JobInfo */
    public JobInfo(String jobId){ 
      set(Keys.JOBID, jobId);  
    }

    /**
     * Returns all map and reduce tasks <taskid-Task>. 
     */
    public Map<String, Task> getAllTasks() { return allTasks; }
    
    /**
     * Get the path of the locally stored job file
     * @param jobId id of the job
     * @return the path of the job file on the local file system 
     */
    public static String getLocalJobFilePath(JobID jobId){
      return System.getProperty("hadoop.log.dir") + File.separator +
               jobId + "_conf.xml";
    }
    
    /**
     * Helper function to encode the URL of the path of the job-history
     * log file. 
     * 
     * @param logFile path of the job-history file
     * @return URL encoded path
     * @throws IOException
     */
    public static String encodeJobHistoryFilePath(String logFile)
    throws IOException {
      Path rawPath = new Path(logFile);
      String encodedFileName = null;
      try {
        encodedFileName = URLEncoder.encode(rawPath.getName(), "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      
      Path encodedPath = new Path(rawPath.getParent(), encodedFileName);
      return encodedPath.toString();
    }
    
    /**
     * Helper function to encode the URL of the filename of the job-history 
     * log file.
     * 
     * @param logFileName file name of the job-history file
     * @return URL encoded filename
     * @throws IOException
     */
    public static String encodeJobHistoryFileName(String logFileName)
    throws IOException {
      String encodedFileName = null;
      try {
        encodedFileName = URLEncoder.encode(logFileName, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      return encodedFileName;
    }
    
    /**
     * Helper function to decode the URL of the filename of the job-history 
     * log file.
     * 
     * @param logFileName file name of the job-history file
     * @return URL decoded filename
     * @throws IOException
     */
    public static String decodeJobHistoryFileName(String logFileName)
    throws IOException {
      String decodedFileName = null;
      try {
        decodedFileName = URLDecoder.decode(logFileName, "UTF-8");
      } catch (UnsupportedEncodingException uee) {
        IOException ioe = new IOException();
        ioe.initCause(uee);
        ioe.setStackTrace(uee.getStackTrace());
        throw ioe;
      }
      return decodedFileName;
    }
    
    /**
     * Get the job name from the job conf
     */
    static String getJobName(JobConf jobConf) {
      String jobName = jobConf.getJobName();
      if (jobName == null || jobName.length() == 0) {
        jobName = "NA";
      }
      return jobName;
    }
    
    /**
     * Get the user name from the job conf
     */
    public static String getUserName(JobConf jobConf) {
      String user = jobConf.getUser();
      if (user == null || user.length() == 0) {
        user = "NA";
      }
      return user;
    }
    
    /**
     * Get the job history file path given the history filename
     */
    public static Path getJobHistoryLogLocation(String logFileName)
    {
      return LOG_DIR == null ? null : new Path(LOG_DIR, logFileName);
    }

    /**
     * Get the user job history file path
     */
    public static Path getJobHistoryLogLocationForUser(String logFileName, 
                                                       JobConf jobConf) {
      // find user log directory 
      Path userLogFile = null;
      Path outputPath = FileOutputFormat.getOutputPath(jobConf);
      String userLogDir = jobConf.get("hadoop.job.history.user.location",
                                      outputPath == null 
                                      ? null 
                                      : outputPath.toString());
      if ("none".equals(userLogDir)) {
        userLogDir = null;
      }
      if (userLogDir != null) {
        userLogDir = userLogDir + Path.SEPARATOR + "_logs" + Path.SEPARATOR 
                     + "history";
        userLogFile = new Path(userLogDir, logFileName);
      }
      return userLogFile;
    }

    /**
     * Generates the job history filename for a new job
     */
    private static String getNewJobHistoryFileName(JobConf jobConf, JobID id) {
      return JOBTRACKER_UNIQUE_STRING
             + id.toString() + "_" + getUserName(jobConf) + "_" 
             + trimJobName(getJobName(jobConf));
    }
    
    /**
     * Trims the job-name if required
     */
    private static String trimJobName(String jobName) {
      if (jobName.length() > JOB_NAME_TRIM_LENGTH) {
        jobName = jobName.substring(0, JOB_NAME_TRIM_LENGTH);
      }
      return jobName;
    }
    
    private static String escapeRegexChars( String string ) {
      return "\\Q"+string.replaceAll("\\\\E", "\\\\E\\\\\\\\E\\\\Q")+"\\E";
    }

    /**
     * Recover the job history filename from the history folder. 
     * Uses the following pattern
     *    $jt-hostname_[0-9]*_$job-id_$user-$job-name*
     * @param jobConf the job conf
     * @param id job id
     */
    public static synchronized String getJobHistoryFileName(JobConf jobConf, 
                                                            JobID id) 
    throws IOException {
      String user = getUserName(jobConf);
      String jobName = trimJobName(getJobName(jobConf));
      
      FileSystem fs = new Path(LOG_DIR).getFileSystem(jobConf);
      if (LOG_DIR == null) {
        return null;
      }
      
      jobName = escapeRegexChars( jobName );

      // Make the pattern matching the job's history file
      final Pattern historyFilePattern = 
        Pattern.compile(jobtrackerHostname + "_" + "[0-9]+" + "_" 
                        + id.toString() + "_" + user + "_" + jobName + "+");
      // a path filter that matches 4 parts of the filenames namely
      //  - jt-hostname
      //  - job-id
      //  - username
      //  - jobname
      PathFilter filter = new PathFilter() {
        public boolean accept(Path path) {
          String fileName = path.getName();
          try {
            fileName = decodeJobHistoryFileName(fileName);
          } catch (IOException ioe) {
            LOG.info("Error while decoding history file " + fileName + "."
                     + " Ignoring file.", ioe);
            return false;
          }
          return historyFilePattern.matcher(fileName).find();
        }
      };
      
      FileStatus[] statuses = fs.listStatus(new Path(LOG_DIR), filter);
      String filename;
      if (statuses.length == 0) {
        filename = 
          encodeJobHistoryFileName(getNewJobHistoryFileName(jobConf, id));
      } else {
        // return filename considering that fact the name can be a 
        // secondary filename like filename.recover
        filename = decodeJobHistoryFileName(statuses[0].getPath().getName());
        // Remove the '.recover' suffix if it exists
        if (filename.endsWith(jobName + SECONDARY_FILE_SUFFIX)) {
          int newLength = filename.length() - SECONDARY_FILE_SUFFIX.length();
          filename = filename.substring(0, newLength);
        }
        filename = encodeJobHistoryFileName(filename);
      }
      return filename;
    }
    
    /** Since there was a restart, there should be a master file and 
     * a recovery file. Once the recovery is complete, the master should be 
     * deleted as an indication that the recovery file should be treated as the 
     * master upon completion or next restart.
     * @param fileName the history filename that needs checkpointing
     * @param conf Job conf
     * @throws IOException
     */
    static synchronized void checkpointRecovery(String fileName, JobConf conf) 
    throws IOException {
      Path logPath = JobHistory.JobInfo.getJobHistoryLogLocation(fileName);
      if (logPath != null) {
        FileSystem fs = logPath.getFileSystem(conf);
        fs.delete(logPath, false);
      }
      // do the same for the user file too
      logPath = JobHistory.JobInfo.getJobHistoryLogLocationForUser(fileName, 
                                                                   conf);
      if (logPath != null) {
        FileSystem fs = logPath.getFileSystem(conf);
        fs.delete(logPath, false);
      }
    }
    
    static String getSecondaryJobHistoryFile(String filename) 
    throws IOException {
      return encodeJobHistoryFileName(
          decodeJobHistoryFileName(filename) + SECONDARY_FILE_SUFFIX);
    }
    
    /** Selects one of the two files generated as a part of recovery. 
     * The thumb rule is that always select the oldest file. 
     * This call makes sure that only one file is left in the end. 
     * @param conf job conf
     * @param logFilePath Path of the log file
     * @throws IOException 
     */
    public synchronized static Path recoverJobHistoryFile(JobConf conf, 
                                                          Path logFilePath) 
    throws IOException {
      FileSystem fs = logFilePath.getFileSystem(conf);
      String tmpFilename = getSecondaryJobHistoryFile(logFilePath.getName());
      Path logDir = logFilePath.getParent();
      Path tmpFilePath = new Path(logDir, tmpFilename);
      if (fs.exists(logFilePath)) {
        if (fs.exists(tmpFilePath)) {
          fs.delete(tmpFilePath, false);
        }
        return tmpFilePath;
      } else {
        if (fs.exists(tmpFilePath)) {
          fs.rename(tmpFilePath, logFilePath);
          return tmpFilePath;
        } else {
          return logFilePath;
        }
      }
    }

    /** Finalize the recovery and make one file in the end. 
     * This invloves renaming the recover file to the master file.
     * @param id Job id  
     * @param conf the job conf
     * @throws IOException
     */
    static synchronized void finalizeRecovery(JobID id, JobConf conf) 
    throws IOException {
      String masterLogFileName = 
        JobHistory.JobInfo.getJobHistoryFileName(conf, id);
      Path masterLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocation(masterLogFileName);
      String tmpLogFileName = getSecondaryJobHistoryFile(masterLogFileName);
      Path tmpLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocation(tmpLogFileName);
      if (masterLogPath != null) {
        FileSystem fs = masterLogPath.getFileSystem(conf);

        // rename the tmp file to the master file. Note that this should be 
        // done only when the file is closed and handles are released.
        if(fs.exists(tmpLogPath)) {
          fs.rename(tmpLogPath, masterLogPath);
        }
      }
      
      // do the same for the user file too
      masterLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocationForUser(masterLogFileName,
                                                           conf);
      tmpLogPath = 
        JobHistory.JobInfo.getJobHistoryLogLocationForUser(tmpLogFileName, 
                                                           conf);
      if (masterLogPath != null) {
        FileSystem fs = masterLogPath.getFileSystem(conf);
        if (fs.exists(tmpLogPath)) {
          fs.rename(tmpLogPath, masterLogPath);
        }
      }
    }

    /**
     * Log job submitted event to history. Creates a new file in history 
     * for the job. if history file creation fails, it disables history 
     * for all other events. 
     * @param jobId job id assigned by job tracker.
     * @param jobConf job conf of the job
     * @param jobConfPath path to job conf xml file in HDFS.
     * @param submitTime time when job tracker received the job
     * @throws IOException
     */
    public static void logSubmitted(JobID jobId, JobConf jobConf, 
                                    String jobConfPath, long submitTime) 
    throws IOException {
      FileSystem fs = null;
      String userLogDir = null;
      String jobUniqueString = JOBTRACKER_UNIQUE_STRING + jobId;

      if (!disableHistory){
        // Get the username and job name to be used in the actual log filename;
        // sanity check them too        
        String jobName = getJobName(jobConf);

        String user = getUserName(jobConf);
        
        // get the history filename
        String logFileName = 
          getJobHistoryFileName(jobConf, jobId);

        // setup the history log file for this job
        Path logFile = getJobHistoryLogLocation(logFileName);
        
        // find user log directory
        Path userLogFile = 
          getJobHistoryLogLocationForUser(logFileName, jobConf);

        try{
          ArrayList<PrintWriter> writers = new ArrayList<PrintWriter>();
          FSDataOutputStream out = null;
          PrintWriter writer = null;

          if (LOG_DIR != null) {
            // create output stream for logging in hadoop.job.history.location
            fs = new Path(LOG_DIR).getFileSystem(jobConf);
            
            logFile = recoverJobHistoryFile(jobConf, logFile);
            
            int defaultBufferSize = 
              fs.getConf().getInt("io.file.buffer.size", 4096);
            out = fs.create(logFile, FsPermission.getDefault(), true, 
                            defaultBufferSize, 
                            fs.getDefaultReplication(), 
                            jobHistoryBlockSize, null);
            writer = new PrintWriter(out);
            writers.add(writer);
          }
          if (userLogFile != null) {
            userLogDir = userLogFile.getParent().toString();
            // create output stream for logging 
            // in hadoop.job.history.user.location
            fs = userLogFile.getFileSystem(jobConf);
 
            userLogFile = recoverJobHistoryFile(jobConf, userLogFile);
            
            out = fs.create(userLogFile, true, 4096);
            writer = new PrintWriter(out);
            writers.add(writer);
          }

          openJobs.put(jobUniqueString, writers);
          
          // Log the history meta info
          JobHistory.MetaInfoManager.logMetaInfo(writers);

          //add to writer as well 
          JobHistory.log(writers, RecordTypes.Job, 
                         new Keys[]{Keys.JOBID, Keys.JOBNAME, Keys.USER, Keys.SUBMIT_TIME, Keys.JOBCONF }, 
                         new String[]{jobId.toString(), jobName, user, 
                                      String.valueOf(submitTime) , jobConfPath}
                        ); 
             
        }catch(IOException e){
          LOG.error("Failed creating job history log file, disabling history", e);
          disableHistory = true; 
        }
      }
      // Always store job conf on local file system 
      String localJobFilePath =  JobInfo.getLocalJobFilePath(jobId); 
      File localJobFile = new File(localJobFilePath);
      FileOutputStream jobOut = null;
      try {
        jobOut = new FileOutputStream(localJobFile);
        jobConf.writeXml(jobOut);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Job conf for " + jobId + " stored at " 
                    + localJobFile.getAbsolutePath());
        }
      } catch (IOException ioe) {
        LOG.error("Failed to store job conf on the local filesystem ", ioe);
      } finally {
        if (jobOut != null) {
          try {
            jobOut.close();
          } catch (IOException ie) {
            LOG.info("Failed to close the job configuration file " 
                       + StringUtils.stringifyException(ie));
          }
        }
      }

      /* Storing the job conf on the log dir */
      Path jobFilePath = null;
      if (LOG_DIR != null) {
        jobFilePath = new Path(LOG_DIR + File.separator + 
                               jobUniqueString + "_conf.xml");
      }
      Path userJobFilePath = null;
      if (userLogDir != null) {
        userJobFilePath = new Path(userLogDir + File.separator +
                                   jobUniqueString + "_conf.xml");
      }
      FSDataOutputStream jobFileOut = null;
      try {
        if (LOG_DIR != null) {
          fs = new Path(LOG_DIR).getFileSystem(jobConf);
          if (!fs.exists(jobFilePath)) {
            jobFileOut = fs.create(jobFilePath);
            jobConf.writeXml(jobFileOut);
            jobFileOut.close();
          }
        } 
        if (userLogDir != null) {
          fs = new Path(userLogDir).getFileSystem(jobConf);
          jobFileOut = fs.create(userJobFilePath);
          jobConf.writeXml(jobFileOut);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Job conf for " + jobId + " stored at " 
                    + jobFilePath + "and" + userJobFilePath );
        }
      } catch (IOException ioe) {
        LOG.error("Failed to store job conf on the local filesystem ", ioe);
      } finally {
        if (jobFileOut != null) {
          try {
            jobFileOut.close();
          } catch (IOException ie) {
            LOG.info("Failed to close the job configuration file " 
                     + StringUtils.stringifyException(ie));
          }
        }
      } 
    }
    /**
     * Logs launch time of job. 
     * 
     * @param jobId job id, assigned by jobtracker. 
     * @param startTime start time of job. 
     * @param totalMaps total maps assigned by jobtracker. 
     * @param totalReduces total reduces. 
     */
    public static void logInited(JobID jobId, long startTime, 
                                 int totalMaps, int totalReduces) {
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobId; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job, 
              new Keys[] {Keys.JOBID, Keys.LAUNCH_TIME, Keys.TOTAL_MAPS, 
                          Keys.TOTAL_REDUCES, Keys.JOB_STATUS},
              new String[] {jobId.toString(), String.valueOf(startTime), 
                            String.valueOf(totalMaps), 
                            String.valueOf(totalReduces), 
                            Values.PREP.name()}); 
        }
      }
    }
    
   /**
     * Logs the job as RUNNING. 
     *
     * @param jobId job id, assigned by jobtracker. 
     * @param startTime start time of job. 
     * @param totalMaps total maps assigned by jobtracker. 
     * @param totalReduces total reduces. 
     * @deprecated Use {@link #logInited(JobID, long, int, int)} and 
     * {@link #logStarted(JobID)}
     */
    @Deprecated
    public static void logStarted(JobID jobId, long startTime, 
                                  int totalMaps, int totalReduces) {
      logStarted(jobId);
    }
    
    /**
     * Logs job as running 
     * @param jobId job id, assigned by jobtracker. 
     */
    public static void logStarted(JobID jobId){
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobId; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job, 
              new Keys[] {Keys.JOBID, Keys.JOB_STATUS},
              new String[] {jobId.toString(),  
                            Values.RUNNING.name()}); 
        }
      }
    }
    
    /**
     * Log job finished. closes the job file in history. 
     * @param jobId job id, assigned by jobtracker. 
     * @param finishTime finish time of job in ms. 
     * @param finishedMaps no of maps successfully finished. 
     * @param finishedReduces no of reduces finished sucessfully. 
     * @param failedMaps no of failed map tasks. 
     * @param failedReduces no of failed reduce tasks. 
     * @param counters the counters from the job
     */ 
    public static void logFinished(JobID jobId, long finishTime, 
                                   int finishedMaps, int finishedReduces,
                                   int failedMaps, int failedReduces,
                                   Counters counters){
      if (!disableHistory){
        // close job file for this job
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobId; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job,          
                         new Keys[] {Keys.JOBID, Keys.FINISH_TIME, 
                                     Keys.JOB_STATUS, Keys.FINISHED_MAPS, 
                                     Keys.FINISHED_REDUCES,
                                     Keys.FAILED_MAPS, Keys.FAILED_REDUCES,
                                     Keys.COUNTERS},
                         new String[] {jobId.toString(),  Long.toString(finishTime), 
                                       Values.SUCCESS.name(), 
                                       String.valueOf(finishedMaps), 
                                       String.valueOf(finishedReduces),
                                       String.valueOf(failedMaps), 
                                       String.valueOf(failedReduces),
                                       counters.makeEscapedCompactString()});
          for (PrintWriter out : writer) {
            out.close();
          }
          openJobs.remove(logFileKey); 
        }
        Thread historyCleaner  = new Thread(new HistoryCleaner());
        historyCleaner.start(); 
      }
    }
    /**
     * Logs job failed event. Closes the job history log file. 
     * @param jobid job id
     * @param timestamp time when job failure was detected in ms.  
     * @param finishedMaps no finished map tasks. 
     * @param finishedReduces no of finished reduce tasks. 
     */
    public static void logFailed(JobID jobid, long timestamp, int finishedMaps, int finishedReduces){
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobid; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job,
                         new Keys[] {Keys.JOBID, Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS, Keys.FINISHED_REDUCES },
                         new String[] {jobid.toString(),  String.valueOf(timestamp), Values.FAILED.name(), String.valueOf(finishedMaps), 
                                       String.valueOf(finishedReduces)}); 
          for (PrintWriter out : writer) {
            out.close();
          }
          openJobs.remove(logFileKey); 
        }
      }
    }
    /**
     * Logs job killed event. Closes the job history log file.
     * 
     * @param jobid
     *          job id
     * @param timestamp
     *          time when job killed was issued in ms.
     * @param finishedMaps
     *          no finished map tasks.
     * @param finishedReduces
     *          no of finished reduce tasks.
     */
    public static void logKilled(JobID jobid, long timestamp, int finishedMaps,
        int finishedReduces) {
      if (!disableHistory) {
        String logFileKey = JOBTRACKER_UNIQUE_STRING + jobid;
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey);

        if (null != writer) {
          JobHistory.log(writer, RecordTypes.Job, new Keys[] { Keys.JOBID,
              Keys.FINISH_TIME, Keys.JOB_STATUS, Keys.FINISHED_MAPS,
              Keys.FINISHED_REDUCES }, new String[] { jobid.toString(),
              String.valueOf(timestamp), Values.KILLED.name(),
              String.valueOf(finishedMaps), String.valueOf(finishedReduces) });
          for (PrintWriter out : writer) {
            out.close();
          }
          openJobs.remove(logFileKey);
        }
      }
    }
    /**
     * Log job's priority. 
     * @param jobid job id
     * @param priority Jobs priority 
     */
    public static void logJobPriority(JobID jobid, JobPriority priority){
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobid; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job,
                         new Keys[] {Keys.JOBID, Keys.JOB_PRIORITY},
                         new String[] {jobid.toString(), priority.toString()});
        }
      }
    }
    /**
     * Log job's submit-time/launch-time 
     * @param jobid job id
     * @param submitTime job's submit time
     * @param launchTime job's launch time
     * @param restartCount number of times the job got restarted
     */
    public static void logJobInfo(JobID jobid, long submitTime, long launchTime,
                                  int restartCount){
      if (!disableHistory){
        String logFileKey =  JOBTRACKER_UNIQUE_STRING + jobid; 
        ArrayList<PrintWriter> writer = openJobs.get(logFileKey); 

        if (null != writer){
          JobHistory.log(writer, RecordTypes.Job,
                         new Keys[] {Keys.JOBID, Keys.SUBMIT_TIME, 
                                     Keys.LAUNCH_TIME, Keys.RESTART_COUNT},
                         new String[] {jobid.toString(), 
                                       String.valueOf(submitTime), 
                                       String.valueOf(launchTime),
                                       String.valueOf(restartCount)});
        }
      }
    }
  }