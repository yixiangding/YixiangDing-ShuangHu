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
package org.apache.hadoop.mapred;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

private class JvmRunner extends Thread {
      JvmEnv env;
      volatile boolean killed = false;
      volatile int numTasksRan;
      final int numTasksToRun;
      JVMId jvmId;
      volatile boolean busy = true;
      private ShellCommandExecutor shexec; // shell terminal for running the task
      public JvmRunner(JvmEnv env, JobID jobId) {
        this.env = env;
        this.jvmId = new JVMId(jobId, isMap, rand.nextInt());
        this.numTasksToRun = env.conf.getNumTasksToExecutePerJvm();
        LOG.info("In JvmRunner constructed JVM ID: " + jvmId);
      }
      public void run() {
        runChild(env);
      }

      public void runChild(JvmEnv env) {
        try {
          env.vargs.add(Integer.toString(jvmId.getId()));
          List<String> wrappedCommand = 
            TaskLog.captureOutAndError(env.setup, env.vargs, env.stdout, env.stderr,
                env.logSize, env.pidFile);
          shexec = new ShellCommandExecutor(wrappedCommand.toArray(new String[0]), 
              env.workDir, env.env);
          shexec.execute();
        } catch (IOException ioe) {
          // do nothing
          // error and output are appropriately redirected
        } finally { // handle the exit code
          if (shexec == null) {
            return;
          }
          int exitCode = shexec.getExitCode();
          updateOnJvmExit(jvmId, exitCode, killed);
          LOG.info("JVM : " + jvmId +" exited. Number of tasks it ran: " + 
              numTasksRan);
          try {
            //the task jvm cleans up the common workdir for every 
            //task at the beginning of each task in the task JVM.
            //For the last task, we do it here.
            if (env.conf.getNumTasksToExecutePerJvm() != 1) {
              FileUtil.fullyDelete(env.workDir);
            }
          } catch (IOException ie){}
          if (tracker.isTaskMemoryManagerEnabled()) {
          // Remove the associated pid-file, if any
            tracker.getTaskMemoryManager().
               removePidFile(TaskAttemptID.forName(
                   env.conf.get("mapred.task.id")));
          }
        }
      }

      public void kill() {
        if (shexec != null) {
          Process process = shexec.getProcess();
          if (process != null) {
            process.destroy();
          }
        }
        removeJvm(jvmId);
      }
      
      public void taskRan() {
        busy = false;
        numTasksRan++;
      }
      
      public boolean ranAll() {
        return(numTasksRan == numTasksToRun);
      }
      public void setBusy(boolean busy) {
        this.busy = busy;
      }
      public boolean isBusy() {
        return busy;
      }
    }