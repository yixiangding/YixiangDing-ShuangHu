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
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 *******************************************************/
package org.apache.hadoop.mapred;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.LinkedHashMap;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.JobClient.TaskStatusFilter;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.log4j.LogManager;

private class TaskLauncher extends Thread {
    private IntWritable numFreeSlots;
    private final int maxSlots;
    private List<TaskInProgress> tasksToLaunch;

    public TaskLauncher(int numSlots) {
      this.maxSlots = numSlots;
      this.numFreeSlots = new IntWritable(numSlots);
      this.tasksToLaunch = new LinkedList<TaskInProgress>();
      setDaemon(true);
      setName("TaskLauncher for task");
    }

    public void addToTaskQueue(LaunchTaskAction action) {
      synchronized (tasksToLaunch) {
        TaskInProgress tip = registerTask(action, this);
        tasksToLaunch.add(tip);
        tasksToLaunch.notifyAll();
      }
    }
    
    public void cleanTaskQueue() {
      tasksToLaunch.clear();
    }
    
    public void addFreeSlot() {
      synchronized (numFreeSlots) {
        numFreeSlots.set(numFreeSlots.get() + 1);
        assert (numFreeSlots.get() <= maxSlots);
        LOG.info("addFreeSlot : current free slots : " + numFreeSlots.get());
        numFreeSlots.notifyAll();
      }
    }
    
    public void run() {
      while (!Thread.interrupted()) {
        try {
          TaskInProgress tip;
          synchronized (tasksToLaunch) {
            while (tasksToLaunch.isEmpty()) {
              tasksToLaunch.wait();
            }
            //get the TIP
            tip = tasksToLaunch.remove(0);
            LOG.info("Trying to launch : " + tip.getTask().getTaskID());
          }
          //wait for a slot to run
          synchronized (numFreeSlots) {
            while (numFreeSlots.get() == 0) {
              numFreeSlots.wait();
            }
            LOG.info("In TaskLauncher, current free slots : " + numFreeSlots.get()+
                " and trying to launch "+tip.getTask().getTaskID());
            numFreeSlots.set(numFreeSlots.get() - 1);
            assert (numFreeSlots.get() >= 0);
          }
          synchronized (tip) {
            //to make sure that there is no kill task action for this
            if (tip.getRunState() != TaskStatus.State.UNASSIGNED) {
              //got killed externally while still in the launcher queue
              addFreeSlot();
              continue;
            }
            tip.slotTaken = true;
          }
          //got a free slot. launch the task
          startNewTask(tip);
        } catch (InterruptedException e) { 
          return; // ALL DONE
        } catch (Throwable th) {
          LOG.error("TaskLauncher error " + 
              StringUtils.stringifyException(th));
        }
      }
    }
  }