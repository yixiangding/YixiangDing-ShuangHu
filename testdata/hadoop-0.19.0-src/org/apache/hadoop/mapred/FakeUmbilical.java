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
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JvmTask;

private static class FakeUmbilical implements TaskUmbilicalProtocol {

    public long getProtocolVersion(String protocol, long clientVersion) {
      return TaskUmbilicalProtocol.versionID;
    }
    
    public void done(TaskAttemptID taskid) throws IOException {
      LOG.info("Task " + taskid + " reporting done.");
    }

    public void fsError(TaskAttemptID taskId, String message) throws IOException {
      LOG.info("Task " + taskId + " reporting file system error: " + message);
    }

    public void shuffleError(TaskAttemptID taskId, String message) throws IOException {
      LOG.info("Task " + taskId + " reporting shuffle error: " + message);
    }

    public JvmTask getTask(JVMId jvmId) throws IOException {
      return null;
    }

    public boolean ping(TaskAttemptID taskid) throws IOException {
      return true;
    }

    public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      statusUpdate(taskId, taskStatus);
    }
    
    public boolean canCommit(TaskAttemptID taskid) throws IOException {
      return true;
    }
    
    public boolean statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus) 
    throws IOException, InterruptedException {
      StringBuffer buf = new StringBuffer("Task ");
      buf.append(taskId);
      buf.append(" making progress to ");
      buf.append(taskStatus.getProgress());
      String state = taskStatus.getStateString();
      if (state != null) {
        buf.append(" and state of ");
        buf.append(state);
      }
      LOG.info(buf.toString());
      // ignore phase
      // ignore counters
      return true;
    }

    public void reportDiagnosticInfo(TaskAttemptID taskid, String trace) throws IOException {
      LOG.info("Task " + taskid + " has problem " + trace);
    }
    
    public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
        int fromEventId, int maxLocs, TaskAttemptID id) throws IOException {
      return new MapTaskCompletionEventsUpdate(TaskCompletionEvent.EMPTY_ARRAY, 
                                               false);
    }

    public void reportNextRecordRange(TaskAttemptID taskid, 
        SortedRanges.Range range) throws IOException {
      LOG.info("Task " + taskid + " reportedNextRecordRange " + range);
    }
  }