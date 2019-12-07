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
 * A simple logger to handle the task-specific user logs.
 * This class uses the system property <code>hadoop.log.dir</code>.
 * 
 */
package org.apache.hadoop.mapred;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

static class Reader extends InputStream {
    private long bytesRemaining;
    private FileInputStream file;
    /**
     * Read a log file from start to end positions. The offsets may be negative,
     * in which case they are relative to the end of the file. For example,
     * Reader(taskid, kind, 0, -1) is the entire file and 
     * Reader(taskid, kind, -4197, -1) is the last 4196 bytes. 
     * @param taskid the id of the task to read the log file for
     * @param kind the kind of log to read
     * @param start the offset to read from (negative is relative to tail)
     * @param end the offset to read upto (negative is relative to tail)
     * @throws IOException
     */
    public Reader(TaskAttemptID taskid, LogName kind, 
                  long start, long end) throws IOException {
      // find the right log file
      LogFileDetail fileDetail = getTaskLogFileDetail(taskid, kind);
      // calculate the start and stop
      long size = fileDetail.length;
      if (start < 0) {
        start += size + 1;
      }
      if (end < 0) {
        end += size + 1;
      }
      start = Math.max(0, Math.min(start, size));
      end = Math.max(0, Math.min(end, size));
      start += fileDetail.start;
      end += fileDetail.start;
      bytesRemaining = end - start;
      file = new FileInputStream(new File(getBaseDir(fileDetail.location), 
          kind.toString()));
      // skip upto start
      long pos = 0;
      while (pos < start) {
        long result = file.skip(start - pos);
        if (result < 0) {
          bytesRemaining = 0;
          break;
        }
        pos += result;
      }
    }
    
    @Override
    public int read() throws IOException {
      int result = -1;
      if (bytesRemaining > 0) {
        bytesRemaining -= 1;
        result = file.read();
      }
      return result;
    }
    
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      length = (int) Math.min(length, bytesRemaining);
      int bytes = file.read(buffer, offset, length);
      if (bytes > 0) {
        bytesRemaining -= bytes;
      }
      return bytes;
    }
    
    @Override
    public int available() throws IOException {
      return (int) Math.min(bytesRemaining, file.available());
    }

    @Override
    public void close() throws IOException {
      file.close();
    }
  }