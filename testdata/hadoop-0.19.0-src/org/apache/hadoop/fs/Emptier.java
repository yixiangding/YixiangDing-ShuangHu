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
/** Provides a <i>trash</i> feature.  Files are moved to a user's trash
 * directory, a subdirectory of their home directory named ".Trash".  Files are
 * initially moved to a <i>current</i> sub-directory of the trash directory.
 * Within that sub-directory their original path is preserved.  Periodically
 * one may checkpoint the current trash and remove older checkpoints.  (This
 * design permits trash management without enumeration of the full trash
 * content, without date support in the filesystem, and without clock
 * synchronization.)
 */
package org.apache.hadoop.fs;
import java.text.*;
import java.io.*;
import java.util.Date;
import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.util.StringUtils;

private static class Emptier implements Runnable {

    private Configuration conf;
    private FileSystem fs;
    private long interval;

    public Emptier(Configuration conf) throws IOException {
      this.conf = conf;
      this.interval = conf.getLong("fs.trash.interval", 60) * MSECS_PER_MINUTE;
      this.fs = FileSystem.get(conf);
    }

    public void run() {
      if (interval == 0)
        return;                                   // trash disabled

      long now = System.currentTimeMillis();
      long end;
      while (true) {
        end = ceiling(now, interval);
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (InterruptedException e) {
          return;                                 // exit on interrupt
        }
          
        try {
          now = System.currentTimeMillis();
          if (now >= end) {

            FileStatus[] homes = null;
            try {
              homes = fs.listStatus(HOMES);         // list all home dirs
            } catch (IOException e) {
              LOG.warn("Trash can't list homes: "+e+" Sleeping.");
              continue;
            }

            if (homes == null)
              continue;

            for (FileStatus home : homes) {         // dump each trash
              if (!home.isDir())
                continue;
              try {
                Trash trash = new Trash(home.getPath(), conf);
                trash.expunge();
                trash.checkpoint();
              } catch (IOException e) {
                LOG.warn("Trash caught: "+e+". Skipping "+home.getPath()+".");
              } 
            }
          }
        } catch (Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run() " + 
                   StringUtils.stringifyException(e));
        }
      }
    }

    private long ceiling(long time, long interval) {
      return floor(time, interval) + interval;
    }
    private long floor(long time, long interval) {
      return (time / interval) * interval;
    }

  }