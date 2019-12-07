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
 * Multithreaded implementation for @link org.apache.hadoop.mapred.MapRunnable.
 * <p>
 * It can be used instead of the default implementation,
 * @link org.apache.hadoop.mapred.MapRunner, when the Map operation is not CPU
 * bound in order to improve throughput.
 * <p>
 * Map implementations using this MapRunnable must be thread-safe.
 * <p>
 * The Map-Reduce job has to be configured to use this MapRunnable class (using
 * the JobConf.setMapRunnerClass method) and
 * the number of thread the thread-pool can use with the
 * <code>mapred.map.multithreadedrunner.threads</code> property, its default
 * value is 10 threads.
 * <p>
 */
package org.apache.hadoop.mapred.lib;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.util.concurrent.*;

private static class BlockingArrayQueue extends ArrayBlockingQueue<Runnable> {
    public BlockingArrayQueue(int capacity) {
      super(capacity);
    }
    public boolean offer(Runnable r) {
      return add(r);
    }
    public boolean add(Runnable r) {
      try {
        put(r);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
      return true;
    }
  }