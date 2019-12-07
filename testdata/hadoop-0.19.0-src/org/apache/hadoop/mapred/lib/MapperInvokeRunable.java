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

private class MapperInvokeRunable implements Runnable {
    private K1 key;
    private V1 value;
    private OutputCollector<K2, V2> output;
    private Reporter reporter;

    /**
     * Collecting all required parameters to execute a Mapper.map call.
     * <p>
     *
     * @param key
     * @param value
     * @param output
     * @param reporter
     */
    public MapperInvokeRunable(K1 key, V1 value,
                               OutputCollector<K2, V2> output,
                               Reporter reporter) {
      this.key = key;
      this.value = value;
      this.output = output;
      this.reporter = reporter;
    }

    /**
     * Executes a Mapper.map call with the given Mapper and parameters.
     * <p>
     * This method is called from the thread-pool thread.
     *
     */
    public void run() {
      try {
        // map pair to output
        MultithreadedMapRunner.this.mapper.map(key, value, output, reporter);
        if(incrProcCount) {
          reporter.incrCounter(SkipBadRecords.COUNTER_GROUP, 
              SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS, 1);
        }
      } catch (IOException ex) {
        // If there is an IOException during the call it is set in an instance
        // variable of the MultithreadedMapRunner from where it will be
        // rethrown.
        synchronized (MultithreadedMapRunner.this) {
          if (MultithreadedMapRunner.this.ioException == null) {
            MultithreadedMapRunner.this.ioException = ex;
          }
        }
      } catch (RuntimeException ex) {
        // If there is a RuntimeException during the call it is set in an
        // instance variable of the MultithreadedMapRunner from where it will be
        // rethrown.
        synchronized (MultithreadedMapRunner.this) {
          if (MultithreadedMapRunner.this.runtimeException == null) {
            MultithreadedMapRunner.this.runtimeException = ex;
          }
        }
      }
    }
  }