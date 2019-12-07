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
 * Utility for collecting samples and writing a partition file for
 * {@link org.apache.hadoop.mapred.lib.TotalOrderPartitioner}.
 */
package org.apache.hadoop.mapred.lib;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public static class IntervalSampler<K,V> implements Sampler<K,V> {
    private final double freq;
    private final int maxSplitsSampled;

    /**
     * Create a new IntervalSampler sampling <em>all</em> splits.
     * @param freq The frequency with which records will be emitted.
     */
    public IntervalSampler(double freq) {
      this(freq, Integer.MAX_VALUE);
    }

    /**
     * Create a new IntervalSampler.
     * @param freq The frequency with which records will be emitted.
     * @param maxSplitsSampled The maximum number of splits to examine.
     * @see #getSample
     */
    public IntervalSampler(double freq, int maxSplitsSampled) {
      this.freq = freq;
      this.maxSplitsSampled = maxSplitsSampled;
    }

    /**
     * For each split sampled, emit when the ratio of the number of records
     * retained to the total record count is less than the specified
     * frequency.
     */
    @SuppressWarnings("unchecked") // ArrayList::toArray doesn't preserve type
    public K[] getSample(InputFormat<K,V> inf, JobConf job) throws IOException {
      InputSplit[] splits = inf.getSplits(job, job.getNumMapTasks());
      ArrayList<K> samples = new ArrayList<K>();
      int splitsToSample = Math.min(maxSplitsSampled, splits.length);
      int splitStep = splits.length / splitsToSample;
      long records = 0;
      long kept = 0;
      for (int i = 0; i < splitsToSample; ++i) {
        RecordReader<K,V> reader = inf.getRecordReader(splits[i * splitStep],
            job, Reporter.NULL);
        K key = reader.createKey();
        V value = reader.createValue();
        while (reader.next(key, value)) {
          ++records;
          if ((double) kept / records < freq) {
            ++kept;
            samples.add(key);
            key = reader.createKey();
          }
        }
        reader.close();
      }
      return (K[])samples.toArray();
    }
  }