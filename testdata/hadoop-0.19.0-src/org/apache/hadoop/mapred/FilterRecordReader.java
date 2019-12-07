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
 * A class that allows a map/red job to work on a sample of sequence files.
 * The sample is decided by the filter class set by the job.
 * 
 */
package org.apache.hadoop.mapred;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

private static class FilterRecordReader<K, V>
    extends SequenceFileRecordReader<K, V> {
    
    private Filter filter;
        
    public FilterRecordReader(Configuration conf, FileSplit split)
      throws IOException {
      super(conf, split);
      // instantiate filter
      filter = (Filter)ReflectionUtils.newInstance(
                                                   conf.getClass(FILTER_CLASS, PercentFilter.class), 
                                                   conf);
    }
        
    public synchronized boolean next(K key, V value) throws IOException {
      while (next(key)) {
        if (filter.accept(key)) {
          getCurrentValue(value);
          return true;
        }
      }
            
      return false;
    }
  }