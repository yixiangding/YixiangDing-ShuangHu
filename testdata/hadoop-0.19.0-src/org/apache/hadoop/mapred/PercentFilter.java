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

public static class PercentFilter extends FilterBase {
    private int frequency;
    private int count;

    /** set the frequency and stores it in conf
     * @param conf configuration
     * @param frequency filtering frequencey
     */
    public static void setFrequency(Configuration conf, int frequency){
      if (frequency<=0)
        throw new IllegalArgumentException(
                                           "Negative " + FILTER_FREQUENCY + ": "+frequency);
      conf.setInt(FILTER_FREQUENCY, frequency);
    }
        
    public PercentFilter() { }
        
    /** configure the filter by checking the configuration
     * 
     * @param conf configuration
     */
    public void setConf(Configuration conf) {
      this.frequency = conf.getInt("sequencefile.filter.frequency", 10);
      if (this.frequency <=0) {
        throw new RuntimeException(
                                   "Negative "+FILTER_FREQUENCY+": "+this.frequency);
      }
      this.conf = conf;
    }

    /** Filtering method
     * If record# % frequency==0, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      boolean accepted = false;
      if (count == 0)
        accepted = true;
      if (++count == frequency) {
        count = 0;
      }
      return accepted;
    }
  }