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

public static class RegexFilter extends FilterBase {
    private Pattern p;
    /** Define the filtering regex and stores it in conf
     * @param conf where the regex is set
     * @param regex regex used as a filter
     */
    public static void setPattern(Configuration conf, String regex)
      throws PatternSyntaxException {
      try {
        Pattern.compile(regex);
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("Invalid pattern: "+regex);
      }
      conf.set(FILTER_REGEX, regex);
    }
        
    public RegexFilter() { }
        
    /** configure the Filter by checking the configuration
     */
    public void setConf(Configuration conf) {
      String regex = conf.get(FILTER_REGEX);
      if (regex==null)
        throw new RuntimeException(FILTER_REGEX + "not set");
      this.p = Pattern.compile(regex);
      this.conf = conf;
    }


    /** Filtering method
     * If key matches the regex, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      return p.matcher(key.toString()).matches();
    }
  }