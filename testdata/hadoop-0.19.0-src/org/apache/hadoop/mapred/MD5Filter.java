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

public static class MD5Filter extends FilterBase {
    private int frequency;
    private static final MessageDigest DIGESTER;
    public static final int MD5_LEN = 16;
    private byte [] digest = new byte[MD5_LEN];
        
    static {
      try {
        DIGESTER = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }


    /** set the filtering frequency in configuration
     * 
     * @param conf configuration
     * @param frequency filtering frequency
     */
    public static void setFrequency(Configuration conf, int frequency){
      if (frequency<=0)
        throw new IllegalArgumentException(
                                           "Negative " + FILTER_FREQUENCY + ": "+frequency);
      conf.setInt(FILTER_FREQUENCY, frequency);
    }
        
    public MD5Filter() { }
        
    /** configure the filter according to configuration
     * 
     * @param conf configuration
     */
    public void setConf(Configuration conf) {
      this.frequency = conf.getInt(FILTER_FREQUENCY, 10);
      if (this.frequency <=0) {
        throw new RuntimeException(
                                   "Negative "+FILTER_FREQUENCY+": "+this.frequency);
      }
      this.conf = conf;
    }

    /** Filtering method
     * If MD5(key) % frequency==0, return true; otherwise return false
     * @see org.apache.hadoop.mapred.SequenceFileInputFilter.Filter#accept(Object)
     */
    public boolean accept(Object key) {
      try {
        long hashcode;
        if (key instanceof Text) {
          hashcode = MD5Hashcode((Text)key);
        } else if (key instanceof BytesWritable) {
          hashcode = MD5Hashcode((BytesWritable)key);
        } else {
          ByteBuffer bb;
          bb = Text.encode(key.toString());
          hashcode = MD5Hashcode(bb.array(), 0, bb.limit());
        }
        if (hashcode/frequency*frequency==hashcode)
          return true;
      } catch(Exception e) {
        LOG.warn(e);
        throw new RuntimeException(e);
      }
      return false;
    }
        
    private long MD5Hashcode(Text key) throws DigestException {
      return MD5Hashcode(key.getBytes(), 0, key.getLength());
    }
        
    private long MD5Hashcode(BytesWritable key) throws DigestException {
      return MD5Hashcode(key.getBytes(), 0, key.getLength());
    }
    synchronized private long MD5Hashcode(byte[] bytes, 
                                          int start, int length) throws DigestException {
      DIGESTER.update(bytes, 0, length);
      DIGESTER.digest(digest, 0, MD5_LEN);
      long hashcode=0;
      for (int i = 0; i < 8; i++)
        hashcode |= ((digest[i] & 0xffL) << (8*(7-i)));
      return hashcode;
    }
  }