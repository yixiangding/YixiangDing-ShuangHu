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
 * Keeps the Ranges sorted by startIndex.
 * The added ranges are always ensured to be non-overlapping.
 * Provides the SkipRangeIterator, which skips the Ranges 
 * stored in this object.
 */
package org.apache.hadoop.mapred;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

static class Range implements Comparable<Range>, Writable{
    private long startIndex;
    private long length;
        
    Range(long startIndex, long length) {
      if(length<0) {
        throw new RuntimeException("length can't be negative");
      }
      this.startIndex = startIndex;
      this.length = length;
    }
    
    Range() {
      this(0,0);
    }
    
    /**
     * Get the start index. Start index in inclusive.
     * @return startIndex. 
     */
    long getStartIndex() {
      return startIndex;
    }
    
    /**
     * Get the end index. End index is exclusive.
     * @return endIndex.
     */
    long getEndIndex() {
      return startIndex + length;
    }
    
   /**
    * Get Length.
    * @return length
    */
    long getLength() {
      return length;
    }
    
    /**
     * Range is empty if its length is zero.
     * @return <code>true</code> if empty
     *         <code>false</code> otherwise.
     */
    boolean isEmpty() {
      return length==0;
    }
    
    public boolean equals(Object o) {
      if(o!=null && o instanceof Range) {
        Range range = (Range)o;
        return startIndex==range.startIndex &&
        length==range.length;
      }
      return false;
    }
    
    public int hashCode() {
      return Long.valueOf(startIndex).hashCode() +
          Long.valueOf(length).hashCode();
    }
    
    public int compareTo(Range o) {
      if(this.equals(o)) {
        return 0;
      }
      return (this.startIndex > o.startIndex) ? 1:-1;
    }

    public void readFields(DataInput in) throws IOException {
      startIndex = in.readLong();
      length = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
      out.writeLong(startIndex);
      out.writeLong(length);
    }
    
    public String toString() {
      return startIndex +":" + length;
    }    
  }