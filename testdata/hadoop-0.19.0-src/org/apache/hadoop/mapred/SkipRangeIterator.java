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

static class SkipRangeIterator implements Iterator<Long> {
    Iterator<Range> rangeIterator;
    Range range = new Range();
    long next = -1;
    
    /**
     * Constructor
     * @param rangeIterator the iterator which gives the ranges.
     */
    SkipRangeIterator(Iterator<Range> rangeIterator) {
      this.rangeIterator = rangeIterator;
      doNext();
    }
    
    /**
     * Returns true till the index reaches Long.MAX_VALUE.
     * @return <code>true</code> next index exists.
     *         <code>false</code> otherwise.
     */
    public synchronized boolean hasNext() {
      return next<Long.MAX_VALUE;
    }
    
    /**
     * Get the next available index. The index starts from 0.
     * @return next index
     */
    public synchronized Long next() {
      long ci = next;
      doNext();
      return ci;
    }
    
    private void doNext() {
      next++;
      LOG.debug("currentIndex "+next +"   "+range);
      skipIfInRange();
      while(next>=range.getEndIndex() && rangeIterator.hasNext()) {
        range = rangeIterator.next();
        skipIfInRange();
      }
    }
    
    private void skipIfInRange() {
      if(next>=range.getStartIndex() && 
          next<range.getEndIndex()) {
        //need to skip the range
        LOG.warn("Skipping index " + next +"-" + range.getEndIndex());
        next = range.getEndIndex();
        
      }
    }
    
    /**
     * Get whether all the ranges have been skipped.
     * @return <code>true</code> if all ranges have been skipped.
     *         <code>false</code> otherwise.
     */
    synchronized boolean skippedAllRanges() {
      return !rangeIterator.hasNext() && next>range.getEndIndex();
    }
    
    /**
     * Remove is not supported. Doesn't apply.
     */
    public void remove() {
      throw new UnsupportedOperationException("remove not supported.");
    }
    
  }