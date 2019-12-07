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
/*************************************************************
 * TaskInProgress maintains all the info needed for a
 * Task in the lifetime of its owning Job.  A given Task
 * might be speculatively executed or reexecuted, so we
 * need a level of indirection above the running-id itself.
 * <br>
 * A given TaskInProgress contains multiple taskids,
 * 0 or more of which might be executing at any one time.
 * (That's what allows speculative execution.)  A taskid
 * is now *never* recycled.  A TIP allocates enough taskids
 * to account for all the speculation and failures it will
 * ever have to handle.  Once those are up, the TIP is dead.
 * **************************************************************
 */
package org.apache.hadoop.mapred;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient.RawSplit;
import org.apache.hadoop.mapred.SortedRanges.Range;
import org.apache.hadoop.net.Node;

class Divide {
      private final SortedRanges skipRange;
      private final Range test;
      private final Range other;
      private boolean testPassed;
      Divide(Range range){
        long half = range.getLength()/2;
        test = new Range(range.getStartIndex(), half);
        other = new Range(test.getEndIndex(), range.getLength()-half);
        //construct the skip range from the skipRanges
        skipRange = new SortedRanges();
        for(Range r : skipRanges.getRanges()) {
          skipRange.add(r);
        }
        skipRange.add(new Range(0,test.getStartIndex()));
        skipRange.add(new Range(test.getEndIndex(), 
            (Long.MAX_VALUE-test.getEndIndex())));
      }
    }