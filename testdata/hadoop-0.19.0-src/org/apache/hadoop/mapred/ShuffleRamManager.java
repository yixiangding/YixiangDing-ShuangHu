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
/** A Reduce task. */
package org.apache.hadoop.mapred;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.Math;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.IFile.*;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

class ShuffleRamManager implements RamManager {
      /* Maximum percentage of the in-memory limit that a single shuffle can 
       * consume*/ 
      private static final float MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION = 0.25f;
      
      /* Maximum percentage of shuffle-threads which can be stalled 
       * simultaneously after which a merge is triggered. */ 
      private static final float MAX_STALLED_SHUFFLE_THREADS_FRACTION = 0.75f;
      
      private final int maxSize;
      private final int maxSingleShuffleLimit;
      
      private int size = 0;
      
      private Object dataAvailable = new Object();
      private int fullSize = 0;
      private int numPendingRequests = 0;
      private int numRequiredMapOutputs = 0;
      private int numClosed = 0;
      private boolean closed = false;
      
      public ShuffleRamManager(Configuration conf) throws IOException {
        final float maxInMemCopyUse =
          conf.getFloat("mapred.job.shuffle.input.buffer.percent", 0.70f);
        if (maxInMemCopyUse > 1.0 || maxInMemCopyUse < 0.0) {
          throw new IOException("mapred.job.shuffle.input.buffer.percent" +
                                maxInMemCopyUse);
        }
        maxSize = (int)Math.min(
            Runtime.getRuntime().maxMemory() * maxInMemCopyUse,
            Integer.MAX_VALUE);
        maxSingleShuffleLimit = (int)(maxSize * MAX_SINGLE_SHUFFLE_SEGMENT_FRACTION);
        LOG.info("ShuffleRamManager: MemoryLimit=" + maxSize + 
                 ", MaxSingleShuffleLimit=" + maxSingleShuffleLimit);
      }
      
      public synchronized boolean reserve(int requestedSize, InputStream in) 
      throws InterruptedException {
        // Wait till the request can be fulfilled...
        while ((size + requestedSize) > maxSize) {
          
          // Close the input...
          if (in != null) {
            try {
              in.close();
            } catch (IOException ie) {
              LOG.info("Failed to close connection with: " + ie);
            } finally {
              in = null;
            }
          } 

          // Track pending requests
          synchronized (dataAvailable) {
            ++numPendingRequests;
            dataAvailable.notify();
          }

          // Wait for memory to free up
          wait();
          
          // Track pending requests
          synchronized (dataAvailable) {
            --numPendingRequests;
          }
        }
        
        size += requestedSize;
        
        return (in != null);
      }
      
      public synchronized void unreserve(int requestedSize) {
        size -= requestedSize;
        
        synchronized (dataAvailable) {
          fullSize -= requestedSize;
          --numClosed;
        }
        
        // Notify the threads blocked on RamManager.reserve
        notifyAll();
      }
      
      public boolean waitForDataToMerge() throws InterruptedException {
        boolean done = false;
        synchronized (dataAvailable) {
                 // Start in-memory merge if manager has been closed or...
          while (!closed
                 &&
                 // In-memory threshold exceeded and at least two segments
                 // have been fetched
                 (getPercentUsed() < maxInMemCopyPer || numClosed < 2)
                 &&
                 // More than "mapred.inmem.merge.threshold" map outputs
                 // have been fetched into memory
                 (maxInMemOutputs <= 0 || numClosed < maxInMemOutputs)
                 && 
                 // More than MAX... threads are blocked on the RamManager
                 // or the blocked threads are the last map outputs to be
                 // fetched. If numRequiredMapOutputs is zero, either
                 // setNumCopiedMapOutputs has not been called (no map ouputs
                 // have been fetched, so there is nothing to merge) or the
                 // last map outputs being transferred without
                 // contention, so a merge would be premature.
                 (numPendingRequests < 
                      numCopiers*MAX_STALLED_SHUFFLE_THREADS_FRACTION && 
                  (0 == numRequiredMapOutputs ||
                   numPendingRequests < numRequiredMapOutputs))) {
            dataAvailable.wait();
          }
          done = closed;
        }
        return done;
      }
      
      public void closeInMemoryFile(int requestedSize) {
        synchronized (dataAvailable) {
          fullSize += requestedSize;
          ++numClosed;
          dataAvailable.notify();
        }
      }
      
      public void setNumCopiedMapOutputs(int numRequiredMapOutputs) {
        synchronized (dataAvailable) {
          this.numRequiredMapOutputs = numRequiredMapOutputs;
          dataAvailable.notify();
        }
      }
      
      public void close() {
        synchronized (dataAvailable) {
          closed = true;
          LOG.info("Closed ram manager");
          dataAvailable.notify();
        }
      }
      
      private float getPercentUsed() {
        return (float)fullSize/maxSize;
      }

      int getMemoryLimit() {
        return maxSize;
      }
      
      boolean canFitInMemory(long requestedSize) {
        return (requestedSize < Integer.MAX_VALUE && 
                requestedSize < maxSingleShuffleLimit);
      }
    }