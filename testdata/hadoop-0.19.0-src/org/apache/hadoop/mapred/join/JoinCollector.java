/** * Licensed to the Apache Software Foundation (ASF) under one
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
 * A RecordReader that can effect joins of RecordReaders sharing a common key
 * type and partitioning.
 */
package org.apache.hadoop.mapred.join;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

class JoinCollector {
    private K key;
    private ResetableIterator<X>[] iters;
    private int pos = -1;
    private boolean first = true;

    /**
     * Construct a collector capable of handling the specified number of
     * children.
     */
    @SuppressWarnings("unchecked") // Generic array assignment
    public JoinCollector(int card) {
      iters = new ResetableIterator[card];
      for (int i = 0; i < iters.length; ++i) {
        iters[i] = EMPTY;
      }
    }

    /**
     * Register a given iterator at position id.
     */
    public void add(int id, ResetableIterator<X> i)
        throws IOException {
      iters[id] = i;
    }

    /**
     * Return the key associated with this collection.
     */
    public K key() {
      return key;
    }

    /**
     * Codify the contents of the collector to be iterated over.
     * When this is called, all RecordReaders registered for this
     * key should have added ResetableIterators.
     */
    public void reset(K key) {
      this.key = key;
      first = true;
      pos = iters.length - 1;
      for (int i = 0; i < iters.length; ++i) {
        iters[i].reset();
      }
    }

    /**
     * Clear all state information.
     */
    public void clear() {
      key = null;
      pos = -1;
      for (int i = 0; i < iters.length; ++i) {
        iters[i].clear();
        iters[i] = EMPTY;
      }
    }

    /**
     * Returns false if exhausted or if reset(K) has not been called.
     */
    protected boolean hasNext() {
      return !(pos < 0);
    }

    /**
     * Populate Tuple from iterators.
     * It should be the case that, given iterators i_1...i_n over values from
     * sources s_1...s_n sharing key k, repeated calls to next should yield
     * I x I.
     */
    @SuppressWarnings("unchecked") // No static typeinfo on Tuples
    protected boolean next(TupleWritable val) throws IOException {
      if (first) {
        int i = -1;
        for (pos = 0; pos < iters.length; ++pos) {
          if (iters[pos].hasNext() && iters[pos].next((X)val.get(pos))) {
            i = pos;
            val.setWritten(i);
          }
        }
        pos = i;
        first = false;
        if (pos < 0) {
          clear();
          return false;
        }
        return true;
      }
      while (0 <= pos && !(iters[pos].hasNext() &&
                           iters[pos].next((X)val.get(pos)))) {
        --pos;
      }
      if (pos < 0) {
        clear();
        return false;
      }
      val.setWritten(pos);
      for (int i = 0; i < pos; ++i) {
        if (iters[i].replay((X)val.get(i))) {
          val.setWritten(i);
        }
      }
      while (pos + 1 < iters.length) {
        ++pos;
        iters[pos].reset();
        if (iters[pos].hasNext() && iters[pos].next((X)val.get(pos))) {
          val.setWritten(pos);
        }
      }
      return true;
    }

    /**
     * Replay the last Tuple emitted.
     */
    @SuppressWarnings("unchecked") // No static typeinfo on Tuples
    public boolean replay(TupleWritable val) throws IOException {
      // The last emitted tuple might have drawn on an empty source;
      // it can't be cleared prematurely, b/c there may be more duplicate
      // keys in iterator positions < pos
      assert !first;
      boolean ret = false;
      for (int i = 0; i < iters.length; ++i) {
        if (iters[i].replay((X)val.get(i))) {
          val.setWritten(i);
          ret = true;
        }
      }
      return ret;
    }

    /**
     * Close all child iterators.
     */
    public void close() throws IOException {
      for (int i = 0; i < iters.length; ++i) {
        iters[i].close();
      }
    }

    /**
     * Write the next value into key, value as accepted by the operation
     * associated with this set of RecordReaders.
     */
    public boolean flush(TupleWritable value) throws IOException {
      while (hasNext()) {
        value.clearWritten();
        if (next(value) && combine(kids, value)) {
          return true;
        }
      }
      return false;
    }
  }