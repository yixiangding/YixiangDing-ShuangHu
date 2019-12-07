/*
 * AbstractMetricsContext.java
 *
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
 * The main class of the Service Provider Interface.  This class should be
 * extended in order to integrate the Metrics API with a specific metrics
 * client library. <p/>
 *
 * This class implements the internal table of metric data, and the timer
 * on which data is to be sent to the metrics system.  Subclasses must
 * override the abstract <code>emitRecord</code> method in order to transmit
 * the data. <p/>
 */
package org.apache.hadoop.metrics.spi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.Updater;

static class TagMap extends TreeMap<String,Object> {
    private static final long serialVersionUID = 3546309335061952993L;
    TagMap() {
      super();
    }
    TagMap(TagMap orig) {
      super(orig);
    }
    /**
     * Returns true if this tagmap contains every tag in other.
     */
    public boolean containsAll(TagMap other) {
      for (Map.Entry<String,Object> entry : other.entrySet()) {
        Object value = get(entry.getKey());
        if (value == null || !value.equals(entry.getValue())) {
          // either key does not exist here, or the value is different
          return false;
        }
      }
      return true;
    }
  }