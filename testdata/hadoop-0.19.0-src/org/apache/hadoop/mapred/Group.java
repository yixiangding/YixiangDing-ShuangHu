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
 * A set of named counters.
 * 
 * <p><code>Counters</code> represent global counters, defined either by the 
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 * 
 * <p><code>Counters</code> are bunched into {@link Group}s, each comprising of
 * counters from a particular <code>Enum</code> class. 
 */
package org.apache.hadoop.mapred;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.apache.commons.logging.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

public static class Group implements Writable, Iterable<Counter> {
    private String groupName;
    private String displayName;
    private Map<String, Counter> subcounters = new HashMap<String, Counter>();
    
    // Optional ResourceBundle for localization of group and counter names.
    private ResourceBundle bundle = null;    
    
    Group(String groupName) {
      try {
        bundle = getResourceBundle(groupName);
      }
      catch (MissingResourceException neverMind) {
      }
      this.groupName = groupName;
      this.displayName = localize("CounterGroupName", groupName);
      LOG.debug("Creating group " + groupName + " with " +
               (bundle == null ? "nothing" : "bundle"));
    }
    
    /**
     * Returns the specified resource bundle, or throws an exception.
     * @throws MissingResourceException if the bundle isn't found
     */
    private static ResourceBundle getResourceBundle(String enumClassName) {
      String bundleName = enumClassName.replace('$','_');
      return ResourceBundle.getBundle(bundleName);
    }
    
    /**
     * Returns raw name of the group.  This is the name of the enum class
     * for this group of counters.
     */
    public String getName() {
      return groupName;
    }
    
    /**
     * Returns localized name of the group.  This is the same as getName() by
     * default, but different if an appropriate ResourceBundle is found.
     */
    public String getDisplayName() {
      return displayName;
    }
    
    /**
     * Set the display name
     */
    public void setDisplayName(String displayName) {
      this.displayName = displayName;
    }
    
    /**
     * Returns the compact stringified version of the group in the format
     * {(actual-name)(display-name)(value)[][][]} where [] are compact strings for the
     * counters within.
     */
    public String makeEscapedCompactString() {
      StringBuffer buf = new StringBuffer();
      buf.append(GROUP_OPEN); // group start
      
      // Add the group name
      buf.append(UNIT_OPEN);
      buf.append(escape(getName()));
      buf.append(UNIT_CLOSE);
      
      // Add the display name
      buf.append(UNIT_OPEN);
      buf.append(escape(getDisplayName()));
      buf.append(UNIT_CLOSE);
      
      // write the value
      for(Counter counter: subcounters.values()) {
        buf.append(counter.makeEscapedCompactString());
      }
      
      buf.append(GROUP_CLOSE); // group end
      return buf.toString();
    }
        
    /** 
     * Checks for (content) equality of Groups
     */
    synchronized boolean contentEquals(Group g) {
      boolean isEqual = false;
      if (g != null) {
        if (size() == g.size()) {
          isEqual = true;
          for (Map.Entry<String, Counter> entry : subcounters.entrySet()) {
            String key = entry.getKey();
            Counter c1 = entry.getValue();
            Counter c2 = g.getCounterForName(key);
            if (!c1.contentEquals(c2)) {
              isEqual = false;
              break;
            }
          }
        }
      }
      return isEqual;
    }
    
    /**
     * Returns the value of the specified counter, or 0 if the counter does
     * not exist.
     */
    public synchronized long getCounter(String counterName) {
      for(Counter counter: subcounters.values()) {
        if (counter != null && counter.displayName.equals(counterName)) {
          return counter.value;
        }
      }
      return 0L;
    }
    
    /**
     * Get the counter for the given id and create it if it doesn't exist.
     * @param id the numeric id of the counter within the group
     * @param name the internal counter name
     * @return the counter
     * @deprecated use {@link #getCounter(String)} instead
     */
    @Deprecated
    public synchronized Counter getCounter(int id, String name) {
      return getCounterForName(name);
    }
    
    /**
     * Get the counter for the given name and create it if it doesn't exist.
     * @param name the internal counter name
     * @return the counter
     */
    public synchronized Counter getCounterForName(String name) {
      Counter result = subcounters.get(name);
      if (result == null) {
        LOG.debug("Adding " + name);
        result = new Counter(name, localize(name + ".name", name), 0L);
        subcounters.put(name, result);
      }
      return result;
    }
    
    /**
     * Returns the number of counters in this group.
     */
    public synchronized int size() {
      return subcounters.size();
    }
    
    /**
     * Looks up key in the ResourceBundle and returns the corresponding value.
     * If the bundle or the key doesn't exist, returns the default value.
     */
    private String localize(String key, String defaultValue) {
      String result = defaultValue;
      if (bundle != null) {
        try {
          result = bundle.getString(key);
        }
        catch (MissingResourceException mre) {
        }
      }
      return result;
    }
    
    public synchronized void write(DataOutput out) throws IOException {
      Text.writeString(out, displayName);
      WritableUtils.writeVInt(out, subcounters.size());
      for(Counter counter: subcounters.values()) {
        counter.write(out);
      }
    }
    
    public synchronized void readFields(DataInput in) throws IOException {
      displayName = Text.readString(in);
      subcounters.clear();
      int size = WritableUtils.readVInt(in);
      for(int i=0; i < size; i++) {
        Counter counter = new Counter();
        counter.readFields(in);
        subcounters.put(counter.getName(), counter);
      }
    }

    public synchronized Iterator<Counter> iterator() {
      return new ArrayList<Counter>(subcounters.values()).iterator();
    }
  }