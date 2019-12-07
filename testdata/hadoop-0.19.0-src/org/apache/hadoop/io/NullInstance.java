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
/** A polymorphic Writable that writes an instance with it's class name.
 * Handles arrays, strings and primitive types without a Writable wrapper.
 */
package org.apache.hadoop.io;
import java.lang.reflect.Array;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;

private static class NullInstance extends Configured implements Writable {
    private Class<?> declaredClass;
    public NullInstance() { super(null); }
    public NullInstance(Class declaredClass, Configuration conf) {
      super(conf);
      this.declaredClass = declaredClass;
    }
    public void readFields(DataInput in) throws IOException {
      String className = UTF8.readString(in);
      declaredClass = PRIMITIVE_NAMES.get(className);
      if (declaredClass == null) {
        try {
          declaredClass = getConf().getClassByName(className);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e.toString());
        }
      }
    }
    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, declaredClass.getName());
    }
  }