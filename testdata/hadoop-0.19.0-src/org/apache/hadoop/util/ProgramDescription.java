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
/** A driver that is used to run programs added to it
 */
package org.apache.hadoop.util;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;

static private class ProgramDescription {
	
    static final Class<?>[] paramTypes = new Class<?>[] {String[].class};
	
    /**
     * Create a description of an example program.
     * @param mainClass the class with the main for the example program
     * @param description a string to display to the user in help messages
     * @throws SecurityException if we can't use reflection
     * @throws NoSuchMethodException if the class doesn't have a main method
     */
    public ProgramDescription(Class<?> mainClass, 
                              String description)
      throws SecurityException, NoSuchMethodException {
      this.main = mainClass.getMethod("main", paramTypes);
      this.description = description;
    }
	
    /**
     * Invoke the example application with the given arguments
     * @param args the arguments for the application
     * @throws Throwable The exception thrown by the invoked method
     */
    public void invoke(String[] args)
      throws Throwable {
      try {
        main.invoke(null, new Object[]{args});
      } catch (InvocationTargetException except) {
        throw except.getCause();
      }
    }
	
    public String getDescription() {
      return description;
    }
	
    private Method main;
    private String description;
  }