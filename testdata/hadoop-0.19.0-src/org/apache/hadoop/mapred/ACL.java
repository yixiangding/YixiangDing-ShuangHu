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
 * Class that exposes information about queues maintained by the Hadoop
 * Map/Reduce framework.
 * 
 * The Map/Reduce framework can be configured with one or more queues,
 * depending on the scheduler it is configured with. While some 
 * schedulers work only with one queue, some schedulers support multiple 
 * queues.
 *  
 * Queues can be configured with various properties. Some of these
 * properties are common to all schedulers, and those are handled by this
 * class. Schedulers might also associate several custom properties with 
 * queues. Where such a case exists, the queue name must be used to link 
 * the common properties with the scheduler specific ones.  
 */
package org.apache.hadoop.mapred;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

private static class ACL {
    
    // Set of users who are granted access.
    private Set<String> users;
    // Set of groups which are granted access
    private Set<String> groups;
    // Whether all users are granted access.
    private boolean allAllowed;
    
    /**
     * Construct a new ACL from a String representation of the same.
     * 
     * The String is a a comma separated list of users and groups.
     * The user list comes first and is separated by a space followed 
     * by the group list. For e.g. "user1,user2 group1,group2"
     * 
     * @param aclString String representation of the ACL
     */
    ACL (String aclString) {
      users = new TreeSet<String>();
      groups = new TreeSet<String>();
      if (aclString.equals(ALL_ALLOWED_ACL_VALUE)) {
        allAllowed = true;
      } else {
        String[] userGroupStrings = aclString.split(" ", 2);
        
        if (userGroupStrings.length >= 1) {
          String[] usersStr = userGroupStrings[0].split(",");
          if (usersStr.length >= 1) {
            addToSet(users, usersStr);
          }
        }
        
        if (userGroupStrings.length == 2) {
          String[] groupsStr = userGroupStrings[1].split(",");
          if (groupsStr.length >= 1) {
            addToSet(groups, groupsStr);
          }
        }
      }
    }
    
    boolean allUsersAllowed() {
      return allAllowed;
    }
    
    boolean isUserAllowed(String user) {
      return users.contains(user);
    }
    
    boolean isAnyGroupAllowed(String[] otherGroups) {
      for (String g : otherGroups) {
        if (groups.contains(g)) {
          return true;
        }
      }
      return false;
    }
  }