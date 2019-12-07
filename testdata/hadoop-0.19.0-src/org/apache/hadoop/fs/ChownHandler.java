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
 * This class is the home for file permissions related commands.
 * Moved to this seperate class since FsShell is getting too large.
 */
package org.apache.hadoop.fs;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FsShell.CmdHandler;
import org.apache.hadoop.fs.permission.FsPermission;

private static class ChownHandler extends CmdHandler {
    protected String owner = null;
    protected String group = null;

    protected ChownHandler(String cmd, FileSystem fs) { //for chgrp
      super(cmd, fs);
    }

    ChownHandler(FileSystem fs, String ownerStr) throws IOException {
      super("chown", fs);
      Matcher matcher = chownPattern.matcher(ownerStr);
      if (!matcher.matches()) {
        throw new IOException("'" + ownerStr + "' does not match " +
                              "expected pattern for [owner][:group].");
      }
      owner = matcher.group(1);
      group = matcher.group(3);
      if (group != null && group.length() == 0) {
        group = null;
      }
      if (owner == null && group == null) {
        throw new IOException("'" + ownerStr + "' does not specify " +
                              " onwer or group.");
      }
    }

    @Override
    public void run(FileStatus file, FileSystem srcFs) throws IOException {
      //Should we do case insensitive match?  
      String newOwner = (owner == null || owner.equals(file.getOwner())) ?
                        null : owner;
      String newGroup = (group == null || group.equals(file.getGroup())) ?
                        null : group;

      if (newOwner != null || newGroup != null) {
        try {
          srcFs.setOwner(file.getPath(), newOwner, newGroup);
        } catch (IOException e) {
          System.err.println(getName() + ": changing ownership of '" + 
                             file.getPath() + "':" + e.getMessage());

        }
      }
    }
  }