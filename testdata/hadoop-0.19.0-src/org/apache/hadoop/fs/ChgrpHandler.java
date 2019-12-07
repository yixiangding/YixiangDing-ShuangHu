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

private static class ChgrpHandler extends ChownHandler {
    ChgrpHandler(FileSystem fs, String groupStr) throws IOException {
      super("chgrp", fs);

      Matcher matcher = chgrpPattern.matcher(groupStr);
      if (!matcher.matches()) {
        throw new IOException("'" + groupStr + "' does not match " +
        "expected pattern for group");
      }
      group = matcher.group(1);
    }
  }