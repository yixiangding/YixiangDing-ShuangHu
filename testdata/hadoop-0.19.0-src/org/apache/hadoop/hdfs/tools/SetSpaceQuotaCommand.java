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
 * This class provides some DFS administrative access.
 */
package org.apache.hadoop.hdfs.tools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem.DiskStatus;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

private static class SetSpaceQuotaCommand extends DFSAdminCommand {
    private static final String NAME = "setSpaceQuota";
    private static final String USAGE =
      "-"+NAME+" <quota> <dirname>...<dirname>";
    private static final String DESCRIPTION = USAGE + ": " +
      "Set the dik space quota <quota> for each directory <dirName>.\n" + 
      "\t\tThe directory quota is a long integer that puts a hard limit\n" +
      "\t\ton the number of names in the directory tree.\n" +
      "\t\tQuota can also be speciefied with a binary prefix for terabytes,\n" +
      "\t\tpetabytes etc (e.g. 50t is 50TB, 5m is 5MB, 3p is 3PB).\n" + 
      "\t\tBest effort for the directory, with faults reported if\n" +
      "\t\t1. N is not a positive integer, or\n" +
      "\t\t2. user is not an administrator, or\n" +
      "\t\t3. the directory does not exist or is a file, or\n" +
      "\t\t4. the directory would immediately exceed the new space quota.";
    
    private long quota; // the quota to be set
    
    /** Constructor */
    SetSpaceQuotaCommand(String[] args, int pos, FileSystem fs) {
      super(fs);
      CommandFormat c = new CommandFormat(NAME, 2, Integer.MAX_VALUE);
      List<String> parameters = c.parse(args, pos);
      String str = parameters.remove(0).trim();
      quota = StringUtils.TraditionalBinaryPrefix.string2long(str);
      this.args = parameters.toArray(new String[parameters.size()]);
    }
    
    /** Check if a command is the setQuota command
     * 
     * @param cmd A string representation of a command starting with "-"
     * @return true if this is a count command; false otherwise
     */
    public static boolean matches(String cmd) {
      return ("-"+NAME).equals(cmd); 
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    public void run(Path path) throws IOException {
      dfs.setQuota(path, FSConstants.QUOTA_DONT_SET, quota);
    }
  }