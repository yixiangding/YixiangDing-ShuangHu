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
 * A collection of file-processing util methods
 */
package org.apache.hadoop.fs;
import java.io.*;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

public static class HardLink { 
    enum OSType {
      OS_TYPE_UNIX, 
      OS_TYPE_WINXP,
      OS_TYPE_SOLARIS,
      OS_TYPE_MAC; 
    }
  
    private static String[] hardLinkCommand;
    private static String[] getLinkCountCommand;
    private static OSType osType;
    
    static {
      osType = getOSType();
      switch(osType) {
      case OS_TYPE_WINXP:
        hardLinkCommand = new String[] {"fsutil","hardlink","create", null, null};
        getLinkCountCommand = new String[] {"stat","-c%h"};
        break;
      case OS_TYPE_SOLARIS:
        hardLinkCommand = new String[] {"ln", null, null};
        getLinkCountCommand = new String[] {"ls","-l"};
        break;
      case OS_TYPE_MAC:
        hardLinkCommand = new String[] {"ln", null, null};
        getLinkCountCommand = new String[] {"stat","-f%l"};
        break;
      case OS_TYPE_UNIX:
      default:
        hardLinkCommand = new String[] {"ln", null, null};
        getLinkCountCommand = new String[] {"stat","-c%h"};
      }
    }

    static private OSType getOSType() {
      String osName = System.getProperty("os.name");
      if (osName.indexOf("Windows") >= 0 && 
          (osName.indexOf("XP") >= 0 || osName.indexOf("2003") >= 0 || osName.indexOf("Vista") >= 0))
        return OSType.OS_TYPE_WINXP;
      else if (osName.indexOf("SunOS") >= 0)
         return OSType.OS_TYPE_SOLARIS;
      else if (osName.indexOf("Mac") >= 0)
         return OSType.OS_TYPE_MAC;
      else
        return OSType.OS_TYPE_UNIX;
    }
    
    /**
     * Creates a hardlink 
     */
    public static void createHardLink(File target, 
                                      File linkName) throws IOException {
      int len = hardLinkCommand.length;
      if (osType == OSType.OS_TYPE_WINXP) {
       hardLinkCommand[len-1] = target.getCanonicalPath();
       hardLinkCommand[len-2] = linkName.getCanonicalPath();
      } else {
       hardLinkCommand[len-2] = makeShellPath(target, true);
       hardLinkCommand[len-1] = makeShellPath(linkName, true);
      }
      // execute shell command
      Process process = Runtime.getRuntime().exec(hardLinkCommand);
      try {
        if (process.waitFor() != 0) {
          String errMsg = new BufferedReader(new InputStreamReader(
                                                                   process.getInputStream())).readLine();
          if (errMsg == null)  errMsg = "";
          String inpMsg = new BufferedReader(new InputStreamReader(
                                                                   process.getErrorStream())).readLine();
          if (inpMsg == null)  inpMsg = "";
          throw new IOException(errMsg + inpMsg);
        }
      } catch (InterruptedException e) {
        throw new IOException(StringUtils.stringifyException(e));
      } finally {
        process.destroy();
      }
    }

    /**
     * Retrieves the number of links to the specified file.
     */
    public static int getLinkCount(File fileName) throws IOException {
      int len = getLinkCountCommand.length;
      String[] cmd = new String[len + 1];
      for (int i = 0; i < len; i++) {
        cmd[i] = getLinkCountCommand[i];
      }
      cmd[len] = fileName.toString();
      String inpMsg = "";
      String errMsg = "";
      int exitValue = -1;
      BufferedReader in = null;
      BufferedReader err = null;

      // execute shell command
      Process process = Runtime.getRuntime().exec(cmd);
      try {
        exitValue = process.waitFor();
        in = new BufferedReader(new InputStreamReader(
                                    process.getInputStream()));
        inpMsg = in.readLine();
        if (inpMsg == null)  inpMsg = "";
        
        err = new BufferedReader(new InputStreamReader(
                                     process.getErrorStream()));
        errMsg = err.readLine();
        if (errMsg == null)  errMsg = "";
        if (exitValue != 0) {
          throw new IOException(inpMsg + errMsg);
        }
        if (getOSType() == OSType.OS_TYPE_SOLARIS) {
          String[] result = inpMsg.split("\\s+");
          return Integer.parseInt(result[1]);
        } else {
          return Integer.parseInt(inpMsg);
        }
      } catch (NumberFormatException e) {
        throw new IOException(StringUtils.stringifyException(e) + 
                              inpMsg + errMsg +
                              " on file:" + fileName);
      } catch (InterruptedException e) {
        throw new IOException(StringUtils.stringifyException(e) + 
                              inpMsg + errMsg +
                              " on file:" + fileName);
      } finally {
        process.destroy();
        if (in != null) in.close();
        if (err != null) err.close();
      }
    }
  }