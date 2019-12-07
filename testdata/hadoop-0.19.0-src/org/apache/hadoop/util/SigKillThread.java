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
 * A Proc file-system based ProcessTree. Works only on Linux.
 */
package org.apache.hadoop.util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.LinkedList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

private class SigKillThread extends Thread {

    public void run() {
      this.setName(this.getClass().getName() + "-" + String.valueOf(pid));
      ShellCommandExecutor shexec = null;

      try {
        // Sleep for some time before sending SIGKILL
        Thread.sleep(sleepTimeBeforeSigKill);
      } catch (InterruptedException i) {
        LOG.warn("Thread sleep is interrupted.");
      }

      // Kill the root process with SIGKILL if it is still alive
      if (ProcfsBasedProcessTree.this.isAlive(pid)) {
        try {
          String[] args = { "kill", "-9", pid.toString() };
          shexec = new ShellCommandExecutor(args);
          shexec.execute();
        } catch (IOException ioe) {
          LOG.warn("Error executing shell command " + ioe);
        } finally {
          LOG.info("Killing " + pid + " with SIGKILL. Exit code "
              + shexec.getExitCode());
        }
      }
    }
  }