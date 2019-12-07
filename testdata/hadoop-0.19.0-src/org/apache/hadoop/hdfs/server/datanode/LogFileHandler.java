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
/*
 * This keeps track of blocks and their last verification times.
 * Currently it does not modify the metadata for block.
 */
package org.apache.hadoop.hdfs.server.datanode;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;

private static class LogFileHandler {
    
    private static final String curFileSuffix = ".curr";
    private static final String prevFileSuffix = ".prev";
    
    // Don't roll files more often than this
    private static final long minRollingPeriod = 6 * 3600 * 1000L; // 6 hours
    private static final long minWarnPeriod = minRollingPeriod;
    private static final int minLineLimit = 1000;
    
    
    static boolean isFilePresent(File dir, String filePrefix) {
      return new File(dir, filePrefix + curFileSuffix).exists() ||
             new File(dir, filePrefix + prevFileSuffix).exists();
    }
    private File curFile;
    private File prevFile;
    
    private int maxNumLines = -1; // not very hard limit on number of lines.
    private int curNumLines = -1;
    
    long lastWarningTime = 0;
    
    private PrintStream out;
    
    int numReaders = 0;
        
    /**
     * Opens the log file for appending.
     * Note that rolling will happen only after "updateLineCount()" is 
     * called. This is so that line count could be updated in a separate
     * thread without delaying start up.
     * 
     * @param dir where the logs files are located.
     * @param filePrefix prefix of the file.
     * @param maxNumLines max lines in a file (its a soft limit).
     * @throws IOException
     */
    LogFileHandler(File dir, String filePrefix, int maxNumLines) 
                                                throws IOException {
      curFile = new File(dir, filePrefix + curFileSuffix);
      prevFile = new File(dir, filePrefix + prevFileSuffix);
      openCurFile();
      curNumLines = -1;
      setMaxNumLines(maxNumLines);
    }
    
    // setting takes affect when next entry is added.
    synchronized void setMaxNumLines(int maxNumLines) {
      this.maxNumLines = Math.max(maxNumLines, minLineLimit);
    }
    
    /**
     * Append "\n" + line.
     * If the log file need to be rolled, it will done after 
     * appending the text.
     * This does not throw IOException when there is an error while 
     * appending. Currently does not throw an error even if rolling 
     * fails (may be it should?).
     * return true if append was successful.
     */
    synchronized boolean appendLine(String line) {
      out.println();
      out.print(line);
      curNumLines += (curNumLines < 0) ? -1 : 1;
      try {
        rollIfRequired();
      } catch (IOException e) {
        warn("Rolling failed for " + curFile + " : " + e.getMessage());
        return false;
      }
      return true;
    }
    
    //warns only once in a while
    synchronized private void warn(String msg) {
      long now = System.currentTimeMillis();
      if ((now - lastWarningTime) >= minWarnPeriod) {
        lastWarningTime = now;
        LOG.warn(msg);
      }
    }
    
    private synchronized void openCurFile() throws FileNotFoundException {
      close();
      out = new PrintStream(new FileOutputStream(curFile, true));
    }
    
    //This reads the current file and updates the count.
    void updateCurNumLines() {
      int count = 0;
      try {
        for(Reader it = new Reader(true); it.hasNext(); count++) {
          it.next();
        }
      } catch (IOException e) {
        
      } finally {
        synchronized (this) {
          curNumLines = count;
        }
      }
    }
    
    private void rollIfRequired() throws IOException {
      if (curNumLines < maxNumLines || numReaders > 0) {
        return;
      }
      
      long now = System.currentTimeMillis();
      if (now < minRollingPeriod) {
        return;
      }
      
      if (!prevFile.delete() && prevFile.exists()) {
        throw new IOException("Could not delete " + prevFile);
      }
      
      close();

      if (!curFile.renameTo(prevFile)) {
        openCurFile();
        throw new IOException("Could not rename " + curFile + 
                              " to " + prevFile);
      }
      
      openCurFile();
      updateCurNumLines();
    }
    
    synchronized void close() {
      if (out != null) {
        out.close();
        out = null;
      }
    }
    
    /**
     * This is used to read the lines in order.
     * If the data is not read completely (i.e, untill hasNext() returns
     * false), it needs to be explicitly 
     */
    private class Reader implements Iterator<String>, Closeable {
      
      BufferedReader reader;
      File file;
      String line;
      boolean closed = false;
      
      private Reader(boolean skipPrevFile) throws IOException {
        synchronized (LogFileHandler.this) {
          numReaders++; 
        }
        reader = null;
        file = (skipPrevFile) ? curFile : prevFile;
        readNext();        
      }
      
      private boolean openFile() throws IOException {

        for(int i=0; i<2; i++) {
          if (reader != null || i > 0) {
            // move to next file
            file = (file == prevFile) ? curFile : null;
          }
          if (file == null) {
            return false;
          }
          if (file.exists()) {
            break;
          }
        }
        
        if (reader != null ) {
          reader.close();
          reader = null;
        }
        
        reader = new BufferedReader(new FileReader(file));
        return true;
      }
      
      // read next line if possible.
      private void readNext() throws IOException {
        line = null;
        try {
          if (reader != null && (line = reader.readLine()) != null) {
            return;
          }
          if (line == null) {
            // move to the next file.
            if (openFile()) {
              readNext();
            }
          }
        } finally {
          if (!hasNext()) {
            close();
          }
        }
      }
      
      public boolean hasNext() {
        return line != null;
      }

      public String next() {
        String curLine = line;
        try {
          readNext();
        } catch (IOException e) {
          LOG.info("Could not reade next line in LogHandler : " +
                   StringUtils.stringifyException(e));
        }
        return curLine;
      }

      public void remove() {
        throw new RuntimeException("remove() is not supported.");
      }

      public void close() throws IOException {
        if (!closed) {
          try {
            if (reader != null) {
              reader.close();
            }
          } finally {
            file = null;
            reader = null;
            closed = true;
            synchronized (LogFileHandler.this) {
              numReaders--;
              assert(numReaders >= 0);
            }
          }
        }
      }
    }    
  }