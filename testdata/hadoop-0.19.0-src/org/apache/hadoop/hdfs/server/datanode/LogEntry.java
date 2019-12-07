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

static private class LogEntry {
    long blockId = -1;
    long verificationTime = -1;
    long genStamp = Block.GRANDFATHER_GENERATION_STAMP;
    
    /**
     * The format consists of single line with multiple entries. each 
     * entry is in the form : name="value".
     * This simple text and easily extendable and easily parseable with a
     * regex.
     */
    private static Pattern entryPattern = 
      Pattern.compile("\\G\\s*([^=\\p{Space}]+)=\"(.*?)\"\\s*");
    
    static String newEnry(Block block, long time) {
      return "date=\"" + dateFormat.format(new Date(time)) + "\"\t " +
             "time=\"" + time + "\"\t " +
             "genstamp=\"" + block.getGenerationStamp() + "\"\t " +
             "id=\"" + block.getBlockId() +"\"";
    }
    
    static LogEntry parseEntry(String line) {
      LogEntry entry = new LogEntry();
      
      Matcher matcher = entryPattern.matcher(line);
      while (matcher.find()) {
        String name = matcher.group(1);
        String value = matcher.group(2);
        
        try {
          if (name.equals("id")) {
            entry.blockId = Long.valueOf(value);
          } else if (name.equals("time")) {
            entry.verificationTime = Long.valueOf(value);
          } else if (name.equals("genstamp")) {
            entry.genStamp = Long.valueOf(value);
          }
        } catch(NumberFormatException nfe) {
          LOG.warn("Cannot parse line: " + line, nfe);
          return null;
        }
      }
      
      return entry;
    }
  }