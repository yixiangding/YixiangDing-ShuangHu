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

public static class Servlet extends HttpServlet {
    
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response) throws IOException {
      
      response.setContentType("text/plain");
      
      DataBlockScanner blockScanner = (DataBlockScanner)  
          getServletContext().getAttribute("datanode.blockScanner");
      
      boolean summary = (request.getParameter("listblocks") == null);
      
      StringBuilder buffer = new StringBuilder(8*1024);
      if (blockScanner == null) {
        buffer.append("Periodic block scanner is not running. " +
                      "Please check the datanode log if this is unexpected.");
      } else if (blockScanner.isInitiliazed()) {
        blockScanner.printBlockReport(buffer, summary);
      } else {
        buffer.append("Periodic block scanner is not yet initialized. " +
                      "Please check back again after some time.");
      }
      response.getWriter().write(buffer.toString()); // extra copy!
    }
  }