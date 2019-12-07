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
/*******************************************************
 * TaskTracker is a process that starts and tracks MR Tasks
 * in a networked environment.  It contacts the JobTracker
 * for Task assignments and reporting results.
 *
 *******************************************************/
package org.apache.hadoop.mapred;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.LinkedHashMap;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.mapred.JobClient.TaskStatusFilter;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.pipes.Submitter;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.ProcfsBasedProcessTree;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.RunJar;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.log4j.LogManager;

public static class MapOutputServlet extends HttpServlet {
    private static final int MAX_BYTES_TO_READ = 64 * 1024;
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      String mapId = request.getParameter("map");
      String reduceId = request.getParameter("reduce");
      String jobId = request.getParameter("job");

      if (jobId == null) {
        throw new IOException("job parameter is required");
      }

      if (mapId == null || reduceId == null) {
        throw new IOException("map and reduce parameters are required");
      }
      ServletContext context = getServletContext();
      int reduce = Integer.parseInt(reduceId);
      byte[] buffer = new byte[MAX_BYTES_TO_READ];
      // true iff IOException was caused by attempt to access input
      boolean isInputException = true;
      OutputStream outStream = null;
      FSDataInputStream mapOutputIn = null;
 
      IFileInputStream checksumInputStream = null;
      
      long totalRead = 0;
      ShuffleServerMetrics shuffleMetrics = (ShuffleServerMetrics)
                                      context.getAttribute("shuffleServerMetrics");
      TaskTracker tracker = 
        (TaskTracker) context.getAttribute("task.tracker");

      try {
        shuffleMetrics.serverHandlerBusy();
        outStream = response.getOutputStream();
        JobConf conf = (JobConf) context.getAttribute("conf");
        LocalDirAllocator lDirAlloc = 
          (LocalDirAllocator)context.getAttribute("localDirAllocator");
        FileSystem fileSys = 
          (FileSystem) context.getAttribute("local.file.system");

        // Index file
        Path indexFileName = lDirAlloc.getLocalPathToRead(
            TaskTracker.getJobCacheSubdir() + Path.SEPARATOR + 
            jobId + Path.SEPARATOR +
            mapId + "/output" + "/file.out.index", conf);
        
        // Map-output file
        Path mapOutputFileName = lDirAlloc.getLocalPathToRead(
            TaskTracker.getJobCacheSubdir() + Path.SEPARATOR + 
            jobId + Path.SEPARATOR +
            mapId + "/output" + "/file.out", conf);

        /**
         * Read the index file to get the information about where
         * the map-output for the given reducer is available. 
         */
       IndexRecord info = 
          tracker.indexCache.getIndexInformation(mapId, reduce,indexFileName);
          
        final long startOffset = info.startOffset;
        final long rawPartLength = info.rawLength;
        final long partLength = info.partLength;

        //set the custom "Raw-Map-Output-Length" http header to 
        //the raw (decompressed) length
        response.setHeader(RAW_MAP_OUTPUT_LENGTH, Long.toString(rawPartLength));

        //set the custom "Map-Output-Length" http header to 
        //the actual number of bytes being transferred
        response.setHeader(MAP_OUTPUT_LENGTH, 
                           Long.toString(partLength));

        //use the same buffersize as used for reading the data from disk
        response.setBufferSize(MAX_BYTES_TO_READ);
        
        /**
         * Read the data from the sigle map-output file and
         * send it to the reducer.
         */
        //open the map-output file
        FileSystem rfs = ((LocalFileSystem)fileSys).getRaw();

        mapOutputIn = rfs.open(mapOutputFileName);
        // TODO: Remove this after a 'fix' for HADOOP-3647
        // The clever trick here to reduce the impact of the extra seek for
        // logging the first key/value lengths is to read the lengths before
        // the second seek for the actual shuffle. The second seek is almost
        // a no-op since it is very short (go back length of two VInts) and the 
        // data is almost guaranteed to be in the filesystem's buffers.
        // WARN: This won't work for compressed map-outputs!
        int firstKeyLength = 0;
        int firstValueLength = 0;
        if (partLength > 0) {
          mapOutputIn.seek(startOffset);
          firstKeyLength = WritableUtils.readVInt(mapOutputIn);
          firstValueLength = WritableUtils.readVInt(mapOutputIn);
        }
        

        //seek to the correct offset for the reduce
        mapOutputIn.seek(startOffset);
        checksumInputStream = new IFileInputStream(mapOutputIn,partLength);
          
        int len = checksumInputStream.readWithChecksum(buffer, 0,
                                   partLength < MAX_BYTES_TO_READ 
                                   ? (int)partLength : MAX_BYTES_TO_READ);
        while (len > 0) {
          try {
            shuffleMetrics.outputBytes(len);
            outStream.write(buffer, 0, len);
            outStream.flush();
          } catch (IOException ie) {
            isInputException = false;
            throw ie;
          }
          totalRead += len;
          if (totalRead == partLength) break;
          len = checksumInputStream.readWithChecksum(buffer, 0, 
                        (partLength - totalRead) < MAX_BYTES_TO_READ
                          ? (int)(partLength - totalRead) : MAX_BYTES_TO_READ);
        }
        
        LOG.info("Sent out " + totalRead + " bytes for reduce: " + reduce + 
                 " from map: " + mapId + " given " + partLength + "/" + 
                 rawPartLength + " from " + startOffset + " with (" + 
                 firstKeyLength + ", " + firstValueLength + ")");
      } catch (IOException ie) {
        Log log = (Log) context.getAttribute("log");
        String errorMsg = ("getMapOutput(" + mapId + "," + reduceId + 
                           ") failed :\n"+
                           StringUtils.stringifyException(ie));
        log.warn(errorMsg);
        if (isInputException) {
          tracker.mapOutputLost(TaskAttemptID.forName(mapId), errorMsg);
        }
        response.sendError(HttpServletResponse.SC_GONE, errorMsg);
        shuffleMetrics.failedOutput();
        throw ie;
      } finally {
        if (checksumInputStream != null) {
          checksumInputStream.close();
        }
        shuffleMetrics.serverHandlerFree();
        if (ClientTraceLog.isInfoEnabled()) {
          ClientTraceLog.info(String.format(MR_CLIENTTRACE_FORMAT,
                request.getLocalAddr() + ":" + request.getLocalPort(),
                request.getRemoteAddr() + ":" + request.getRemotePort(),
                totalRead, "MAPRED_SHUFFLE", mapId));
        }
      }
      outStream.close();
      shuffleMetrics.successOutput();
    }
  }