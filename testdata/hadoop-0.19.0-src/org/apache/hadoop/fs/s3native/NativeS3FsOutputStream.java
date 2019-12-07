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
 * <p>
 * A {@link FileSystem} for reading and writing files stored on
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * Unlike {@link org.apache.hadoop.fs.s3.S3FileSystem} this implementation
 * stores files on S3 in their
 * native form so they can be read by other S3 tools.
 * </p>
 * @see org.apache.hadoop.fs.s3.S3FileSystem
 */
package org.apache.hadoop.fs.s3native;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Exception;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

private class NativeS3FsOutputStream extends OutputStream {
    
    private Configuration conf;
    private String key;
    private File backupFile;
    private OutputStream backupStream;
    private MessageDigest digest;
    private boolean closed;
    
    public NativeS3FsOutputStream(Configuration conf,
        NativeFileSystemStore store, String key, Progressable progress,
        int bufferSize) throws IOException {
      this.conf = conf;
      this.key = key;
      this.backupFile = newBackupFile();
      try {
        this.digest = MessageDigest.getInstance("MD5");
        this.backupStream = new BufferedOutputStream(new DigestOutputStream(
            new FileOutputStream(backupFile), this.digest));
      } catch (NoSuchAlgorithmException e) {
        LOG.warn("Cannot load MD5 digest algorithm," +
            "skipping message integrity check.", e);
        this.backupStream = new BufferedOutputStream(
            new FileOutputStream(backupFile));
      }
    }

    private File newBackupFile() throws IOException {
      File dir = new File(conf.get("fs.s3.buffer.dir"));
      if (!dir.exists() && !dir.mkdirs()) {
        throw new IOException("Cannot create S3 buffer directory: " + dir);
      }
      File result = File.createTempFile("output-", ".tmp", dir);
      result.deleteOnExit();
      return result;
    }
    
    @Override
    public void flush() throws IOException {
      backupStream.flush();
    }
    
    @Override
    public synchronized void close() throws IOException {
      if (closed) {
        throw new IOException("Stream closed");
      }

      backupStream.close();
      
      try {
        byte[] md5Hash = digest == null ? null : digest.digest();
        store.storeFile(key, backupFile, md5Hash);
      } finally {
        if (!backupFile.delete()) {
          LOG.warn("Could not delete temporary s3n file: " + backupFile);
        }
        super.close();
        closed = true;
      } 

    }

    @Override
    public void write(int b) throws IOException {
      backupStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      backupStream.write(b, off, len);
    }
    
    
  }