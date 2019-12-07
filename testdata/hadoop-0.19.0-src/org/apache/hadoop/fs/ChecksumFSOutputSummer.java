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
/****************************************************************
 * Abstract Checksumed FileSystem.
 * It provide a basice implementation of a Checksumed FileSystem,
 * which creates a checksum file for each raw file.
 * It generates & verifies checksums at the client side.
 *
 *****************************************************************/
package org.apache.hadoop.fs;
import java.io.*;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

private static class ChecksumFSOutputSummer extends FSOutputSummer {
    private FSDataOutputStream datas;    
    private FSDataOutputStream sums;
    private static final float CHKSUM_AS_FRACTION = 0.01f;
    
    public ChecksumFSOutputSummer(ChecksumFileSystem fs, 
                          Path file, 
                          boolean overwrite, 
                          short replication,
                          long blockSize,
                          Configuration conf)
      throws IOException {
      this(fs, file, overwrite, 
           conf.getInt("io.file.buffer.size", 4096),
           replication, blockSize, null);
    }
    
    public ChecksumFSOutputSummer(ChecksumFileSystem fs, 
                          Path file, 
                          boolean overwrite,
                          int bufferSize,
                          short replication,
                          long blockSize,
                          Progressable progress)
      throws IOException {
      super(new CRC32(), fs.getBytesPerSum(), 4);
      int bytesPerSum = fs.getBytesPerSum();
      this.datas = fs.getRawFileSystem().create(file, overwrite, bufferSize, 
                                         replication, blockSize, progress);
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file), true, 
                                               sumBufferSize, replication,
                                               blockSize);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }
    
    public void close() throws IOException {
      flushBuffer();
      sums.close();
      datas.close();
    }
    
    @Override
    protected void writeChunk(byte[] b, int offset, int len, byte[] checksum)
    throws IOException {
      datas.write(b, offset, len);
      sums.write(checksum);
    }
  }