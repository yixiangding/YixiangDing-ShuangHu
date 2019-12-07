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
 * A block-based {@link FileSystem} backed by
 * <a href="http://aws.amazon.com/s3">Amazon S3</a>.
 * </p>
 * @see NativeS3FileSystem
 */
package org.apache.hadoop.fs.s3;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.util.Progressable;

private static class S3FileStatus extends FileStatus {

    S3FileStatus(Path f, INode inode) throws IOException {
      super(findLength(inode), inode.isDirectory(), 1,
            findBlocksize(inode), 0, f);
    }

    private static long findLength(INode inode) {
      if (!inode.isDirectory()) {
        long length = 0L;
        for (Block block : inode.getBlocks()) {
          length += block.getLength();
        }
        return length;
      }
      return 0;
    }

    private static long findBlocksize(INode inode) {
      final Block[] ret = inode.getBlocks();
      return ret == null ? 0L : ret[0].getLength();
    }
  }