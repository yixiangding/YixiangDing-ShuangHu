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
 * This is an implementation of the Hadoop Archive 
 * Filesystem. This archive Filesystem has index files
 * of the form _index* and has contents of the form
 * part-*. The index files store the indexes of the 
 * real files. The index files are of the form _masterindex
 * and _index. The master index is a level of indirection 
 * in to the index file to make the look ups faster. the index
 * file is sorted with hash code of the paths that it contains 
 * and the master index contains pointers to the positions in 
 * index for ranges of hashcodes.
 */
package org.apache.hadoop.fs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Progressable;

private static class HarFSDataInputStream extends FSDataInputStream {
    /**
     * Create an input stream that fakes all the reads/positions/seeking.
     */
    private static class HarFsInputStream extends FSInputStream {
      private long position, start, end;
      //The underlying data input stream that the
      // underlying filesystem will return.
      private FSDataInputStream underLyingStream;
      //one byte buffer
      private byte[] oneBytebuff = new byte[1];
      HarFsInputStream(FileSystem fs, Path path, long start,
          long length, int bufferSize) throws IOException {
        underLyingStream = fs.open(path, bufferSize);
        underLyingStream.seek(start);
        // the start of this file in the part file
        this.start = start;
        // the position pointer in the part file
        this.position = start;
        // the end pointer in the part file
        this.end = start + length;
      }
      
      public synchronized int available() throws IOException {
        long remaining = end - underLyingStream.getPos();
        if (remaining > (long)Integer.MAX_VALUE) {
          return Integer.MAX_VALUE;
        }
        return (int) remaining;
      }
      
      public synchronized  void close() throws IOException {
        underLyingStream.close();
        super.close();
      }
      
      //not implemented
      @Override
      public void mark(int readLimit) {
        // do nothing 
      }
      
      /**
       * reset is not implemented
       */
      public void reset() throws IOException {
        throw new IOException("reset not implemented.");
      }
      
      public synchronized int read() throws IOException {
        int ret = read(oneBytebuff, 0, 1);
        return (ret <= 0) ? -1: (oneBytebuff[0] & 0xff);
      }
      
      public synchronized int read(byte[] b) throws IOException {
        int ret = read(b, 0, b.length);
        if (ret != -1) {
          position += ret;
        }
        return ret;
      }
      
      /**
       * 
       */
      public synchronized int read(byte[] b, int offset, int len) 
        throws IOException {
        int newlen = len;
        int ret = -1;
        if (position + len > end) {
          newlen = (int) (end - position);
        }
        // end case
        if (newlen == 0) 
          return ret;
        ret = underLyingStream.read(b, offset, newlen);
        position += ret;
        return ret;
      }
      
      public synchronized long skip(long n) throws IOException {
        long tmpN = n;
        if (tmpN > 0) {
          if (position + tmpN > end) {
            tmpN = end - position;
          }
          underLyingStream.seek(tmpN + position);
          position += tmpN;
          return tmpN;
        }
        return (tmpN < 0)? -1 : 0;
      }
      
      public synchronized long getPos() throws IOException {
        return (position - start);
      }
      
      public synchronized void seek(long pos) throws IOException {
        if (pos < 0 || (start + pos > end)) {
          throw new IOException("Failed to seek: EOF");
        }
        position = start + pos;
        underLyingStream.seek(position);
      }

      public boolean seekToNewSource(long targetPos) throws IOException {
        //do not need to implement this
        // hdfs in itself does seektonewsource 
        // while reading.
        return false;
      }
      
      /**
       * implementing position readable. 
       */
      public int read(long pos, byte[] b, int offset, int length) 
      throws IOException {
        int nlength = length;
        if (start + nlength + pos > end) {
          nlength = (int) (end - (start + pos));
        }
        return underLyingStream.read(pos + start , b, offset, nlength);
      }
      
      /**
       * position readable again.
       */
      public void readFully(long pos, byte[] b, int offset, int length) 
      throws IOException {
        if (start + length + pos > end) {
          throw new IOException("Not enough bytes to read.");
        }
        underLyingStream.readFully(pos + start, b, offset, length);
      }
      
      public void readFully(long pos, byte[] b) throws IOException {
          readFully(pos, b, 0, b.length);
      }
      
    }
  
    /**
     * constructors for har input stream.
     * @param fs the underlying filesystem
     * @param p The path in the underlying filesystem
     * @param start the start position in the part file
     * @param length the length of valid data in the part file
     * @param bufsize the buffer size
     * @throws IOException
     */
    public HarFSDataInputStream(FileSystem fs, Path  p, long start, 
        long length, int bufsize) throws IOException {
        super(new HarFsInputStream(fs, p, start, length, bufsize));
    }

    /**
     * constructor for har input stream.
     * @param fs the underlying filesystem
     * @param p the path in the underlying file system
     * @param start the start position in the part file
     * @param length the length of valid data in the part file.
     * @throws IOException
     */
    public HarFSDataInputStream(FileSystem fs, Path  p, long start, long length)
      throws IOException {
        super(new HarFsInputStream(fs, p, start, length, 0));
    }
  }