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
/** An implementation of the in-memory filesystem. This implementation assumes
 * that the file lengths are known ahead of time and the total lengths of all
 * the files is below a certain number (like 100 MB, configurable). Use the API
 * reserveSpaceWithCheckSum(Path f, int size) (see below for a description of
 * the API for reserving space in the FS. The uri of this filesystem starts with
 * ramfs:// .
 */
package org.apache.hadoop.fs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Progressable;

private class InMemoryOutputStream extends OutputStream {
      private int count;
      private FileAttributes fAttr;
      private Path f;
    
      public InMemoryOutputStream(Path f, FileAttributes fAttr) 
        throws IOException {
        this.fAttr = fAttr;
        this.f = f;
      }
    
      public long getPos() throws IOException {
        return count;
      }
    
      public void close() throws IOException {
        synchronized (RawInMemoryFileSystem.this) {
          pathToFileAttribs.put(getPath(f), fAttr);
        }
      }
    
      public void write(byte[] b, int off, int len) throws IOException {
        if ((off < 0) || (off > b.length) || (len < 0) ||
            ((off + len) > b.length) || ((off + len) < 0)) {
          throw new IndexOutOfBoundsException();
        } else if (len == 0) {
          return;
        }
        int newcount = count + len;
        if (newcount > fAttr.size) {
          throw new IOException("Insufficient space");
        }
        System.arraycopy(b, off, fAttr.data, count, len);
        count = newcount;
      }
    
      public void write(int b) throws IOException {
        int newcount = count + 1;
        if (newcount > fAttr.size) {
          throw new IOException("Insufficient space");
        }
        fAttr.data[count] = (byte)b;
        count = newcount;
      }
    }