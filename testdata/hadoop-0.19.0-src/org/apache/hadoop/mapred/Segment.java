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
package org.apache.hadoop.mapred;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.IFile.Reader;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.util.PriorityQueue;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;

public static class Segment<K extends Object, V extends Object> {
    Reader<K, V> reader = null;
    DataInputBuffer key = new DataInputBuffer();
    DataInputBuffer value = new DataInputBuffer();
    
    Configuration conf = null;
    FileSystem fs = null;
    Path file = null;
    boolean preserve = false;
    CompressionCodec codec = null;
    long segmentLength = -1;
    
    public Segment(Configuration conf, FileSystem fs, Path file,
                   CompressionCodec codec, boolean preserve) throws IOException {
      this.conf = conf;
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.preserve = preserve;
      
      this.segmentLength = fs.getFileStatus(file).getLen();
    }
    
    public Segment(Reader<K, V> reader, boolean preserve) {
      this.reader = reader;
      this.preserve = preserve;
      
      this.segmentLength = reader.getLength();
    }

    private void init() throws IOException {
      if (reader == null) {
        reader = new Reader<K, V>(conf, fs, file, codec);
      }
    }
    
    DataInputBuffer getKey() { return key; }
    DataInputBuffer getValue() { return value; }

    long getLength() { 
      return (reader == null) ?
        segmentLength : reader.getLength();
    }
    
    boolean next() throws IOException {
      return reader.next(key, value);
    }
    
    void close() throws IOException {
      reader.close();
      
      if (!preserve && fs != null) {
        fs.delete(file, false);
      }
    }

    public long getPosition() throws IOException {
      return reader.getPosition();
    }
  }