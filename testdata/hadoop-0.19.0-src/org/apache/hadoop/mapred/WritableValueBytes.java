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
 * An {@link OutputFormat} that writes keys, values to 
 * {@link SequenceFile}s in binary(raw) format
 */
package org.apache.hadoop.mapred;
import java.io.IOException;
import java.io.DataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Progressable;

static protected class WritableValueBytes implements ValueBytes {
    private BytesWritable value;

    public WritableValueBytes() {
      this.value = null;
    }
    public WritableValueBytes(BytesWritable value) {
      this.value = value;
    }

    public void reset(BytesWritable value) {
      this.value = value;
    }

    public void writeUncompressedBytes(DataOutputStream outStream)
      throws IOException {
      outStream.write(value.getBytes(), 0, value.getLength());
    }

    public void writeCompressedBytes(DataOutputStream outStream)
      throws IllegalArgumentException, IOException {
      throw
        new UnsupportedOperationException("WritableValueBytes doesn't support " 
                                          + "RECORD compression"); 
    }
    public int getSize(){
      return value.getLength();
    }
  }