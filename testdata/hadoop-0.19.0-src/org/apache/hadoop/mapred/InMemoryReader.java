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
 * <code>IFile</code> is the simple <key-len, key, value-len, value> format
 * for the intermediate map-outputs in Map-Reduce.
 * 
 * There is a <code>Writer</code> to write out map-outputs in this format and 
 * a <code>Reader</code> to read files of this format.
 */
package org.apache.hadoop.mapred;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

public static class InMemoryReader<K, V> extends Reader<K, V> {
    RamManager ramManager;
    TaskAttemptID taskAttemptId;
    
    public InMemoryReader(RamManager ramManager, TaskAttemptID taskAttemptId,
                          byte[] data, int start, int length)
                          throws IOException {
      super(null, null, length - start, null);
      this.ramManager = ramManager;
      this.taskAttemptId = taskAttemptId;
      
      buffer = data;
      bufferSize = (int)fileLength;
      dataIn.reset(buffer, start, length);
    }
    
    @Override
    public long getPosition() throws IOException {
      // InMemoryReader does not initialize streams like Reader, so in.getPos()
      // would not work. Instead, return the number of uncompressed bytes read,
      // which will be correct since in-memory data is not compressed.
      return bytesRead;
    }
    
    @Override
    public long getLength() { 
      return fileLength;
    }
    
    private void dumpOnError() {
      File dumpFile = new File("../output/" + taskAttemptId + ".dump");
      System.err.println("Dumping corrupt map-output of " + taskAttemptId + 
                         " to " + dumpFile.getAbsolutePath());
      try {
        FileOutputStream fos = new FileOutputStream(dumpFile);
        fos.write(buffer, 0, bufferSize);
        fos.close();
      } catch (IOException ioe) {
        System.err.println("Failed to dump map-output of " + taskAttemptId);
      }
    }
    
    public boolean next(DataInputBuffer key, DataInputBuffer value) 
    throws IOException {
      try {
      // Sanity check
      if (eof) {
        throw new EOFException("Completed reading " + bytesRead);
      }
      
      // Read key and value lengths
      int oldPos = dataIn.getPosition();
      int keyLength = WritableUtils.readVInt(dataIn);
      int valueLength = WritableUtils.readVInt(dataIn);
      int pos = dataIn.getPosition();
      bytesRead += pos - oldPos;
      
      // Check for EOF
      if (keyLength == EOF_MARKER && valueLength == EOF_MARKER) {
        eof = true;
        return false;
      }
      
      // Sanity check
      if (keyLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative key-length: " + 
                              keyLength);
      }
      if (valueLength < 0) {
        throw new IOException("Rec# " + recNo + ": Negative value-length: " + 
                              valueLength);
      }

      final int recordLength = keyLength + valueLength;
      
      // Setup the key and value
      pos = dataIn.getPosition();
      byte[] data = dataIn.getData();
      key.reset(data, pos, keyLength);
      value.reset(data, (pos + keyLength), valueLength);
      
      // Position for the next record
      long skipped = dataIn.skip(recordLength);
      if (skipped != recordLength) {
        throw new IOException("Rec# " + recNo + ": Failed to skip past record of length: " + 
                              recordLength);
      }
      
      // Record the byte
      bytesRead += recordLength;

      ++recNo;
      
      return true;
      } catch (IOException ioe) {
        dumpOnError();
        throw ioe;
      }
    }
      
    public void close() {
      // Release
      dataIn = null;
      buffer = null;
      
      // Inform the RamManager
      ramManager.unreserve(bufferSize);
    }
  }