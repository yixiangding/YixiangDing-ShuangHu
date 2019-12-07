/*
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
 * This class creates gzip compressors/decompressors. 
 */
package org.apache.hadoop.io.compress;
import java.io.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.zlib.*;

protected static class GzipInputStream extends DecompressorStream {
    
    private static class ResetableGZIPInputStream extends GZIPInputStream {

      public ResetableGZIPInputStream(InputStream in) throws IOException {
        super(in);
      }
      
      public void resetState() throws IOException {
        inf.reset();
      }
    }
    
    public GzipInputStream(InputStream in) throws IOException {
      super(new ResetableGZIPInputStream(in));
    }
    
    /**
     * Allow subclasses to directly set the inflater stream.
     */
    protected GzipInputStream(DecompressorStream in) {
      super(in);
    }

    public int available() throws IOException {
      return in.available(); 
    }

    public void close() throws IOException {
      in.close();
    }

    public int read() throws IOException {
      return in.read();
    }
    
    public int read(byte[] data, int offset, int len) throws IOException {
      return in.read(data, offset, len);
    }
    
    public long skip(long offset) throws IOException {
      return in.skip(offset);
    }
    
    public void resetState() throws IOException {
      ((ResetableGZIPInputStream) in).resetState();
    }
  }