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
 * This class provides CompressionOutputStream and CompressionInputStream for
 * compression and decompression. Currently we dont have an implementation of
 * the Compressor and Decompressor interfaces, so those methods of
 * CompressionCodec which have a Compressor or Decompressor type argument, throw
 * UnsupportedOperationException.
 */
package org.apache.hadoop.io.compress;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;

private static class BZip2CompressionOutputStream extends CompressionOutputStream {

    // class data starts here//
    private CBZip2OutputStream output;

    // class data ends here//

    public BZip2CompressionOutputStream(OutputStream out)
        throws IOException {
      super(out);
      writeStreamHeader();
      this.output = new CBZip2OutputStream(out);
    }

    private void writeStreamHeader() throws IOException {
      if (super.out != null) {
        // The compressed bzip2 stream should start with the
        // identifying characters BZ. Caller of CBZip2OutputStream
        // i.e. this class must write these characters.
        out.write(HEADER.getBytes());
      }
    }

    public void write(byte[] b, int off, int len) throws IOException {
      this.output.write(b, off, len);

    }

    public void finish() throws IOException {
      this.output.flush();
    }

    public void resetState() throws IOException {

    }

    public void write(int b) throws IOException {
      this.output.write(b);
    }

    public void close() throws IOException {
      this.output.flush();
      this.output.close();
    }

    protected void finalize() throws IOException {
      if (this.output != null) {
        this.close();
      }
    }

  }