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

private static class BZip2CompressionInputStream extends CompressionInputStream {

    // class data starts here//
    private CBZip2InputStream input;

    // class data ends here//

    public BZip2CompressionInputStream(InputStream in) throws IOException {

      super(in);
      BufferedInputStream bufferedIn = readStreamHeader();
      input = new CBZip2InputStream(bufferedIn);
    }

    private BufferedInputStream readStreamHeader() throws IOException {
      // We are flexible enough to allow the compressed stream not to
      // start with the header of BZ. So it works fine either we have
      // the header or not.
      BufferedInputStream bufferedIn = null;
      if (super.in != null) {
        bufferedIn = new BufferedInputStream(super.in);
        bufferedIn.mark(HEADER_LEN);
        byte[] headerBytes = new byte[HEADER_LEN];
        int actualRead = bufferedIn.read(headerBytes, 0, HEADER_LEN);
        if (actualRead != -1) {
          String header = new String(headerBytes);
          if (header.compareTo(HEADER) != 0) {
            bufferedIn.reset();
          }
        }
      }

      if (bufferedIn == null) {
        throw new IOException("Failed to read bzip2 stream.");
      }

      return bufferedIn;

    }// end of method

    public void close() throws IOException {
      this.input.close();
    }

    public int read(byte[] b, int off, int len) throws IOException {

      return this.input.read(b, off, len);

    }

    public void resetState() throws IOException {

    }

    public int read() throws IOException {
      return this.input.read();

    }

    protected void finalize() throws IOException {
      if (this.input != null) {
        this.close();
      }

    }

  }