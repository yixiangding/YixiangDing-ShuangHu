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
 * A {@link org.apache.hadoop.io.compress.CompressionCodec} for a streaming
 * <b>lzo</b> compression/decompression pair compatible with lzop.
 * http://www.lzop.org/
 */
package org.apache.hadoop.io.compress;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.EnumMap;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.lzo.*;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

protected static class LzopOutputStream extends BlockCompressorStream {

    /**
     * Write an lzop-compatible header to the OutputStream provided.
     */
    protected static void writeLzopHeader(OutputStream out,
        LzoCompressor.CompressionStrategy strategy) throws IOException {
      DataOutputBuffer dob = new DataOutputBuffer();
      try {
        dob.writeShort(LZOP_VERSION);
        dob.writeShort(LzoCompressor.LZO_LIBRARY_VERSION);
        dob.writeShort(LZOP_COMPAT_VERSION);
        switch (strategy) {
          case LZO1X_1:
            dob.writeByte(1);
            dob.writeByte(5);
            break;
          case LZO1X_15:
            dob.writeByte(2);
            dob.writeByte(1);
            break;
          case LZO1X_999:
            dob.writeByte(3);
            dob.writeByte(9);
            break;
          default:
            throw new IOException("Incompatible lzop strategy: " + strategy);
        }
        dob.writeInt(0);                                    // all flags 0
        dob.writeInt(0x81A4);                               // mode
        dob.writeInt((int)(System.currentTimeMillis() / 1000)); // mtime
        dob.writeInt(0);                                    // gmtdiff ignored
        dob.writeByte(0);                                   // no filename
        Adler32 headerChecksum = new Adler32();
        headerChecksum.update(dob.getData(), 0, dob.getLength());
        int hc = (int)headerChecksum.getValue();
        dob.writeInt(hc);
        out.write(LZO_MAGIC);
        out.write(dob.getData(), 0, dob.getLength());
      } finally {
        dob.close();
      }
    }

    public LzopOutputStream(OutputStream out, Compressor compressor,
        int bufferSize, LzoCompressor.CompressionStrategy strategy)
        throws IOException {
      super(out, compressor, bufferSize, strategy.name().contains("LZO1")
          ? (bufferSize >> 4) + 64 + 3
          : (bufferSize >> 3) + 128 + 3);
      writeLzopHeader(out, strategy);
    }

    /**
     * Close the underlying stream and write a null word to the output stream.
     */
    public void close() throws IOException {
      if (!closed) {
        finish();
        out.write(new byte[]{ 0, 0, 0, 0 });
        out.close();
        closed = true;
      }
    }

  }