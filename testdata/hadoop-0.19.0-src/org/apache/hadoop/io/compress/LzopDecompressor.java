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

protected static class LzopDecompressor extends LzoDecompressor {

    private EnumMap<DChecksum,Checksum> chkDMap =
      new EnumMap<DChecksum,Checksum>(DChecksum.class);
    private EnumMap<CChecksum,Checksum> chkCMap =
      new EnumMap<CChecksum,Checksum>(CChecksum.class);
    private final int bufferSize;

    /**
     * Create an LzoDecompressor with LZO1X strategy (the only lzo algorithm
     * supported by lzop).
     */
    public LzopDecompressor(int bufferSize) {
      super(LzoDecompressor.CompressionStrategy.LZO1X_SAFE, bufferSize);
      this.bufferSize = bufferSize;
    }

    /**
     * Given a set of decompressed and compressed checksums, 
     */
    public void initHeaderFlags(EnumSet<DChecksum> dflags,
        EnumSet<CChecksum> cflags) {
      try {
        for (DChecksum flag : dflags) {
          chkDMap.put(flag, flag.getChecksumClass().newInstance());
        }
        for (CChecksum flag : cflags) {
          chkCMap.put(flag, flag.getChecksumClass().newInstance());
        }
      } catch (InstantiationException e) {
        throw new RuntimeException("Internal error", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Internal error", e);
      }
    }

    /**
     * Reset all checksums registered for this decompressor instance.
     */
    public synchronized void resetChecksum() {
      for (Checksum chk : chkDMap.values()) chk.reset();
      for (Checksum chk : chkCMap.values()) chk.reset();
    }

    /**
     * Given a checksum type, verify its value against that observed in
     * decompressed data.
     */
    public synchronized boolean verifyDChecksum(DChecksum typ, int checksum) {
      return (checksum == (int)chkDMap.get(typ).getValue());
    }

    /**
     * Given a checksum type, verity its value against that observed in
     * compressed data.
     */
    public synchronized boolean verifyCChecksum(CChecksum typ, int checksum) {
      return (checksum == (int)chkCMap.get(typ).getValue());
    }

    public synchronized void setInput(byte[] b, int off, int len) {
      for (Checksum chk : chkCMap.values()) chk.update(b, off, len);
      super.setInput(b, off, len);
    }

    public synchronized int decompress(byte[] b, int off, int len)
        throws IOException {
      int ret = super.decompress(b, off, len);
      if (ret > 0) {
        for (Checksum chk : chkDMap.values()) chk.update(b, off, ret);
      }
      return ret;
    }
  }