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

protected static class LzopInputStream extends BlockDecompressorStream {

    private EnumSet<DChecksum> dflags = EnumSet.allOf(DChecksum.class);
    private EnumSet<CChecksum> cflags = EnumSet.allOf(CChecksum.class);

    private final byte[] buf = new byte[9];
    private EnumMap<DChecksum,Integer> dcheck
      = new EnumMap<DChecksum,Integer>(DChecksum.class);
    private EnumMap<CChecksum,Integer> ccheck
      = new EnumMap<CChecksum,Integer>(CChecksum.class);

    public LzopInputStream(InputStream in, Decompressor decompressor,
        int bufferSize) throws IOException {
      super(in, decompressor, bufferSize);
      readHeader(in);
    }

    /**
     * Read len bytes into buf, st LSB of int returned is the last byte of the
     * first word read.
     */
    private static int readInt(InputStream in, byte[] buf, int len) 
        throws IOException {
      if (0 > in.read(buf, 0, len)) {
        throw new EOFException();
      }
      int ret = (0xFF & buf[0]) << 24;
      ret    |= (0xFF & buf[1]) << 16;
      ret    |= (0xFF & buf[2]) << 8;
      ret    |= (0xFF & buf[3]);
      return (len > 3) ? ret : (ret >>> (8 * (4 - len)));
    }

    /**
     * Read bytes, update checksums, return first four bytes as an int, first
     * byte read in the MSB.
     */
    private static int readHeaderItem(InputStream in, byte[] buf, int len,
        Adler32 adler, CRC32 crc32) throws IOException {
      int ret = readInt(in, buf, len);
      adler.update(buf, 0, len);
      crc32.update(buf, 0, len);
      Arrays.fill(buf, (byte)0);
      return ret;
    }

    /**
     * Read and verify an lzo header, setting relevant block checksum options
     * and ignoring most everything else.
     */
    protected void readHeader(InputStream in) throws IOException {
      if (0 > in.read(buf, 0, 9)) {
        throw new EOFException();
      }
      if (!Arrays.equals(buf, LZO_MAGIC)) {
        throw new IOException("Invalid LZO header");
      }
      Arrays.fill(buf, (byte)0);
      Adler32 adler = new Adler32();
      CRC32 crc32 = new CRC32();
      int hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzop version
      if (hitem > LZOP_VERSION) {
        LOG.debug("Compressed with later version of lzop: " +
            Integer.toHexString(hitem) + " (expected 0x" +
            Integer.toHexString(LZOP_VERSION) + ")");
      }
      hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzo library version
      if (hitem > LzoDecompressor.LZO_LIBRARY_VERSION) {
        throw new IOException("Compressed with incompatible lzo version: 0x" +
            Integer.toHexString(hitem) + " (expected 0x" +
            Integer.toHexString(LzoDecompressor.LZO_LIBRARY_VERSION) + ")");
      }
      hitem = readHeaderItem(in, buf, 2, adler, crc32); // lzop extract version
      if (hitem > LZOP_VERSION) {
        throw new IOException("Compressed with incompatible lzop version: 0x" +
            Integer.toHexString(hitem) + " (expected 0x" +
            Integer.toHexString(LZOP_VERSION) + ")");
      }
      hitem = readHeaderItem(in, buf, 1, adler, crc32); // method
      if (hitem < 1 || hitem > 3) {
          throw new IOException("Invalid strategy: " +
              Integer.toHexString(hitem));
      }
      readHeaderItem(in, buf, 1, adler, crc32); // ignore level

      // flags
      hitem = readHeaderItem(in, buf, 4, adler, crc32);
      try {
        for (DChecksum f : dflags) {
          if (0 == (f.getHeaderMask() & hitem)) {
            dflags.remove(f);
          } else {
            dcheck.put(f, (int)f.getChecksumClass().newInstance().getValue());
          }
        }
        for (CChecksum f : cflags) {
          if (0 == (f.getHeaderMask() & hitem)) {
            cflags.remove(f);
          } else {
            ccheck.put(f, (int)f.getChecksumClass().newInstance().getValue());
          }
        }
      } catch (InstantiationException e) {
        throw new RuntimeException("Internal error", e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Internal error", e);
      }
      ((LzopDecompressor)decompressor).initHeaderFlags(dflags, cflags);
      boolean useCRC32 = 0 != (hitem & 0x00001000);   // F_H_CRC32
      boolean extraField = 0 != (hitem & 0x00000040); // F_H_EXTRA_FIELD
      if (0 != (hitem & 0x400)) {                     // F_MULTIPART
        throw new IOException("Multipart lzop not supported");
      }
      if (0 != (hitem & 0x800)) {                     // F_H_FILTER
        throw new IOException("lzop filter not supported");
      }
      if (0 != (hitem & 0x000FC000)) {                // F_RESERVED
        throw new IOException("Unknown flags in header");
      }
      // known !F_H_FILTER, so no optional block

      readHeaderItem(in, buf, 4, adler, crc32); // ignore mode
      readHeaderItem(in, buf, 4, adler, crc32); // ignore mtime
      readHeaderItem(in, buf, 4, adler, crc32); // ignore gmtdiff
      hitem = readHeaderItem(in, buf, 1, adler, crc32); // fn len
      if (hitem > 0) {
        // skip filename
        readHeaderItem(in, new byte[hitem], hitem, adler, crc32);
      }
      int checksum = (int)(useCRC32 ? crc32.getValue() : adler.getValue());
      hitem = readHeaderItem(in, buf, 4, adler, crc32); // read checksum
      if (hitem != checksum) {
        throw new IOException("Invalid header checksum: " +
            Long.toHexString(checksum) + " (expected 0x" +
            Integer.toHexString(hitem) + ")");
      }
      if (extraField) { // lzop 1.08 ultimately ignores this
        LOG.debug("Extra header field not processed");
        adler.reset();
        crc32.reset();
        hitem = readHeaderItem(in, buf, 4, adler, crc32);
        readHeaderItem(in, new byte[hitem], hitem, adler, crc32);
        checksum = (int)(useCRC32 ? crc32.getValue() : adler.getValue());
        if (checksum != readHeaderItem(in, buf, 4, adler, crc32)) {
          throw new IOException("Invalid checksum for extra header field");
        }
      }
    }

    /**
     * Take checksums recorded from block header and verify them against
     * those recorded by the decomrpessor.
     */
    private void verifyChecksums() throws IOException {
      LzopDecompressor ldecompressor = ((LzopDecompressor)decompressor);
      for (Map.Entry<DChecksum,Integer> chk : dcheck.entrySet()) {
        if (!ldecompressor.verifyDChecksum(chk.getKey(), chk.getValue())) {
          throw new IOException("Corrupted uncompressed block");
        }
      }
      for (Map.Entry<CChecksum,Integer> chk : ccheck.entrySet()) {
        if (!ldecompressor.verifyCChecksum(chk.getKey(), chk.getValue())) {
          throw new IOException("Corrupted compressed block");
        }
      }
    }

    /**
     * Read checksums and feed compressed block data into decompressor.
     */
    void getCompressedData() throws IOException {
      checkStream();

      LzopDecompressor ldecompressor = (LzopDecompressor)decompressor;

      // Get the size of the compressed chunk
      int len = readInt(in, buf, 4);

      verifyChecksums();

      for (DChecksum chk : dcheck.keySet()) {
        dcheck.put(chk, readInt(in, buf, 4));
      }
      for (CChecksum chk : ccheck.keySet()) {
        // NOTE: if the compressed size is not less than the uncompressed
        //       size, this value is not present and decompression will fail.
        //       Fortunately, checksums on compressed data are rare, as is
        //       this case.
        ccheck.put(chk, readInt(in, buf, 4));
      }

      ldecompressor.resetChecksum();

      // Read len bytes from underlying stream
      if (len > buffer.length) {
        buffer = new byte[len];
      }
      int n = 0, off = 0;
      while (n < len) {
        int count = in.read(buffer, off + n, len - n);
        if (count < 0) {
          throw new EOFException();
        }
        n += count;
      }

      // Send the read data to the decompressor
      decompressor.setInput(buffer, 0, len);
    }

    public void close() throws IOException {
      super.close();
      verifyChecksums();
    }
  }