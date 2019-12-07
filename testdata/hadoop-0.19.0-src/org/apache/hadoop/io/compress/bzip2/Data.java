/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
/*
 * This package is based on the work done by Keiron Liddle, Aftex Software
 * <keiron@aftexsw.com> to whom the Ant project is very grateful for his
 * great code.
 */
/**
 * An input stream that decompresses from the BZip2 format (without the file
 * header chars) to be read as any other stream.
 *
 * <p>
 * The decompression requires large amounts of memory. Thus you should call the
 * {@link #close() close()} method as soon as possible, to force
 * <tt>CBZip2InputStream</tt> to release the allocated memory. See
 * {@link CBZip2OutputStream CBZip2OutputStream} for information about memory
 * usage.
 * </p>
 *
 * <p>
 * <tt>CBZip2InputStream</tt> reads bytes from the compressed source stream via
 * the single byte {@link java.io.InputStream#read() read()} method exclusively.
 * Thus you should consider to use a buffered source stream.
 * </p>
 *
 * <p>
 * Instances of this class are not threadsafe.
 * </p>
 */
package org.apache.hadoop.io.compress.bzip2;
import java.io.InputStream;
import java.io.IOException;

private static final class Data extends Object {

    // (with blockSize 900k)
    final boolean[] inUse = new boolean[256]; // 256 byte

    final byte[] seqToUnseq = new byte[256]; // 256 byte
    final byte[] selector = new byte[MAX_SELECTORS]; // 18002 byte
    final byte[] selectorMtf = new byte[MAX_SELECTORS]; // 18002 byte

    /**
    * Freq table collected to save a pass over the data during
    * decompression.
    */
    final int[] unzftab = new int[256]; // 1024 byte

    final int[][] limit = new int[N_GROUPS][MAX_ALPHA_SIZE]; // 6192 byte
    final int[][] base = new int[N_GROUPS][MAX_ALPHA_SIZE]; // 6192 byte
    final int[][] perm = new int[N_GROUPS][MAX_ALPHA_SIZE]; // 6192 byte
    final int[] minLens = new int[N_GROUPS]; // 24 byte

    final int[] cftab = new int[257]; // 1028 byte
    final char[] getAndMoveToFrontDecode_yy = new char[256]; // 512 byte
    final char[][] temp_charArray2d = new char[N_GROUPS][MAX_ALPHA_SIZE]; // 3096
                                        // byte
    final byte[] recvDecodingTables_pos = new byte[N_GROUPS]; // 6 byte
    // ---------------
    // 60798 byte

    int[] tt; // 3600000 byte
    byte[] ll8; // 900000 byte

    // ---------------
    // 4560782 byte
    // ===============

    Data(int blockSize100k) {
      super();

      this.ll8 = new byte[blockSize100k * BZip2Constants.baseBlockSize];
    }

    /**
    * Initializes the {@link #tt} array.
    *
    * This method is called when the required length of the array is known.
    * I don't initialize it at construction time to avoid unneccessary
    * memory allocation when compressing small files.
    */
    final int[] initTT(int length) {
      int[] ttShadow = this.tt;

      // tt.length should always be >= length, but theoretically
      // it can happen, if the compressor mixed small and large
      // blocks. Normally only the last block will be smaller
      // than others.
      if ((ttShadow == null) || (ttShadow.length < length)) {
        this.tt = ttShadow = new int[length];
      }

      return ttShadow;
    }

  }