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

private enum DChecksum {
    F_ADLER32D(0x01, Adler32.class), F_CRC32D(0x100, CRC32.class);
    private int mask;
    private Class<? extends Checksum> clazz;
    DChecksum(int mask, Class<? extends Checksum> clazz) {
      this.mask = mask;
      this.clazz = clazz;
    }
    public int getHeaderMask() {
      return mask;
    }
    public Class<? extends Checksum> getChecksumClass() {
      return clazz;
    }
  }