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
 * A {@link Decompressor} based on the lzo algorithm.
 * http://www.oberhumer.com/opensource/lzo/
 * 
 */
package org.apache.hadoop.io.compress.lzo;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;

public static enum CompressionStrategy {
    /**
     * lzo1 algorithms.
     */
    LZO1 (0),

    /**
     * lzo1a algorithms.
     */
    LZO1A (1),

    /**
     * lzo1b algorithms.
     */
    LZO1B (2),
    LZO1B_SAFE(3),

    /**
     * lzo1c algorithms.
     */
    LZO1C (4),
    LZO1C_SAFE(5),
    LZO1C_ASM (6),
    LZO1C_ASM_SAFE (7),

    /**
     * lzo1f algorithms.
     */
    LZO1F (8),
    LZO1F_SAFE (9),
    LZO1F_ASM_FAST (10),
    LZO1F_ASM_FAST_SAFE (11),
    
    /**
     * lzo1x algorithms.
     */
    LZO1X (12),
    LZO1X_SAFE (13),
    LZO1X_ASM (14),
    LZO1X_ASM_SAFE (15),
    LZO1X_ASM_FAST (16),
    LZO1X_ASM_FAST_SAFE (17),
    
    /**
     * lzo1y algorithms.
     */
    LZO1Y (18),
    LZO1Y_SAFE (19),
    LZO1Y_ASM (20),
    LZO1Y_ASM_SAFE (21),
    LZO1Y_ASM_FAST (22),
    LZO1Y_ASM_FAST_SAFE (23),
    
    /**
     * lzo1z algorithms.
     */
    LZO1Z (24),
    LZO1Z_SAFE (25),
    
    /**
     * lzo2a algorithms.
     */
    LZO2A (26),
    LZO2A_SAFE (27);
    
    private final int decompressor;

    private CompressionStrategy(int decompressor) {
      this.decompressor = decompressor;
    }
    
    int getDecompressor() {
      return decompressor;
    }
  }