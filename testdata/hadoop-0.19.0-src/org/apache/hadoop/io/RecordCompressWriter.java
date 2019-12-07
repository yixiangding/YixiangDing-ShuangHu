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
 * <code>SequenceFile</code>s are flat files consisting of binary key/value 
 * pairs.
 * 
 * <p><code>SequenceFile</code> provides {@link Writer}, {@link Reader} and
 * {@link Sorter} classes for writing, reading and sorting respectively.</p>
 * 
 * There are three <code>SequenceFile</code> <code>Writer</code>s based on the 
 * {@link CompressionType} used to compress key/value pairs:
 * <ol>
 *   <li>
 *   <code>Writer</code> : Uncompressed records.
 *   </li>
 *   <li>
 *   <code>RecordCompressWriter</code> : Record-compressed files, only compress 
 *                                       values.
 *   </li>
 *   <li>
 *   <code>BlockCompressWriter</code> : Block-compressed files, both keys & 
 *                                      values are collected in 'blocks' 
 *                                      separately and compressed. The size of 
 *                                      the 'block' is configurable.
 * </ol>
 * 
 * <p>The actual compression algorithm used to compress key and/or values can be
 * specified by using the appropriate {@link CompressionCodec}.</p>
 * 
 * <p>The recommended way is to use the static <tt>createWriter</tt> methods
 * provided by the <code>SequenceFile</code> to chose the preferred format.</p>
 *
 * <p>The {@link Reader} acts as the bridge and can read any of the above 
 * <code>SequenceFile</code> formats.</p>
 *
 * <h4 id="Formats">SequenceFile Formats</h4>
 * 
 * <p>Essentially there are 3 different formats for <code>SequenceFile</code>s
 * depending on the <code>CompressionType</code> specified. All of them share a
 * <a href="#Header">common header</a> described below.
 * 
 * <h5 id="Header">SequenceFile Header</h5>
 * <ul>
 *   <li>
 *   version - 3 bytes of magic header <b>SEQ</b>, followed by 1 byte of actual 
 *             version number (e.g. SEQ4 or SEQ6)
 *   </li>
 *   <li>
 *   keyClassName -key class
 *   </li>
 *   <li>
 *   valueClassName - value class
 *   </li>
 *   <li>
 *   compression - A boolean which specifies if compression is turned on for 
 *                 keys/values in this file.
 *   </li>
 *   <li>
 *   blockCompression - A boolean which specifies if block-compression is 
 *                      turned on for keys/values in this file.
 *   </li>
 *   <li>
 *   compression codec - <code>CompressionCodec</code> class which is used for  
 *                       compression of keys and/or values (if compression is 
 *                       enabled).
 *   </li>
 *   <li>
 *   metadata - {@link Metadata} for this file.
 *   </li>
 *   <li>
 *   sync - A sync marker to denote end of the header.
 *   </li>
 * </ul>
 * 
 * <h5 id="#UncompressedFormat">Uncompressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record
 *   <ul>
 *     <li>Record length</li>
 *     <li>Key length</li>
 *     <li>Key</li>
 *     <li>Value</li>
 *   </ul>
 * </li>
 * <li>
 * A sync-marker every few <code>100</code> bytes or so.
 * </li>
 * </ul>
 *
 * <h5 id="#RecordCompressedFormat">Record-Compressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record
 *   <ul>
 *     <li>Record length</li>
 *     <li>Key length</li>
 *     <li>Key</li>
 *     <li><i>Compressed</i> Value</li>
 *   </ul>
 * </li>
 * <li>
 * A sync-marker every few <code>100</code> bytes or so.
 * </li>
 * </ul>
 * 
 * <h5 id="#BlockCompressedFormat">Block-Compressed SequenceFile Format</h5>
 * <ul>
 * <li>
 * <a href="#Header">Header</a>
 * </li>
 * <li>
 * Record <i>Block</i>
 *   <ul>
 *     <li>Compressed key-lengths block-size</li>
 *     <li>Compressed key-lengths block</li>
 *     <li>Compressed keys block-size</li>
 *     <li>Compressed keys block</li>
 *     <li>Compressed value-lengths block-size</li>
 *     <li>Compressed value-lengths block</li>
 *     <li>Compressed values block-size</li>
 *     <li>Compressed values block</li>
 *   </ul>
 * </li>
 * <li>
 * A sync-marker every few <code>100</code> bytes or so.
 * </li>
 * </ul>
 * 
 * <p>The compressed blocks of key lengths and value lengths consist of the 
 * actual lengths of individual keys/values encoded in ZeroCompressedInteger 
 * format.</p>
 * 
 * @see CompressionCodec
 */
package org.apache.hadoop.io;
import java.io.*;
import java.util.*;
import java.rmi.server.UID;
import java.security.MessageDigest;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.MergeSort;
import org.apache.hadoop.util.PriorityQueue;

static class RecordCompressWriter extends Writer {
    
    /** Create the named file. */
    public RecordCompressWriter(FileSystem fs, Configuration conf, Path name, 
                                Class keyClass, Class valClass, CompressionCodec codec) 
      throws IOException {
      this(conf, fs.create(name), keyClass, valClass, codec, new Metadata());
    }
    
    /** Create the named file with write-progress reporter. */
    public RecordCompressWriter(FileSystem fs, Configuration conf, Path name, 
                                Class keyClass, Class valClass, CompressionCodec codec,
                                Progressable progress, Metadata metadata)
      throws IOException {
      this(fs, conf, name, keyClass, valClass,
           fs.getConf().getInt("io.file.buffer.size", 4096),
           fs.getDefaultReplication(), fs.getDefaultBlockSize(), codec,
           progress, metadata);
    }

    /** Create the named file with write-progress reporter. */
    public RecordCompressWriter(FileSystem fs, Configuration conf, Path name,
                                Class keyClass, Class valClass,
                                int bufferSize, short replication, long blockSize,
                                CompressionCodec codec,
                                Progressable progress, Metadata metadata)
      throws IOException {
      super.init(name, conf,
                 fs.create(name, true, bufferSize, replication, blockSize, progress),
                 keyClass, valClass, true, codec, metadata);

      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
    }

    /** Create the named file with write-progress reporter. */
    public RecordCompressWriter(FileSystem fs, Configuration conf, Path name, 
                                Class keyClass, Class valClass, CompressionCodec codec,
                                Progressable progress)
      throws IOException {
      this(fs, conf, name, keyClass, valClass, codec, progress, new Metadata());
    }
    
    /** Write to an arbitrary stream using a specified buffer size. */
    private RecordCompressWriter(Configuration conf, FSDataOutputStream out,
                                 Class keyClass, Class valClass, CompressionCodec codec, Metadata metadata)
      throws IOException {
      this.ownOutputStream = false;
      super.init(null, conf, out, keyClass, valClass, true, codec, metadata);
      
      initializeFileHeader();
      writeFileHeader();
      finalizeFileHeader();
      
    }
    
    boolean isCompressed() { return true; }
    boolean isBlockCompressed() { return false; }

    /** Append a key/value pair. */
    @SuppressWarnings("unchecked")
    public synchronized void append(Object key, Object val)
      throws IOException {
      if (key.getClass() != keyClass)
        throw new IOException("wrong key class: "+key.getClass().getName()
                              +" is not "+keyClass);
      if (val.getClass() != valClass)
        throw new IOException("wrong value class: "+val.getClass().getName()
                              +" is not "+valClass);

      buffer.reset();

      // Append the 'key'
      keySerializer.serialize(key);
      int keyLength = buffer.getLength();
      if (keyLength < 0)
        throw new IOException("negative length keys not allowed: " + key);

      // Compress 'value' and append it
      deflateFilter.resetState();
      compressedValSerializer.serialize(val);
      deflateOut.flush();
      deflateFilter.finish();

      // Write the record out
      checkAndWriteSync();                                // sync
      out.writeInt(buffer.getLength());                   // total record length
      out.writeInt(keyLength);                            // key portion length
      out.write(buffer.getData(), 0, buffer.getLength()); // data
    }

    /** Append a key/value pair. */
    public synchronized void appendRaw(byte[] keyData, int keyOffset,
        int keyLength, ValueBytes val) throws IOException {

      if (keyLength < 0)
        throw new IOException("negative length keys not allowed: " + keyLength);

      int valLength = val.getSize();
      
      checkAndWriteSync();                        // sync
      out.writeInt(keyLength+valLength);          // total record length
      out.writeInt(keyLength);                    // key portion length
      out.write(keyData, keyOffset, keyLength);   // 'key' data
      val.writeCompressedBytes(out);              // 'value' data
    }
    
  }