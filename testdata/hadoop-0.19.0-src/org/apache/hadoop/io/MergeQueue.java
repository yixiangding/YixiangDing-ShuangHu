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

private class MergeQueue extends PriorityQueue 
      implements RawKeyValueIterator {
      private boolean compress;
      private boolean blockCompress;
      private DataOutputBuffer rawKey = new DataOutputBuffer();
      private ValueBytes rawValue;
      private long totalBytesProcessed;
      private float progPerByte;
      private Progress mergeProgress = new Progress();
      private Path tmpDir;
      private Progressable progress = null; //handle to the progress reporting object
      private SegmentDescriptor minSegment;
      
      //a TreeMap used to store the segments sorted by size (segment offset and
      //segment path name is used to break ties between segments of same sizes)
      private Map<SegmentDescriptor, Void> sortedSegmentSizes =
        new TreeMap<SegmentDescriptor, Void>();
            
      @SuppressWarnings("unchecked")
      public void put(SegmentDescriptor stream) throws IOException {
        if (size() == 0) {
          compress = stream.in.isCompressed();
          blockCompress = stream.in.isBlockCompressed();
        } else if (compress != stream.in.isCompressed() || 
                   blockCompress != stream.in.isBlockCompressed()) {
          throw new IOException("All merged files must be compressed or not.");
        } 
        super.put(stream);
      }
      
      /**
       * A queue of file segments to merge
       * @param segments the file segments to merge
       * @param tmpDir a relative local directory to save intermediate files in
       * @param progress the reference to the Progressable object
       */
      public MergeQueue(List <SegmentDescriptor> segments,
          Path tmpDir, Progressable progress) {
        int size = segments.size();
        for (int i = 0; i < size; i++) {
          sortedSegmentSizes.put(segments.get(i), null);
        }
        this.tmpDir = tmpDir;
        this.progress = progress;
      }
      protected boolean lessThan(Object a, Object b) {
        // indicate we're making progress
        if (progress != null) {
          progress.progress();
        }
        SegmentDescriptor msa = (SegmentDescriptor)a;
        SegmentDescriptor msb = (SegmentDescriptor)b;
        return comparator.compare(msa.getKey().getData(), 0, 
                                  msa.getKey().getLength(), msb.getKey().getData(), 0, 
                                  msb.getKey().getLength()) < 0;
      }
      public void close() throws IOException {
        SegmentDescriptor ms;                           // close inputs
        while ((ms = (SegmentDescriptor)pop()) != null) {
          ms.cleanup();
        }
        minSegment = null;
      }
      public DataOutputBuffer getKey() throws IOException {
        return rawKey;
      }
      public ValueBytes getValue() throws IOException {
        return rawValue;
      }
      public boolean next() throws IOException {
        if (size() == 0)
          return false;
        if (minSegment != null) {
          //minSegment is non-null for all invocations of next except the first
          //one. For the first invocation, the priority queue is ready for use
          //but for the subsequent invocations, first adjust the queue 
          adjustPriorityQueue(minSegment);
          if (size() == 0) {
            minSegment = null;
            return false;
          }
        }
        minSegment = (SegmentDescriptor)top();
        long startPos = minSegment.in.getPosition(); // Current position in stream
        //save the raw key reference
        rawKey = minSegment.getKey();
        //load the raw value. Re-use the existing rawValue buffer
        if (rawValue == null) {
          rawValue = minSegment.in.createValueBytes();
        }
        minSegment.nextRawValue(rawValue);
        long endPos = minSegment.in.getPosition(); // End position after reading value
        updateProgress(endPos - startPos);
        return true;
      }
      
      public Progress getProgress() {
        return mergeProgress; 
      }

      private void adjustPriorityQueue(SegmentDescriptor ms) throws IOException{
        long startPos = ms.in.getPosition(); // Current position in stream
        boolean hasNext = ms.nextRawKey();
        long endPos = ms.in.getPosition(); // End position after reading key
        updateProgress(endPos - startPos);
        if (hasNext) {
          adjustTop();
        } else {
          pop();
          ms.cleanup();
        }
      }

      private void updateProgress(long bytesProcessed) {
        totalBytesProcessed += bytesProcessed;
        if (progPerByte > 0) {
          mergeProgress.set(totalBytesProcessed * progPerByte);
        }
      }
      
      /** This is the single level merge that is called multiple times 
       * depending on the factor size and the number of segments
       * @return RawKeyValueIterator
       * @throws IOException
       */
      public RawKeyValueIterator merge() throws IOException {
        //create the MergeStreams from the sorted map created in the constructor
        //and dump the final output to a file
        int numSegments = sortedSegmentSizes.size();
        int origFactor = factor;
        int passNo = 1;
        LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");
        do {
          //get the factor for this pass of merge
          factor = getPassFactor(passNo, numSegments);
          List<SegmentDescriptor> segmentsToMerge =
            new ArrayList<SegmentDescriptor>();
          int segmentsConsidered = 0;
          int numSegmentsToConsider = factor;
          while (true) {
            //extract the smallest 'factor' number of segment pointers from the 
            //TreeMap. Call cleanup on the empty segments (no key/value data)
            SegmentDescriptor[] mStream = 
              getSegmentDescriptors(numSegmentsToConsider);
            for (int i = 0; i < mStream.length; i++) {
              if (mStream[i].nextRawKey()) {
                segmentsToMerge.add(mStream[i]);
                segmentsConsidered++;
                // Count the fact that we read some bytes in calling nextRawKey()
                updateProgress(mStream[i].in.getPosition());
              }
              else {
                mStream[i].cleanup();
                numSegments--; //we ignore this segment for the merge
              }
            }
            //if we have the desired number of segments
            //or looked at all available segments, we break
            if (segmentsConsidered == factor || 
                sortedSegmentSizes.size() == 0) {
              break;
            }
              
            numSegmentsToConsider = factor - segmentsConsidered;
          }
          //feed the streams to the priority queue
          initialize(segmentsToMerge.size()); clear();
          for (int i = 0; i < segmentsToMerge.size(); i++) {
            put(segmentsToMerge.get(i));
          }
          //if we have lesser number of segments remaining, then just return the
          //iterator, else do another single level merge
          if (numSegments <= factor) {
            //calculate the length of the remaining segments. Required for 
            //calculating the merge progress
            long totalBytes = 0;
            for (int i = 0; i < segmentsToMerge.size(); i++) {
              totalBytes += segmentsToMerge.get(i).segmentLength;
            }
            if (totalBytes != 0) //being paranoid
              progPerByte = 1.0f / (float)totalBytes;
            //reset factor to what it originally was
            factor = origFactor;
            return this;
          } else {
            //we want to spread the creation of temp files on multiple disks if 
            //available under the space constraints
            long approxOutputSize = 0; 
            for (SegmentDescriptor s : segmentsToMerge) {
              approxOutputSize += s.segmentLength + 
                                  ChecksumFileSystem.getApproxChkSumLength(
                                  s.segmentLength);
            }
            Path tmpFilename = 
              new Path(tmpDir, "intermediate").suffix("." + passNo);

            Path outputFile =  lDirAlloc.getLocalPathForWrite(
                                                tmpFilename.toString(),
                                                approxOutputSize, conf);
            LOG.debug("writing intermediate results to " + outputFile);
            Writer writer = cloneFileAttributes(
                                                fs.makeQualified(segmentsToMerge.get(0).segmentPathName), 
                                                fs.makeQualified(outputFile), null);
            writer.sync = null; //disable sync for temp files
            writeFile(this, writer);
            writer.close();
            
            //we finished one single level merge; now clean up the priority 
            //queue
            this.close();
            
            SegmentDescriptor tempSegment = 
              new SegmentDescriptor(0, fs.getLength(outputFile), outputFile);
            //put the segment back in the TreeMap
            sortedSegmentSizes.put(tempSegment, null);
            numSegments = sortedSegmentSizes.size();
            passNo++;
          }
          //we are worried about only the first pass merge factor. So reset the 
          //factor to what it originally was
          factor = origFactor;
        } while(true);
      }
  
      //Hadoop-591
      public int getPassFactor(int passNo, int numSegments) {
        if (passNo > 1 || numSegments <= factor || factor == 1) 
          return factor;
        int mod = (numSegments - 1) % (factor - 1);
        if (mod == 0)
          return factor;
        return mod + 1;
      }
      
      /** Return (& remove) the requested number of segment descriptors from the
       * sorted map.
       */
      public SegmentDescriptor[] getSegmentDescriptors(int numDescriptors) {
        if (numDescriptors > sortedSegmentSizes.size())
          numDescriptors = sortedSegmentSizes.size();
        SegmentDescriptor[] SegmentDescriptors = 
          new SegmentDescriptor[numDescriptors];
        Iterator iter = sortedSegmentSizes.keySet().iterator();
        int i = 0;
        while (i < numDescriptors) {
          SegmentDescriptors[i++] = (SegmentDescriptor)iter.next();
          iter.remove();
        }
        return SegmentDescriptors;
      }
    }