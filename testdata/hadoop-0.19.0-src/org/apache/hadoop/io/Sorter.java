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

public static class Sorter {

    private RawComparator comparator;

    private MergeSort mergeSort; //the implementation of merge sort
    
    private Path[] inFiles;                     // when merging or sorting

    private Path outFile;

    private int memory; // bytes
    private int factor; // merged per pass

    private FileSystem fs = null;

    private Class keyClass;
    private Class valClass;

    private Configuration conf;
    
    private Progressable progressable = null;

    /** Sort and merge files containing the named classes. */
    public Sorter(FileSystem fs, Class<? extends WritableComparable> keyClass,
                  Class valClass, Configuration conf)  {
      this(fs, WritableComparator.get(keyClass), keyClass, valClass, conf);
    }

    /** Sort and merge using an arbitrary {@link RawComparator}. */
    public Sorter(FileSystem fs, RawComparator comparator, Class keyClass, 
                  Class valClass, Configuration conf) {
      this.fs = fs;
      this.comparator = comparator;
      this.keyClass = keyClass;
      this.valClass = valClass;
      this.memory = conf.getInt("io.sort.mb", 100) * 1024 * 1024;
      this.factor = conf.getInt("io.sort.factor", 100);
      this.conf = conf;
    }

    /** Set the number of streams to merge at once.*/
    public void setFactor(int factor) { this.factor = factor; }

    /** Get the number of streams to merge at once.*/
    public int getFactor() { return factor; }

    /** Set the total amount of buffer memory, in bytes.*/
    public void setMemory(int memory) { this.memory = memory; }

    /** Get the total amount of buffer memory, in bytes.*/
    public int getMemory() { return memory; }

    /** Set the progressable object in order to report progress. */
    public void setProgressable(Progressable progressable) {
      this.progressable = progressable;
    }
    
    /** 
     * Perform a file sort from a set of input files into an output file.
     * @param inFiles the files to be sorted
     * @param outFile the sorted output file
     * @param deleteInput should the input files be deleted as they are read?
     */
    public void sort(Path[] inFiles, Path outFile,
                     boolean deleteInput) throws IOException {
      if (fs.exists(outFile)) {
        throw new IOException("already exists: " + outFile);
      }

      this.inFiles = inFiles;
      this.outFile = outFile;

      int segments = sortPass(deleteInput);
      if (segments > 1) {
        mergePass(outFile.getParent());
      }
    }

    /** 
     * Perform a file sort from a set of input files and return an iterator.
     * @param inFiles the files to be sorted
     * @param tempDir the directory where temp files are created during sort
     * @param deleteInput should the input files be deleted as they are read?
     * @return iterator the RawKeyValueIterator
     */
    public RawKeyValueIterator sortAndIterate(Path[] inFiles, Path tempDir, 
                                              boolean deleteInput) throws IOException {
      Path outFile = new Path(tempDir + Path.SEPARATOR + "all.2");
      if (fs.exists(outFile)) {
        throw new IOException("already exists: " + outFile);
      }
      this.inFiles = inFiles;
      //outFile will basically be used as prefix for temp files in the cases
      //where sort outputs multiple sorted segments. For the single segment
      //case, the outputFile itself will contain the sorted data for that
      //segment
      this.outFile = outFile;

      int segments = sortPass(deleteInput);
      if (segments > 1)
        return merge(outFile.suffix(".0"), outFile.suffix(".0.index"), 
                     tempDir);
      else if (segments == 1)
        return merge(new Path[]{outFile}, true, tempDir);
      else return null;
    }

    /**
     * The backwards compatible interface to sort.
     * @param inFile the input file to sort
     * @param outFile the sorted output file
     */
    public void sort(Path inFile, Path outFile) throws IOException {
      sort(new Path[]{inFile}, outFile, false);
    }
    
    private int sortPass(boolean deleteInput) throws IOException {
      LOG.debug("running sort pass");
      SortPass sortPass = new SortPass();         // make the SortPass
      sortPass.setProgressable(progressable);
      mergeSort = new MergeSort(sortPass.new SeqFileComparator());
      try {
        return sortPass.run(deleteInput);         // run it
      } finally {
        sortPass.close();                         // close it
      }
    }

    private class SortPass {
      private int memoryLimit = memory/4;
      private int recordLimit = 1000000;
      
      private DataOutputBuffer rawKeys = new DataOutputBuffer();
      private byte[] rawBuffer;

      private int[] keyOffsets = new int[1024];
      private int[] pointers = new int[keyOffsets.length];
      private int[] pointersCopy = new int[keyOffsets.length];
      private int[] keyLengths = new int[keyOffsets.length];
      private ValueBytes[] rawValues = new ValueBytes[keyOffsets.length];
      
      private ArrayList segmentLengths = new ArrayList();
      
      private Reader in = null;
      private FSDataOutputStream out = null;
      private FSDataOutputStream indexOut = null;
      private Path outName;

      private Progressable progressable = null;

      public int run(boolean deleteInput) throws IOException {
        int segments = 0;
        int currentFile = 0;
        boolean atEof = (currentFile >= inFiles.length);
        boolean isCompressed = false;
        boolean isBlockCompressed = false;
        CompressionCodec codec = null;
        segmentLengths.clear();
        if (atEof) {
          return 0;
        }
        
        // Initialize
        in = new Reader(fs, inFiles[currentFile], conf);
        isCompressed = in.isCompressed();
        isBlockCompressed = in.isBlockCompressed();
        codec = in.getCompressionCodec();
        
        for (int i=0; i < rawValues.length; ++i) {
          rawValues[i] = null;
        }
        
        while (!atEof) {
          int count = 0;
          int bytesProcessed = 0;
          rawKeys.reset();
          while (!atEof && 
                 bytesProcessed < memoryLimit && count < recordLimit) {

            // Read a record into buffer
            // Note: Attempt to re-use 'rawValue' as far as possible
            int keyOffset = rawKeys.getLength();       
            ValueBytes rawValue = 
              (count == keyOffsets.length || rawValues[count] == null) ? 
              in.createValueBytes() : 
              rawValues[count];
            int recordLength = in.nextRaw(rawKeys, rawValue);
            if (recordLength == -1) {
              in.close();
              if (deleteInput) {
                fs.delete(inFiles[currentFile], true);
              }
              currentFile += 1;
              atEof = currentFile >= inFiles.length;
              if (!atEof) {
                in = new Reader(fs, inFiles[currentFile], conf);
              } else {
                in = null;
              }
              continue;
            }

            int keyLength = rawKeys.getLength() - keyOffset;

            if (count == keyOffsets.length)
              grow();

            keyOffsets[count] = keyOffset;                // update pointers
            pointers[count] = count;
            keyLengths[count] = keyLength;
            rawValues[count] = rawValue;

            bytesProcessed += recordLength; 
            count++;
          }

          // buffer is full -- sort & flush it
          LOG.debug("flushing segment " + segments);
          rawBuffer = rawKeys.getData();
          sort(count);
          // indicate we're making progress
          if (progressable != null) {
            progressable.progress();
          }
          flush(count, bytesProcessed, isCompressed, isBlockCompressed, codec, 
                segments==0 && atEof);
          segments++;
        }
        return segments;
      }

      public void close() throws IOException {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
        if (indexOut != null) {
          indexOut.close();
        }
      }

      private void grow() {
        int newLength = keyOffsets.length * 3 / 2;
        keyOffsets = grow(keyOffsets, newLength);
        pointers = grow(pointers, newLength);
        pointersCopy = new int[newLength];
        keyLengths = grow(keyLengths, newLength);
        rawValues = grow(rawValues, newLength);
      }

      private int[] grow(int[] old, int newLength) {
        int[] result = new int[newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        return result;
      }
      
      private ValueBytes[] grow(ValueBytes[] old, int newLength) {
        ValueBytes[] result = new ValueBytes[newLength];
        System.arraycopy(old, 0, result, 0, old.length);
        for (int i=old.length; i < newLength; ++i) {
          result[i] = null;
        }
        return result;
      }

      private void flush(int count, int bytesProcessed, boolean isCompressed, 
                         boolean isBlockCompressed, CompressionCodec codec, boolean done) 
        throws IOException {
        if (out == null) {
          outName = done ? outFile : outFile.suffix(".0");
          out = fs.create(outName);
          if (!done) {
            indexOut = fs.create(outName.suffix(".index"));
          }
        }

        long segmentStart = out.getPos();
        Writer writer = createWriter(conf, out, keyClass, valClass, 
                                     isCompressed, isBlockCompressed, codec, 
                                     new Metadata());
        
        if (!done) {
          writer.sync = null;                     // disable sync on temp files
        }

        for (int i = 0; i < count; i++) {         // write in sorted order
          int p = pointers[i];
          writer.appendRaw(rawBuffer, keyOffsets[p], keyLengths[p], rawValues[p]);
        }
        writer.close();
        
        if (!done) {
          // Save the segment length
          WritableUtils.writeVLong(indexOut, segmentStart);
          WritableUtils.writeVLong(indexOut, (out.getPos()-segmentStart));
          indexOut.flush();
        }
      }

      private void sort(int count) {
        System.arraycopy(pointers, 0, pointersCopy, 0, count);
        mergeSort.mergeSort(pointersCopy, pointers, 0, count);
      }
      class SeqFileComparator implements Comparator<IntWritable> {
        public int compare(IntWritable I, IntWritable J) {
          return comparator.compare(rawBuffer, keyOffsets[I.get()], 
                                    keyLengths[I.get()], rawBuffer, 
                                    keyOffsets[J.get()], keyLengths[J.get()]);
        }
      }
      
      /** set the progressable object in order to report progress */
      public void setProgressable(Progressable progressable)
      {
        this.progressable = progressable;
      }
      
    } // SequenceFile.Sorter.SortPass

    /** The interface to iterate over raw keys/values of SequenceFiles. */
    public static interface RawKeyValueIterator {
      /** Gets the current raw key
       * @return DataOutputBuffer
       * @throws IOException
       */
      DataOutputBuffer getKey() throws IOException; 
      /** Gets the current raw value
       * @return ValueBytes 
       * @throws IOException
       */
      ValueBytes getValue() throws IOException; 
      /** Sets up the current key and value (for getKey and getValue)
       * @return true if there exists a key/value, false otherwise 
       * @throws IOException
       */
      boolean next() throws IOException;
      /** closes the iterator so that the underlying streams can be closed
       * @throws IOException
       */
      void close() throws IOException;
      /** Gets the Progress object; this has a float (0.0 - 1.0) 
       * indicating the bytes processed by the iterator so far
       */
      Progress getProgress();
    }    
    
    /**
     * Merges the list of segments of type <code>SegmentDescriptor</code>
     * @param segments the list of SegmentDescriptors
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIterator
     * @throws IOException
     */
    public RawKeyValueIterator merge(List <SegmentDescriptor> segments, 
                                     Path tmpDir) 
      throws IOException {
      // pass in object to report progress, if present
      MergeQueue mQueue = new MergeQueue(segments, tmpDir, progressable);
      return mQueue.merge();
    }

    /**
     * Merges the contents of files passed in Path[] using a max factor value
     * that is already set
     * @param inNames the array of path names
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public RawKeyValueIterator merge(Path [] inNames, boolean deleteInputs,
                                     Path tmpDir) 
      throws IOException {
      return merge(inNames, deleteInputs, 
                   (inNames.length < factor) ? inNames.length : factor,
                   tmpDir);
    }

    /**
     * Merges the contents of files passed in Path[]
     * @param inNames the array of path names
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @param factor the factor that will be used as the maximum merge fan-in
     * @param tmpDir the directory to write temporary files into
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public RawKeyValueIterator merge(Path [] inNames, boolean deleteInputs,
                                     int factor, Path tmpDir) 
      throws IOException {
      //get the segments from inNames
      ArrayList <SegmentDescriptor> a = new ArrayList <SegmentDescriptor>();
      for (int i = 0; i < inNames.length; i++) {
        SegmentDescriptor s = new SegmentDescriptor(0, 
                                                    fs.getLength(inNames[i]), inNames[i]);
        s.preserveInput(!deleteInputs);
        s.doSync();
        a.add(s);
      }
      this.factor = factor;
      MergeQueue mQueue = new MergeQueue(a, tmpDir, progressable);
      return mQueue.merge();
    }

    /**
     * Merges the contents of files passed in Path[]
     * @param inNames the array of path names
     * @param tempDir the directory for creating temp files during merge
     * @param deleteInputs true if the input files should be deleted when 
     * unnecessary
     * @return RawKeyValueIteratorMergeQueue
     * @throws IOException
     */
    public RawKeyValueIterator merge(Path [] inNames, Path tempDir, 
                                     boolean deleteInputs) 
      throws IOException {
      //outFile will basically be used as prefix for temp files for the
      //intermediate merge outputs           
      this.outFile = new Path(tempDir + Path.SEPARATOR + "merged");
      //get the segments from inNames
      ArrayList <SegmentDescriptor> a = new ArrayList <SegmentDescriptor>();
      for (int i = 0; i < inNames.length; i++) {
        SegmentDescriptor s = new SegmentDescriptor(0, 
                                                    fs.getLength(inNames[i]), inNames[i]);
        s.preserveInput(!deleteInputs);
        s.doSync();
        a.add(s);
      }
      factor = (inNames.length < factor) ? inNames.length : factor;
      // pass in object to report progress, if present
      MergeQueue mQueue = new MergeQueue(a, tempDir, progressable);
      return mQueue.merge();
    }

    /**
     * Clones the attributes (like compression of the input file and creates a 
     * corresponding Writer
     * @param inputFile the path of the input file whose attributes should be 
     * cloned
     * @param outputFile the path of the output file 
     * @param prog the Progressable to report status during the file write
     * @return Writer
     * @throws IOException
     */
    public Writer cloneFileAttributes(Path inputFile, Path outputFile, 
                                      Progressable prog) 
    throws IOException {
      FileSystem srcFileSys = inputFile.getFileSystem(conf);
      Reader reader = new Reader(srcFileSys, inputFile, 4096, conf, true);
      boolean compress = reader.isCompressed();
      boolean blockCompress = reader.isBlockCompressed();
      CompressionCodec codec = reader.getCompressionCodec();
      reader.close();

      Writer writer = createWriter(outputFile.getFileSystem(conf), conf, 
                                   outputFile, keyClass, valClass, compress, 
                                   blockCompress, codec, prog,
                                   new Metadata());
      return writer;
    }

    /**
     * Writes records from RawKeyValueIterator into a file represented by the 
     * passed writer
     * @param records the RawKeyValueIterator
     * @param writer the Writer created earlier 
     * @throws IOException
     */
    public void writeFile(RawKeyValueIterator records, Writer writer) 
      throws IOException {
      while(records.next()) {
        writer.appendRaw(records.getKey().getData(), 0, 
                         records.getKey().getLength(), records.getValue());
      }
      writer.sync();
    }
        
    /** Merge the provided files.
     * @param inFiles the array of input path names
     * @param outFile the final output file
     * @throws IOException
     */
    public void merge(Path[] inFiles, Path outFile) throws IOException {
      if (fs.exists(outFile)) {
        throw new IOException("already exists: " + outFile);
      }
      RawKeyValueIterator r = merge(inFiles, false, outFile.getParent());
      Writer writer = cloneFileAttributes(inFiles[0], outFile, null);
      
      writeFile(r, writer);

      writer.close();
    }

    /** sort calls this to generate the final merged output */
    private int mergePass(Path tmpDir) throws IOException {
      LOG.debug("running merge pass");
      Writer writer = cloneFileAttributes(
                                          outFile.suffix(".0"), outFile, null);
      RawKeyValueIterator r = merge(outFile.suffix(".0"), 
                                    outFile.suffix(".0.index"), tmpDir);
      writeFile(r, writer);

      writer.close();
      return 0;
    }

    /** Used by mergePass to merge the output of the sort
     * @param inName the name of the input file containing sorted segments
     * @param indexIn the offsets of the sorted segments
     * @param tmpDir the relative directory to store intermediate results in
     * @return RawKeyValueIterator
     * @throws IOException
     */
    private RawKeyValueIterator merge(Path inName, Path indexIn, Path tmpDir) 
      throws IOException {
      //get the segments from indexIn
      //we create a SegmentContainer so that we can track segments belonging to
      //inName and delete inName as soon as we see that we have looked at all
      //the contained segments during the merge process & hence don't need 
      //them anymore
      SegmentContainer container = new SegmentContainer(inName, indexIn);
      MergeQueue mQueue = new MergeQueue(container.getSegmentList(), tmpDir, progressable);
      return mQueue.merge();
    }
    
    /** This class implements the core of the merge logic */
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
    } // SequenceFile.Sorter.MergeQueue

    /** This class defines a merge segment. This class can be subclassed to 
     * provide a customized cleanup method implementation. In this 
     * implementation, cleanup closes the file handle and deletes the file 
     */
    public class SegmentDescriptor implements Comparable {
      
      long segmentOffset; //the start of the segment in the file
      long segmentLength; //the length of the segment
      Path segmentPathName; //the path name of the file containing the segment
      boolean ignoreSync = true; //set to true for temp files
      private Reader in = null; 
      private DataOutputBuffer rawKey = null; //this will hold the current key
      private boolean preserveInput = false; //delete input segment files?
      
      /** Constructs a segment
       * @param segmentOffset the offset of the segment in the file
       * @param segmentLength the length of the segment
       * @param segmentPathName the path name of the file containing the segment
       */
      public SegmentDescriptor (long segmentOffset, long segmentLength, 
                                Path segmentPathName) {
        this.segmentOffset = segmentOffset;
        this.segmentLength = segmentLength;
        this.segmentPathName = segmentPathName;
      }
      
      /** Do the sync checks */
      public void doSync() {ignoreSync = false;}
      
      /** Whether to delete the files when no longer needed */
      public void preserveInput(boolean preserve) {
        preserveInput = preserve;
      }

      public boolean shouldPreserveInput() {
        return preserveInput;
      }
      
      public int compareTo(Object o) {
        SegmentDescriptor that = (SegmentDescriptor)o;
        if (this.segmentLength != that.segmentLength) {
          return (this.segmentLength < that.segmentLength ? -1 : 1);
        }
        if (this.segmentOffset != that.segmentOffset) {
          return (this.segmentOffset < that.segmentOffset ? -1 : 1);
        }
        return (this.segmentPathName.toString()).
          compareTo(that.segmentPathName.toString());
      }

      public boolean equals(Object o) {
        if (!(o instanceof SegmentDescriptor)) {
          return false;
        }
        SegmentDescriptor that = (SegmentDescriptor)o;
        if (this.segmentLength == that.segmentLength &&
            this.segmentOffset == that.segmentOffset &&
            this.segmentPathName.toString().equals(
              that.segmentPathName.toString())) {
          return true;
        }
        return false;
      }

      public int hashCode() {
        return 37 * 17 + (int) (segmentOffset^(segmentOffset>>>32));
      }

      /** Fills up the rawKey object with the key returned by the Reader
       * @return true if there is a key returned; false, otherwise
       * @throws IOException
       */
      public boolean nextRawKey() throws IOException {
        if (in == null) {
          int bufferSize = conf.getInt("io.file.buffer.size", 4096); 
          if (fs.getUri().getScheme().startsWith("ramfs")) {
            bufferSize = conf.getInt("io.bytes.per.checksum", 512);
          }
          Reader reader = new Reader(fs, segmentPathName, 
                                     bufferSize, segmentOffset, 
                                     segmentLength, conf, false);
        
          //sometimes we ignore syncs especially for temp merge files
          if (ignoreSync) reader.sync = null;

          if (reader.getKeyClass() != keyClass)
            throw new IOException("wrong key class: " + reader.getKeyClass() +
                                  " is not " + keyClass);
          if (reader.getValueClass() != valClass)
            throw new IOException("wrong value class: "+reader.getValueClass()+
                                  " is not " + valClass);
          this.in = reader;
          rawKey = new DataOutputBuffer();
        }
        rawKey.reset();
        int keyLength = 
          in.nextRawKey(rawKey);
        return (keyLength >= 0);
      }

      /** Fills up the passed rawValue with the value corresponding to the key
       * read earlier
       * @param rawValue
       * @return the length of the value
       * @throws IOException
       */
      public int nextRawValue(ValueBytes rawValue) throws IOException {
        int valLength = in.nextRawValue(rawValue);
        return valLength;
      }
      
      /** Returns the stored rawKey */
      public DataOutputBuffer getKey() {
        return rawKey;
      }
      
      /** closes the underlying reader */
      private void close() throws IOException {
        this.in.close();
        this.in = null;
      }

      /** The default cleanup. Subclasses can override this with a custom 
       * cleanup 
       */
      public void cleanup() throws IOException {
        close();
        if (!preserveInput) {
          fs.delete(segmentPathName, true);
        }
      }
    } // SequenceFile.Sorter.SegmentDescriptor
    
    /** This class provisions multiple segments contained within a single
     *  file
     */
    private class LinkedSegmentsDescriptor extends SegmentDescriptor {

      SegmentContainer parentContainer = null;

      /** Constructs a segment
       * @param segmentOffset the offset of the segment in the file
       * @param segmentLength the length of the segment
       * @param segmentPathName the path name of the file containing the segment
       * @param parent the parent SegmentContainer that holds the segment
       */
      public LinkedSegmentsDescriptor (long segmentOffset, long segmentLength, 
                                       Path segmentPathName, SegmentContainer parent) {
        super(segmentOffset, segmentLength, segmentPathName);
        this.parentContainer = parent;
      }
      /** The default cleanup. Subclasses can override this with a custom 
       * cleanup 
       */
      public void cleanup() throws IOException {
        super.close();
        if (super.shouldPreserveInput()) return;
        parentContainer.cleanup();
      }
    } //SequenceFile.Sorter.LinkedSegmentsDescriptor

    /** The class that defines a container for segments to be merged. Primarily
     * required to delete temp files as soon as all the contained segments
     * have been looked at */
    private class SegmentContainer {
      private int numSegmentsCleanedUp = 0; //track the no. of segment cleanups
      private int numSegmentsContained; //# of segments contained
      private Path inName; //input file from where segments are created
      
      //the list of segments read from the file
      private ArrayList <SegmentDescriptor> segments = 
        new ArrayList <SegmentDescriptor>();
      /** This constructor is there primarily to serve the sort routine that 
       * generates a single output file with an associated index file */
      public SegmentContainer(Path inName, Path indexIn) throws IOException {
        //get the segments from indexIn
        FSDataInputStream fsIndexIn = fs.open(indexIn);
        long end = fs.getLength(indexIn);
        while (fsIndexIn.getPos() < end) {
          long segmentOffset = WritableUtils.readVLong(fsIndexIn);
          long segmentLength = WritableUtils.readVLong(fsIndexIn);
          Path segmentName = inName;
          segments.add(new LinkedSegmentsDescriptor(segmentOffset, 
                                                    segmentLength, segmentName, this));
        }
        fsIndexIn.close();
        fs.delete(indexIn, true);
        numSegmentsContained = segments.size();
        this.inName = inName;
      }

      public List <SegmentDescriptor> getSegmentList() {
        return segments;
      }
      public void cleanup() throws IOException {
        numSegmentsCleanedUp++;
        if (numSegmentsCleanedUp == numSegmentsContained) {
          fs.delete(inName, true);
        }
      }
    } //SequenceFile.Sorter.SegmentContainer

  }