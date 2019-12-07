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
 * The MultipleOutputs class simplifies writting to additional outputs other
 * than the job default output via the <code>OutputCollector</code> passed to
 * the <code>map()</code> and <code>reduce()</code> methods of the
 * <code>Mapper</code> and <code>Reducer</code> implementations.
 * <p/>
 * Each additional output, or named output, may be configured with its own
 * <code>OutputFormat</code>, with its own key class and with its own value
 * class.
 * <p/>
 * A named output can be a single file or a multi file. The later is refered as
 * a multi named output.
 * <p/>
 * A multi named output is an unbound set of files all sharing the same
 * <code>OutputFormat</code>, key class and value class configuration.
 * <p/>
 * When named outputs are used within a <code>Mapper</code> implementation,
 * key/values written to a name output are not part of the reduce phase, only
 * key/values written to the job <code>OutputCollector</code> are part of the
 * reduce phase.
 * <p/>
 * MultipleOutputs supports counters, by default the are disabled. The counters
 * group is the {@link MultipleOutputs} class name.
 * </p>
 * The names of the counters are the same as the named outputs. For multi
 * named outputs the name of the counter is the concatenation of the named
 * output, and underscore '_' and the multiname.
 * <p/>
 * Job configuration usage pattern is:
 * <pre>
 *
 * JobConf conf = new JobConf();
 *
 * conf.setInputPath(inDir);
 * FileOutputFormat.setOutputPath(conf, outDir);
 *
 * conf.setMapperClass(MOMap.class);
 * conf.setReducerClass(MOReduce.class);
 * ...
 *
 * // Defines additional single text based output 'text' for the job
 * MultipleOutputs.addNamedOutput(conf, "text", TextOutputFormat.class,
 * LongWritable.class, Text.class);
 *
 * // Defines additional multi sequencefile based output 'sequence' for the
 * // job
 * MultipleOutputs.addMultiNamedOutput(conf, "seq",
 *   SequenceFileOutputFormat.class,
 *   LongWritable.class, Text.class);
 * ...
 *
 * JobClient jc = new JobClient();
 * RunningJob job = jc.submitJob(conf);
 *
 * ...
 * </pre>
 * <p/>
 * Job configuration usage pattern is:
 * <pre>
 *
 * public class MOReduce implements
 *   Reducer&lt;WritableComparable, Writable&gt; {
 * private MultipleOutputs mos;
 *
 * public void configure(JobConf conf) {
 * ...
 * mos = new MultipleOutputs(conf);
 * }
 *
 * public void reduce(WritableComparable key, Iterator&lt;Writable&gt; values,
 * OutputCollector output, Reporter reporter)
 * throws IOException {
 * ...
 * mos.getCollector("text", reporter).collect(key, new Text("Hello"));
 * mos.getCollector("seq", "A", reporter).collect(key, new Text("Bye"));
 * mos.getCollector("seq", "B", reporter).collect(key, new Text("Chau"));
 * ...
 * }
 *
 * public void close() throws IOException {
 * mos.close();
 * ...
 * }
 *
 * }
 * </pre>
 */
package org.apache.hadoop.mapred.lib;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import java.io.IOException;
import java.util.*;

private static class RecordWriterWithCounter implements RecordWriter {
    private RecordWriter writer;
    private String counterName;
    private Reporter reporter;

    public RecordWriterWithCounter(RecordWriter writer, String counterName,
                                   Reporter reporter) {
      this.writer = writer;
      this.counterName = counterName;
      this.reporter = reporter;
    }

    @SuppressWarnings({"unchecked"})
    public void write(Object key, Object value) throws IOException {
      reporter.incrCounter(COUNTERS_GROUP, counterName, 1);
      writer.write(key, value);
    }

    public void close(Reporter reporter) throws IOException {
      writer.close(reporter);
    }
  }