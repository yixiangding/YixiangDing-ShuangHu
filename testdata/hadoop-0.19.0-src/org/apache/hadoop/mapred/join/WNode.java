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
 * Very simple shift-reduce parser for join expressions.
 *
 * This should be sufficient for the user extension permitted now, but ought to
 * be replaced with a parser generator if more complex grammars are supported.
 * In particular, this &quot;shift-reduce&quot; parser has no states. Each set
 * of formals requires a different internal node type, which is responsible for
 * interpreting the list of tokens it receives. This is sufficient for the
 * current grammar, but it has several annoying properties that might inhibit
 * extension. In particular, parenthesis are always function calls; an
 * algebraic or filter grammar would not only require a node type, but must
 * also work around the internals of this parser.
 *
 * For most other cases, adding classes to the hierarchy- particularly by
 * extending JoinRecordReader and MultiFilterRecordReader- is fairly
 * straightforward. One need only override the relevant method(s) (usually only
 * {@link CompositeRecordReader#combine}) and include a property to map its
 * value to an identifier in the parser.
 */
package org.apache.hadoop.mapred.join;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Stack;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

static class WNode extends Node {
    private static final Class<?>[] cstrSig =
      { Integer.TYPE, RecordReader.class, Class.class };

    static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, WNode.class, cl);
    }

    private String indir;
    private InputFormat inf;

    public WNode(String ident) {
      super(ident);
    }

    /**
     * Let the first actual define the InputFormat and the second define
     * the <tt>mapred.input.dir</tt> property.
     */
    public void parse(List<Token> ll, JobConf job) throws IOException {
      StringBuilder sb = new StringBuilder();
      Iterator<Token> i = ll.iterator();
      while (i.hasNext()) {
        Token t = i.next();
        if (TType.COMMA.equals(t.getType())) {
          try {
          	inf = (InputFormat)ReflectionUtils.newInstance(
          			job.getClassByName(sb.toString()),
                job);
          } catch (ClassNotFoundException e) {
            throw (IOException)new IOException().initCause(e);
          } catch (IllegalArgumentException e) {
            throw (IOException)new IOException().initCause(e);
          }
          break;
        }
        sb.append(t.getStr());
      }
      if (!i.hasNext()) {
        throw new IOException("Parse error");
      }
      Token t = i.next();
      if (!TType.QUOT.equals(t.getType())) {
        throw new IOException("Expected quoted string");
      }
      indir = t.getStr();
      // no check for ll.isEmpty() to permit extension
    }

    private JobConf getConf(JobConf job) {
      JobConf conf = new JobConf(job);
      FileInputFormat.setInputPaths(conf, indir);
      return conf;
    }

    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      return inf.getSplits(getConf(job), numSplits);
    }

    public ComposableRecordReader getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        return rrCstrMap.get(ident).newInstance(id,
            inf.getRecordReader(split, getConf(job), reporter), cmpcl);
      } catch (IllegalAccessException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InvocationTargetException e) {
        throw (IOException)new IOException().initCause(e);
      }
    }

    public String toString() {
      return ident + "(" + inf.getClass().getName() + ",\"" + indir + "\")";
    }
  }