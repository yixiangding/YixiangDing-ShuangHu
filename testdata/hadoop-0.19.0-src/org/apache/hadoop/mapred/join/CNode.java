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

static class CNode extends Node {

    private static final Class<?>[] cstrSig =
      { Integer.TYPE, JobConf.class, Integer.TYPE, Class.class };

    static void addIdentifier(String ident,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Node.addIdentifier(ident, cstrSig, CNode.class, cl);
    }

    // inst
    private ArrayList<Node> kids = new ArrayList<Node>();

    public CNode(String ident) {
      super(ident);
    }

    public void setKeyComparator(Class<? extends WritableComparator> cmpcl) {
      super.setKeyComparator(cmpcl);
      for (Node n : kids) {
        n.setKeyComparator(cmpcl);
      }
    }

    /**
     * Combine InputSplits from child InputFormats into a
     * {@link CompositeInputSplit}.
     */
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      InputSplit[][] splits = new InputSplit[kids.size()][];
      for (int i = 0; i < kids.size(); ++i) {
        final InputSplit[] tmp = kids.get(i).getSplits(job, numSplits);
        if (null == tmp) {
          throw new IOException("Error gathering splits from child RReader");
        }
        if (i > 0 && splits[i-1].length != tmp.length) {
          throw new IOException("Inconsistent split cardinality from child " +
              i + " (" + splits[i-1].length + "/" + tmp.length + ")");
        }
        splits[i] = tmp;
      }
      final int size = splits[0].length;
      CompositeInputSplit[] ret = new CompositeInputSplit[size];
      for (int i = 0; i < size; ++i) {
        ret[i] = new CompositeInputSplit(splits.length);
        for (int j = 0; j < splits.length; ++j) {
          ret[i].add(splits[j][i]);
        }
      }
      return ret;
    }

    @SuppressWarnings("unchecked") // child types unknowable
    public ComposableRecordReader getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      if (!(split instanceof CompositeInputSplit)) {
        throw new IOException("Invalid split type:" +
                              split.getClass().getName());
      }
      final CompositeInputSplit spl = (CompositeInputSplit)split;
      final int capacity = kids.size();
      CompositeRecordReader ret = null;
      try {
        if (!rrCstrMap.containsKey(ident)) {
          throw new IOException("No RecordReader for " + ident);
        }
        ret = (CompositeRecordReader)
          rrCstrMap.get(ident).newInstance(id, job, capacity, cmpcl);
      } catch (IllegalAccessException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InvocationTargetException e) {
        throw (IOException)new IOException().initCause(e);
      }
      for (int i = 0; i < capacity; ++i) {
        ret.add(kids.get(i).getRecordReader(spl.get(i), job, reporter));
      }
      return (ComposableRecordReader)ret;
    }

    /**
     * Parse a list of comma-separated nodes.
     */
    public void parse(List<Token> args, JobConf job) throws IOException {
      ListIterator<Token> i = args.listIterator();
      while (i.hasNext()) {
        Token t = i.next();
        t.getNode().setID(i.previousIndex() >> 1);
        kids.add(t.getNode());
        if (i.hasNext() && !TType.COMMA.equals(i.next().getType())) {
          throw new IOException("Expected ','");
        }
      }
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(ident + "(");
      for (Node n : kids) {
        sb.append(n.toString() + ",");
      }
      sb.setCharAt(sb.length() - 1, ')');
      return sb.toString();
    }
  }