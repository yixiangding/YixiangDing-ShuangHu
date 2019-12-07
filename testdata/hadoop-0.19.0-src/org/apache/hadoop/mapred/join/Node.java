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

public abstract static class Node implements ComposableInputFormat {
    /**
     * Return the node type registered for the particular identifier.
     * By default, this is a CNode for any composite node and a WNode
     * for &quot;wrapped&quot; nodes. User nodes will likely be composite
     * nodes.
     * @see #addIdentifier(java.lang.String, java.lang.Class[], java.lang.Class, java.lang.Class)
     * @see CompositeInputFormat#setFormat(org.apache.hadoop.mapred.JobConf)
     */
    static Node forIdent(String ident) throws IOException {
      try {
        if (!nodeCstrMap.containsKey(ident)) {
          throw new IOException("No nodetype for " + ident);
        }
        return nodeCstrMap.get(ident).newInstance(ident);
      } catch (IllegalAccessException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InstantiationException e) {
        throw (IOException)new IOException().initCause(e);
      } catch (InvocationTargetException e) {
        throw (IOException)new IOException().initCause(e);
      }
    }

    private static final Class<?>[] ncstrSig = { String.class };
    private static final
        Map<String,Constructor<? extends Node>> nodeCstrMap =
        new HashMap<String,Constructor<? extends Node>>();
    protected static final
        Map<String,Constructor<? extends ComposableRecordReader>> rrCstrMap =
        new HashMap<String,Constructor<? extends ComposableRecordReader>>();

    /**
     * For a given identifier, add a mapping to the nodetype for the parse
     * tree and to the ComposableRecordReader to be created, including the
     * formals required to invoke the constructor.
     * The nodetype and constructor signature should be filled in from the
     * child node.
     */
    protected static void addIdentifier(String ident, Class<?>[] mcstrSig,
                              Class<? extends Node> nodetype,
                              Class<? extends ComposableRecordReader> cl)
        throws NoSuchMethodException {
      Constructor<? extends Node> ncstr =
        nodetype.getDeclaredConstructor(ncstrSig);
      ncstr.setAccessible(true);
      nodeCstrMap.put(ident, ncstr);
      Constructor<? extends ComposableRecordReader> mcstr =
        cl.getDeclaredConstructor(mcstrSig);
      mcstr.setAccessible(true);
      rrCstrMap.put(ident, mcstr);
    }

    // inst
    protected int id = -1;
    protected String ident;
    protected Class<? extends WritableComparator> cmpcl;

    protected Node(String ident) {
      this.ident = ident;
    }

    protected void setID(int id) {
      this.id = id;
    }

    protected void setKeyComparator(Class<? extends WritableComparator> cmpcl) {
      this.cmpcl = cmpcl;
    }
    abstract void parse(List<Token> args, JobConf job) throws IOException;
  }