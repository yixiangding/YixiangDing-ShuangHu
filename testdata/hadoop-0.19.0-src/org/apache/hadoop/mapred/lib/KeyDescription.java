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
 * This is used in {@link KeyFieldBasedComparator} & 
 * {@link KeyFieldBasedPartitioner}. Defines all the methods
 * for parsing key specifications. The key specification is of the form:
 * -k pos1[,pos2], where pos is of the form f[.c][opts], where f is the number
 *  of the field to use, and c is the number of the first character from the
 *  beginning of the field. Fields and character posns are numbered starting
 *  with 1; a character position of zero in pos2 indicates the field's last
 *  character. If '.c' is omitted from pos1, it defaults to 1 (the beginning
 *  of the field); if omitted from pos2, it defaults to 0 (the end of the
 *  field). opts are ordering options (supported options are 'nr'). 
 */
package org.apache.hadoop.mapred.lib;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.util.UTF8ByteArrayUtils;

protected static class KeyDescription {
    int beginFieldIdx = 1;
    int beginChar = 1;
    int endFieldIdx = 0;
    int endChar = 0;
    boolean numeric;
    boolean reverse;
  }