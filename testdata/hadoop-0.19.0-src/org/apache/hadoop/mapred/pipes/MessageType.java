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
 * This protocol is a binary implementation of the Pipes protocol.
 */
package org.apache.hadoop.mapred.pipes;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;

private static enum MessageType { START(0),
                                    SET_JOB_CONF(1),
                                    SET_INPUT_TYPES(2),
                                    RUN_MAP(3),
                                    MAP_ITEM(4),
                                    RUN_REDUCE(5),
                                    REDUCE_KEY(6),
                                    REDUCE_VALUE(7),
                                    CLOSE(8),
                                    ABORT(9),
                                    OUTPUT(50),
                                    PARTITIONED_OUTPUT(51),
                                    STATUS(52),
                                    PROGRESS(53),
                                    DONE(54),
                                    REGISTER_COUNTER(55),
                                    INCREMENT_COUNTER(56);
    final int code;
    MessageType(int code) {
      this.code = code;
    }
  }