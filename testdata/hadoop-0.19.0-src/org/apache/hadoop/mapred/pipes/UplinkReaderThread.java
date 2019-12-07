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

private static class UplinkReaderThread<K2 extends WritableComparable,
                                          V2 extends Writable>  
    extends Thread {
    
    private DataInputStream inStream;
    private UpwardProtocol<K2, V2> handler;
    private K2 key;
    private V2 value;
    
    public UplinkReaderThread(InputStream stream,
                              UpwardProtocol<K2, V2> handler, 
                              K2 key, V2 value) throws IOException{
      inStream = new DataInputStream(new BufferedInputStream(stream, 
                                                             BUFFER_SIZE));
      this.handler = handler;
      this.key = key;
      this.value = value;
    }

    public void closeConnection() throws IOException {
      inStream.close();
    }

    public void run() {
      while (true) {
        try {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
          }
          int cmd = WritableUtils.readVInt(inStream);
          LOG.debug("Handling uplink command " + cmd);
          if (cmd == MessageType.OUTPUT.code) {
            readObject(key);
            readObject(value);
            handler.output(key, value);
          } else if (cmd == MessageType.PARTITIONED_OUTPUT.code) {
            int part = WritableUtils.readVInt(inStream);
            readObject(key);
            readObject(value);
            handler.partitionedOutput(part, key, value);
          } else if (cmd == MessageType.STATUS.code) {
            handler.status(Text.readString(inStream));
          } else if (cmd == MessageType.PROGRESS.code) {
            handler.progress(inStream.readFloat());
          } else if (cmd == MessageType.REGISTER_COUNTER.code) {
            int id = WritableUtils.readVInt(inStream);
            String group = Text.readString(inStream);
            String name = Text.readString(inStream);
            handler.registerCounter(id, group, name);
          } else if (cmd == MessageType.INCREMENT_COUNTER.code) {
            int id = WritableUtils.readVInt(inStream);
            long amount = WritableUtils.readVLong(inStream);
            handler.incrementCounter(id, amount);
          } else if (cmd == MessageType.DONE.code) {
            LOG.debug("Pipe child done");
            handler.done();
            return;
          } else {
            throw new IOException("Bad command code: " + cmd);
          }
        } catch (InterruptedException e) {
          return;
        } catch (Throwable e) {
          LOG.error(StringUtils.stringifyException(e));
          handler.failed(e);
          return;
        }
      }
    }
    
    private void readObject(Writable obj) throws IOException {
      int numBytes = WritableUtils.readVInt(inStream);
      byte[] buffer;
      // For BytesWritable and Text, use the specified length to set the length
      // this causes the "obvious" translations to work. So that if you emit
      // a string "abc" from C++, it shows up as "abc".
      if (obj instanceof BytesWritable) {
        buffer = new byte[numBytes];
        inStream.readFully(buffer);
        ((BytesWritable) obj).set(buffer, 0, numBytes);
      } else if (obj instanceof Text) {
        buffer = new byte[numBytes];
        inStream.readFully(buffer);
        ((Text) obj).set(buffer);
      } else {
        obj.readFields(inStream);
      }
    }
  }