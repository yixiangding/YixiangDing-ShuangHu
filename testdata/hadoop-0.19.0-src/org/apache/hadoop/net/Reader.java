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
 * This implements an input stream that can have a timeout while reading.
 * This sets non-blocking flag on the socket channel.
 * So after create this object, read() on 
 * {@link Socket#getInputStream()} and write() on 
 * {@link Socket#getOutputStream()} for the associated socket will throw 
 * IllegalBlockingModeException. 
 * Please use {@link SocketOutputStream} for writing.
 */
package org.apache.hadoop.net;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

private static class Reader extends SocketIOWithTimeout {
    ReadableByteChannel channel;
    
    Reader(ReadableByteChannel channel, long timeout) throws IOException {
      super((SelectableChannel)channel, timeout);
      this.channel = channel;
    }
    
    int performIO(ByteBuffer buf) throws IOException {
      return channel.read(buf);
    }
  }