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
 * This supports input and output streams for a socket channels. 
 * These streams can have a timeout.
 */
package org.apache.hadoop.net;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

private static class SelectorPool {
    
    private static class SelectorInfo {
      Selector              selector;
      long                  lastActivityTime;
      LinkedList<SelectorInfo> queue; 
      
      void close() {
        if (selector != null) {
          try {
            selector.close();
          } catch (IOException e) {
            LOG.warn("Unexpected exception while closing selector : " +
                     StringUtils.stringifyException(e));
          }
        }
      }    
    }
    
    private static class ProviderInfo {
      SelectorProvider provider;
      LinkedList<SelectorInfo> queue; // lifo
      ProviderInfo next;
    }
    
    private static final long IDLE_TIMEOUT = 10 * 1000; // 10 seconds.
    
    private ProviderInfo providerList = null;
    
    /**
     * Waits on the channel with the given timeout using one of the 
     * cached selectors. It also removes any cached selectors that are
     * idle for a few seconds.
     * 
     * @param channel
     * @param ops
     * @param timeout
     * @return
     * @throws IOException
     */
    int select(SelectableChannel channel, int ops, long timeout) 
                                                   throws IOException {
     
      SelectorInfo info = get(channel);
      
      SelectionKey key = null;
      int ret = 0;
      
      try {
        while (true) {
          long start = (timeout == 0) ? 0 : System.currentTimeMillis();

          key = channel.register(info.selector, ops);
          ret = info.selector.select(timeout);
          
          if (ret != 0) {
            return ret;
          }
          
          /* Sometimes select() returns 0 much before timeout for 
           * unknown reasons. So select again if required.
           */
          if (timeout > 0) {
            timeout -= System.currentTimeMillis() - start;
            if (timeout <= 0) {
              return 0;
            }
          }
          
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Interruped while waiting for " +
                                             "IO on channel " + channel +
                                             ". " + timeout + 
                                             " millis timeout left.");
          }
        }
      } finally {
        if (key != null) {
          key.cancel();
        }
        
        //clear the canceled key.
        try {
          info.selector.selectNow();
        } catch (IOException e) {
          LOG.info("Unexpected Exception while clearing selector : " +
                   StringUtils.stringifyException(e));
          // don't put the selector back.
          info.close();
          return ret; 
        }
        
        release(info);
      }
    }
    
    /**
     * Takes one selector from end of LRU list of free selectors.
     * If there are no selectors awailable, it creates a new selector.
     * Also invokes trimIdleSelectors(). 
     * 
     * @param channel
     * @return 
     * @throws IOException
     */
    private synchronized SelectorInfo get(SelectableChannel channel) 
                                                         throws IOException {
      SelectorInfo selInfo = null;
      
      SelectorProvider provider = channel.provider();
      
      // pick the list : rarely there is more than one provider in use.
      ProviderInfo pList = providerList;
      while (pList != null && pList.provider != provider) {
        pList = pList.next;
      }      
      if (pList == null) {
        //LOG.info("Creating new ProviderInfo : " + provider.toString());
        pList = new ProviderInfo();
        pList.provider = provider;
        pList.queue = new LinkedList<SelectorInfo>();
        pList.next = providerList;
        providerList = pList;
      }
      
      LinkedList<SelectorInfo> queue = pList.queue;
      
      if (queue.isEmpty()) {
        Selector selector = provider.openSelector();
        selInfo = new SelectorInfo();
        selInfo.selector = selector;
        selInfo.queue = queue;
      } else {
        selInfo = queue.removeLast();
      }
      
      trimIdleSelectors(System.currentTimeMillis());
      return selInfo;
    }
    
    /**
     * puts selector back at the end of LRU list of free selectos.
     * Also invokes trimIdleSelectors().
     * 
     * @param info
     */
    private synchronized void release(SelectorInfo info) {
      long now = System.currentTimeMillis();
      trimIdleSelectors(now);
      info.lastActivityTime = now;
      info.queue.addLast(info);
    }
    
    /**
     * Closes selectors that are idle for IDLE_TIMEOUT (10 sec). It does not
     * traverse the whole list, just over the one that have crossed 
     * the timeout.
     */
    private void trimIdleSelectors(long now) {
      long cutoff = now - IDLE_TIMEOUT;
      
      for(ProviderInfo pList=providerList; pList != null; pList=pList.next) {
        if (pList.queue.isEmpty()) {
          continue;
        }
        for(Iterator<SelectorInfo> it = pList.queue.iterator(); it.hasNext();) {
          SelectorInfo info = it.next();
          if (info.lastActivityTime > cutoff) {
            break;
          }
          it.remove();
          info.close();
        }
      }
    }
  }