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
/** A class that receives a block and writes to its own disk, meanwhile
 * may copies it to another site. If a throttler is provided,
 * streaming throttling is also supported.
 **/
package org.apache.hadoop.hdfs.server.datanode;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.zip.CRC32;
import java.util.zip.Checksum;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FSInputChecker;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;
import static org.apache.hadoop.hdfs.server.datanode.DataNode.DN_CLIENTTRACE_FORMAT;

class PacketResponder implements Runnable, FSConstants {   

    //packet waiting for ack
    private LinkedList<Packet> ackQueue = new LinkedList<Packet>(); 
    private volatile boolean running = true;
    private Block block;
    DataInputStream mirrorIn;   // input from downstream datanode
    DataOutputStream replyOut;  // output to upstream datanode
    private int numTargets;     // number of downstream datanodes including myself
    private BlockReceiver receiver; // The owner of this responder.

    public String toString() {
      return "PacketResponder " + numTargets + " for Block " + this.block;
    }

    PacketResponder(BlockReceiver receiver, Block b, DataInputStream in, 
                    DataOutputStream out, int numTargets) {
      this.receiver = receiver;
      this.block = b;
      mirrorIn = in;
      replyOut = out;
      this.numTargets = numTargets;
    }

    /**
     * enqueue the seqno that is still be to acked by the downstream datanode.
     * @param seqno
     * @param lastPacketInBlock
     */
    synchronized void enqueue(long seqno, boolean lastPacketInBlock) {
      if (running) {
        LOG.debug("PacketResponder " + numTargets + " adding seqno " + seqno +
                  " to ack queue.");
        ackQueue.addLast(new Packet(seqno, lastPacketInBlock));
        notifyAll();
      }
    }

    /**
     * wait for all pending packets to be acked. Then shutdown thread.
     */
    synchronized void close() {
      while (running && ackQueue.size() != 0 && datanode.shouldRun) {
        try {
          wait();
        } catch (InterruptedException e) {
          running = false;
        }
      }
      LOG.debug("PacketResponder " + numTargets +
               " for block " + block + " Closing down.");
      running = false;
      notifyAll();
    }

    private synchronized void lastDataNodeRun() {
      long lastHeartbeat = System.currentTimeMillis();
      boolean lastPacket = false;

      while (running && datanode.shouldRun && !lastPacket) {
        long now = System.currentTimeMillis();
        try {

            // wait for a packet to be sent to downstream datanode
            while (running && datanode.shouldRun && ackQueue.size() == 0) {
              long idle = now - lastHeartbeat;
              long timeout = (datanode.socketTimeout/2) - idle;
              if (timeout <= 0) {
                timeout = 1000;
              }
              try {
                wait(timeout);
              } catch (InterruptedException e) {
                if (running) {
                  LOG.info("PacketResponder " + numTargets +
                           " for block " + block + " Interrupted.");
                  running = false;
                }
                break;
              }
          
              // send a heartbeat if it is time.
              now = System.currentTimeMillis();
              if (now - lastHeartbeat > datanode.socketTimeout/2) {
                replyOut.writeLong(-1); // send heartbeat
                replyOut.flush();
                lastHeartbeat = now;
              }
            }

            if (!running || !datanode.shouldRun) {
              break;
            }
            Packet pkt = ackQueue.removeFirst();
            long expected = pkt.seqno;
            notifyAll();
            LOG.debug("PacketResponder " + numTargets +
                      " for block " + block + 
                      " acking for packet " + expected);

            // If this is the last packet in block, then close block
            // file and finalize the block before responding success
            if (pkt.lastPacketInBlock) {
              if (!receiver.finalized) {
                receiver.close();
                block.setNumBytes(receiver.offsetInBlock);
                datanode.data.finalizeBlock(block);
                datanode.myMetrics.blocksWritten.inc();
                datanode.notifyNamenodeReceivedBlock(block, 
                    DataNode.EMPTY_DEL_HINT);
                if (ClientTraceLog.isInfoEnabled() &&
                    receiver.clientName.length() > 0) {
                  ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT,
                        receiver.inAddr, receiver.myAddr, block.getNumBytes(),
                        "HDFS_WRITE", receiver.clientName,
                        datanode.dnRegistration.getStorageID(), block));
                } else {
                  LOG.info("Received block " + block + 
                           " of size " + block.getNumBytes() + 
                           " from " + receiver.inAddr);
                }
              }
              lastPacket = true;
            }

            replyOut.writeLong(expected);
            replyOut.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS);
            replyOut.flush();
        } catch (Exception e) {
          if (running) {
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
          }
        }
      }
      LOG.info("PacketResponder " + numTargets + 
               " for block " + block + " terminating");
    }

    /**
     * Thread to process incoming acks.
     * @see java.lang.Runnable#run()
     */
    public void run() {

      // If this is the last datanode in pipeline, then handle differently
      if (numTargets == 0) {
        lastDataNodeRun();
        return;
      }

      boolean lastPacketInBlock = false;
      while (running && datanode.shouldRun && !lastPacketInBlock) {

        try {
            short op = DataTransferProtocol.OP_STATUS_SUCCESS;
            boolean didRead = false;
            long expected = -2;
            try { 
              // read seqno from downstream datanode
              long seqno = mirrorIn.readLong();
              didRead = true;
              if (seqno == -1) {
                replyOut.writeLong(-1); // send keepalive
                replyOut.flush();
                LOG.debug("PacketResponder " + numTargets + " got -1");
                continue;
              } else if (seqno == -2) {
                LOG.debug("PacketResponder " + numTargets + " got -2");
              } else {
                LOG.debug("PacketResponder " + numTargets + " got seqno = " + 
                    seqno);
                Packet pkt = null;
                synchronized (this) {
                  while (running && datanode.shouldRun && ackQueue.size() == 0) {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("PacketResponder " + numTargets + 
                                " seqno = " + seqno +
                                " for block " + block +
                                " waiting for local datanode to finish write.");
                    }
                    wait();
                  }
                  pkt = ackQueue.removeFirst();
                  expected = pkt.seqno;
                  notifyAll();
                  LOG.debug("PacketResponder " + numTargets + " seqno = " + seqno);
                  if (seqno != expected) {
                    throw new IOException("PacketResponder " + numTargets +
                                          " for block " + block +
                                          " expected seqno:" + expected +
                                          " received:" + seqno);
                  }
                  lastPacketInBlock = pkt.lastPacketInBlock;
                }
              }
            } catch (Throwable e) {
              if (running) {
                LOG.info("PacketResponder " + block + " " + numTargets + 
                         " Exception " + StringUtils.stringifyException(e));
                running = false;
              }
            }

            if (Thread.interrupted()) {
              /* The receiver thread cancelled this thread. 
               * We could also check any other status updates from the 
               * receiver thread (e.g. if it is ok to write to replyOut). 
               * It is prudent to not send any more status back to the client
               * because this datanode has a problem. The upstream datanode
               * will detect a timout on heartbeats and will declare that
               * this datanode is bad, and rightly so.
               */
              LOG.info("PacketResponder " + block +  " " + numTargets +
                       " : Thread is interrupted.");
              running = false;
              continue;
            }
            
            if (!didRead) {
              op = DataTransferProtocol.OP_STATUS_ERROR;
            }
            
            // If this is the last packet in block, then close block
            // file and finalize the block before responding success
            if (lastPacketInBlock && !receiver.finalized) {
              receiver.close();
              block.setNumBytes(receiver.offsetInBlock);
              datanode.data.finalizeBlock(block);
              datanode.myMetrics.blocksWritten.inc();
              datanode.notifyNamenodeReceivedBlock(block, 
                  DataNode.EMPTY_DEL_HINT);
              if (ClientTraceLog.isInfoEnabled() &&
                  receiver.clientName.length() > 0) {
                ClientTraceLog.info(String.format(DN_CLIENTTRACE_FORMAT,
                      receiver.inAddr, receiver.myAddr, block.getNumBytes(),
                      "HDFS_WRITE", receiver.clientName,
                      datanode.dnRegistration.getStorageID(), block));
              } else {
                LOG.info("Received block " + block + 
                         " of size " + block.getNumBytes() + 
                         " from " + receiver.inAddr);
              }
            }

            // send my status back to upstream datanode
            replyOut.writeLong(expected); // send seqno upstream
            replyOut.writeShort(DataTransferProtocol.OP_STATUS_SUCCESS);

            LOG.debug("PacketResponder " + numTargets + 
                      " for block " + block +
                      " responded my status " +
                      " for seqno " + expected);

            // forward responses from downstream datanodes.
            for (int i = 0; i < numTargets && datanode.shouldRun; i++) {
              try {
                if (op == DataTransferProtocol.OP_STATUS_SUCCESS) {
                  op = mirrorIn.readShort();
                  if (op != DataTransferProtocol.OP_STATUS_SUCCESS) {
                    LOG.debug("PacketResponder for block " + block +
                              ": error code received from downstream " +
                              " datanode[" + i + "] " + op);
                  }
                }
              } catch (Throwable e) {
                op = DataTransferProtocol.OP_STATUS_ERROR;
              }
              replyOut.writeShort(op);
            }
            replyOut.flush();
            LOG.debug("PacketResponder " + block + " " + numTargets + 
                      " responded other status " + " for seqno " + expected);

            // If we were unable to read the seqno from downstream, then stop.
            if (expected == -2) {
              running = false;
            }
            // If we forwarded an error response from a downstream datanode
            // and we are acting on behalf of a client, then we quit. The 
            // client will drive the recovery mechanism.
            if (op == DataTransferProtocol.OP_STATUS_ERROR && receiver.clientName.length() > 0) {
              running = false;
            }
        } catch (IOException e) {
          if (running) {
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
          }
        } catch (RuntimeException e) {
          if (running) {
            LOG.info("PacketResponder " + block + " " + numTargets + 
                     " Exception " + StringUtils.stringifyException(e));
            running = false;
          }
        }
      }
      LOG.info("PacketResponder " + numTargets + 
               " for block " + block + " terminating");
    }
  }