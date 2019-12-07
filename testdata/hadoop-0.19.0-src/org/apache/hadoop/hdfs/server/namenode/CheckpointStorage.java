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
/**********************************************************
 * The Secondary NameNode is a helper to the primary NameNode.
 * The Secondary is responsible for supporting periodic checkpoints 
 * of the HDFS metadata. The current design allows only one Secondary
 * NameNode per HDFs cluster.
 *
 * The Secondary NameNode is a daemon that periodically wakes
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * The Secondary NameNode uses the ClientProtocol to talk to the
 * primary NameNode.
 *
 **********************************************************/
package org.apache.hadoop.hdfs.server.namenode;
import org.apache.commons.logging.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.ipc.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.net.NetUtils;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.hadoop.metrics.jvm.JvmMetrics;

static class CheckpointStorage extends FSImage {
    /**
     */
    CheckpointStorage() throws IOException {
      super();
    }

    @Override
    public
    boolean isConversionNeeded(StorageDirectory sd) {
      return false;
    }

    /**
     * Analyze checkpoint directories.
     * Create directories if they do not exist.
     * Recover from an unsuccessful checkpoint is necessary. 
     * 
     * @param dataDirs
     * @param editsDirs
     * @throws IOException
     */
    void recoverCreate(Collection<File> dataDirs,
                       Collection<File> editsDirs) throws IOException {
      Collection<File> tempDataDirs = new ArrayList<File>(dataDirs);
      Collection<File> tempEditsDirs = new ArrayList<File>(editsDirs);
      this.storageDirs = new ArrayList<StorageDirectory>();
      setStorageDirectories(tempDataDirs, tempEditsDirs);
      for (Iterator<StorageDirectory> it = 
                   dirIterator(); it.hasNext();) {
        StorageDirectory sd = it.next();
        boolean isAccessible = true;
        try { // create directories if don't exist yet
          if(!sd.getRoot().mkdirs()) {
            // do nothing, directory is already created
          }
        } catch(SecurityException se) {
          isAccessible = false;
        }
        if(!isAccessible)
          throw new InconsistentFSStateException(sd.getRoot(),
              "cannot access checkpoint directory.");
        StorageState curState;
        try {
          curState = sd.analyzeStorage(HdfsConstants.StartupOption.REGULAR);
          // sd is locked but not opened
          switch(curState) {
          case NON_EXISTENT:
            // fail if any of the configured checkpoint dirs are inaccessible 
            throw new InconsistentFSStateException(sd.getRoot(),
                  "checkpoint directory does not exist or is not accessible.");
          case NOT_FORMATTED:
            break;  // it's ok since initially there is no current and VERSION
          case NORMAL:
            break;
          default:  // recovery is possible
            sd.doRecover(curState);
          }
        } catch (IOException ioe) {
          sd.unlock();
          throw ioe;
        }
      }
    }

    /**
     * Prepare directories for a new checkpoint.
     * <p>
     * Rename <code>current</code> to <code>lastcheckpoint.tmp</code>
     * and recreate <code>current</code>.
     * @throws IOException
     */
    void startCheckpoint() throws IOException {
      for(StorageDirectory sd : storageDirs) {
        File curDir = sd.getCurrentDir();
        File tmpCkptDir = sd.getLastCheckpointTmp();
        assert !tmpCkptDir.exists() : 
          tmpCkptDir.getName() + " directory must not exist.";
        if(curDir.exists()) {
          // rename current to tmp
          rename(curDir, tmpCkptDir);
        }
        if (!curDir.mkdir())
          throw new IOException("Cannot create directory " + curDir);
      }
    }

    void endCheckpoint() throws IOException {
      for(StorageDirectory sd : storageDirs) {
        File tmpCkptDir = sd.getLastCheckpointTmp();
        File prevCkptDir = sd.getPreviousCheckpoint();
        // delete previous dir
        if (prevCkptDir.exists())
          deleteDir(prevCkptDir);
        // rename tmp to previous
        if (tmpCkptDir.exists())
          rename(tmpCkptDir, prevCkptDir);
      }
    }

    /**
     * Merge image and edits, and verify consistency with the signature.
     */
    private void doMerge(CheckpointSignature sig) throws IOException {
      getEditLog().open();
      StorageDirectory sdName = null;
      StorageDirectory sdEdits = null;
      Iterator<StorageDirectory> it = null;
      it = dirIterator(NameNodeDirType.IMAGE);
      if (it.hasNext())
        sdName = it.next();
      it = dirIterator(NameNodeDirType.EDITS);
      if (it.hasNext())
        sdEdits = it.next();
      if ((sdName == null) || (sdEdits == null))
        throw new IOException("Could not locate checkpoint directories");
      loadFSImage(FSImage.getImageFile(sdName, NameNodeFile.IMAGE));
      loadFSEdits(sdEdits);
      sig.validateStorageInfo(this);
      saveFSImage();
    }
  }