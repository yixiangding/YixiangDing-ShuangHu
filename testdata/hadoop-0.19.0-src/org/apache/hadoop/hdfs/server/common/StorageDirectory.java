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
 * Storage information file.
 * <p>
 * Local storage information is stored in a separate file VERSION.
 * It contains type of the node, 
 * the storage layout version, the namespace id, and 
 * the fs state creation time.
 * <p>
 * Local storage can reside in multiple directories. 
 * Each directory should contain the same VERSION file as the others.
 * During startup Hadoop servers (name-node and data-nodes) read their local 
 * storage information from them.
 * <p>
 * The servers hold a lock for each storage directory while they run so that 
 * other nodes were not able to startup sharing the same storage.
 * The locks are released when the servers stop (normally or abnormally).
 * 
 */
package org.apache.hadoop.hdfs.server.common;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;

public class StorageDirectory {
    File              root; // root directory
    FileLock          lock; // storage lock
    StorageDirType dirType; // storage dir type
    
    public StorageDirectory(File dir) {
      // default dirType is null
      this(dir, null);
    }
    
    public StorageDirectory(File dir, StorageDirType dirType) {
      this.root = dir;
      this.lock = null;
      this.dirType = dirType;
    }
    
    /**
     * Get root directory of this storage
     */
    public File getRoot() {
      return root;
    }

    /**
     * Get storage directory type
     */
    public StorageDirType getStorageDirType() {
      return dirType;
    }
    
    /**
     * Read version file.
     * 
     * @throws IOException if file cannot be read or contains inconsistent data
     */
    public void read() throws IOException {
      read(getVersionFile());
    }
    
    public void read(File from) throws IOException {
      RandomAccessFile file = new RandomAccessFile(from, "rws");
      FileInputStream in = null;
      try {
        in = new FileInputStream(file.getFD());
        file.seek(0);
        Properties props = new Properties();
        props.load(in);
        getFields(props, this);
      } finally {
        if (in != null) {
          in.close();
        }
        file.close();
      }
    }

    /**
     * Write version file.
     * 
     * @throws IOException
     */
    public void write() throws IOException {
      corruptPreUpgradeStorage(root);
      write(getVersionFile());
    }

    public void write(File to) throws IOException {
      Properties props = new Properties();
      setFields(props, this);
      RandomAccessFile file = new RandomAccessFile(to, "rws");
      FileOutputStream out = null;
      try {
        file.seek(0);
        out = new FileOutputStream(file.getFD());
        /*
         * If server is interrupted before this line, 
         * the version file will remain unchanged.
         */
        props.store(out, null);
        /*
         * Now the new fields are flushed to the head of the file, but file 
         * length can still be larger then required and therefore the file can 
         * contain whole or corrupted fields from its old contents in the end.
         * If server is interrupted here and restarted later these extra fields
         * either should not effect server behavior or should be handled
         * by the server correctly.
         */
        file.setLength(out.getChannel().position());
      } finally {
        if (out != null) {
          out.close();
        }
        file.close();
      }
    }

    /**
     * Clear and re-create storage directory.
     * <p>
     * Removes contents of the current directory and creates an empty directory.
     * 
     * This does not fully format storage directory. 
     * It cannot write the version file since it should be written last after  
     * all other storage type dependent files are written.
     * Derived storage is responsible for setting specific storage values and
     * writing the version file to disk.
     * 
     * @throws IOException
     */
    public void clearDirectory() throws IOException {
      File curDir = this.getCurrentDir();
      if (curDir.exists())
        if (!(FileUtil.fullyDelete(curDir)))
          throw new IOException("Cannot remove current directory: " + curDir);
      if (!curDir.mkdirs())
        throw new IOException("Cannot create directory " + curDir);
    }

    public File getCurrentDir() {
      return new File(root, STORAGE_DIR_CURRENT);
    }
    public File getVersionFile() {
      return new File(new File(root, STORAGE_DIR_CURRENT), STORAGE_FILE_VERSION);
    }
    public File getPreviousVersionFile() {
      return new File(new File(root, STORAGE_DIR_PREVIOUS), STORAGE_FILE_VERSION);
    }
    public File getPreviousDir() {
      return new File(root, STORAGE_DIR_PREVIOUS);
    }
    public File getPreviousTmp() {
      return new File(root, STORAGE_TMP_PREVIOUS);
    }
    public File getRemovedTmp() {
      return new File(root, STORAGE_TMP_REMOVED);
    }
    public File getFinalizedTmp() {
      return new File(root, STORAGE_TMP_FINALIZED);
    }
    public File getLastCheckpointTmp() {
      return new File(root, STORAGE_TMP_LAST_CKPT);
    }
    public File getPreviousCheckpoint() {
      return new File(root, STORAGE_PREVIOUS_CKPT);
    }

    /**
     * Check consistency of the storage directory
     * 
     * @param startOpt a startup option.
     *  
     * @return state {@link StorageState} of the storage directory 
     * @throws {@link InconsistentFSStateException} if directory state is not 
     * consistent and cannot be recovered 
     */
    public StorageState analyzeStorage(StartupOption startOpt) throws IOException {
      assert root != null : "root is null";
      String rootPath = root.getCanonicalPath();
      try { // check that storage exists
        if (!root.exists()) {
          // storage directory does not exist
          if (startOpt != StartupOption.FORMAT) {
            LOG.info("Storage directory " + rootPath + " does not exist.");
            return StorageState.NON_EXISTENT;
          }
          LOG.info(rootPath + " does not exist. Creating ...");
          if (!root.mkdirs())
            throw new IOException("Cannot create directory " + rootPath);
        }
        // or is inaccessible
        if (!root.isDirectory()) {
          LOG.info(rootPath + "is not a directory.");
          return StorageState.NON_EXISTENT;
        }
        if (!root.canWrite()) {
          LOG.info("Cannot access storage directory " + rootPath);
          return StorageState.NON_EXISTENT;
        }
      } catch(SecurityException ex) {
        LOG.info("Cannot access storage directory " + rootPath, ex);
        return StorageState.NON_EXISTENT;
      }

      this.lock(); // lock storage if it exists

      if (startOpt == HdfsConstants.StartupOption.FORMAT)
        return StorageState.NOT_FORMATTED;
      if (startOpt != HdfsConstants.StartupOption.IMPORT) {
        //make sure no conversion is required
        checkConversionNeeded(this);
      }

      // check whether current directory is valid
      File versionFile = getVersionFile();
      boolean hasCurrent = versionFile.exists();

      // check which directories exist
      boolean hasPrevious = getPreviousDir().exists();
      boolean hasPreviousTmp = getPreviousTmp().exists();
      boolean hasRemovedTmp = getRemovedTmp().exists();
      boolean hasFinalizedTmp = getFinalizedTmp().exists();
      boolean hasCheckpointTmp = getLastCheckpointTmp().exists();

      if (!(hasPreviousTmp || hasRemovedTmp
          || hasFinalizedTmp || hasCheckpointTmp)) {
        // no temp dirs - no recovery
        if (hasCurrent)
          return StorageState.NORMAL;
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                              "version file in current directory is missing.");
        return StorageState.NOT_FORMATTED;
      }

      if ((hasPreviousTmp?1:0) + (hasRemovedTmp?1:0)
          + (hasFinalizedTmp?1:0) + (hasCheckpointTmp?1:0) > 1)
        // more than one temp dirs
        throw new InconsistentFSStateException(root,
                                               "too many temporary directories.");

      // # of temp dirs == 1 should either recover or complete a transition
      if (hasCheckpointTmp) {
        return hasCurrent ? StorageState.COMPLETE_CHECKPOINT
                          : StorageState.RECOVER_CHECKPOINT;
      }

      if (hasFinalizedTmp) {
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_FINALIZED
                                                 + "cannot exist together.");
        return StorageState.COMPLETE_FINALIZE;
      }

      if (hasPreviousTmp) {
        if (hasPrevious)
          throw new InconsistentFSStateException(root,
                                                 STORAGE_DIR_PREVIOUS + " and " + STORAGE_TMP_PREVIOUS
                                                 + " cannot exist together.");
        if (hasCurrent)
          return StorageState.COMPLETE_UPGRADE;
        return StorageState.RECOVER_UPGRADE;
      }
      
      assert hasRemovedTmp : "hasRemovedTmp must be true";
      if (!(hasCurrent ^ hasPrevious))
        throw new InconsistentFSStateException(root,
                                               "one and only one directory " + STORAGE_DIR_CURRENT 
                                               + " or " + STORAGE_DIR_PREVIOUS 
                                               + " must be present when " + STORAGE_TMP_REMOVED
                                               + " exists.");
      if (hasCurrent)
        return StorageState.COMPLETE_ROLLBACK;
      return StorageState.RECOVER_ROLLBACK;
    }

    /**
     * Complete or recover storage state from previously failed transition.
     * 
     * @param curState specifies what/how the state should be recovered
     * @throws IOException
     */
    public void doRecover(StorageState curState) throws IOException {
      File curDir = getCurrentDir();
      String rootPath = root.getCanonicalPath();
      switch(curState) {
      case COMPLETE_UPGRADE:  // mv previous.tmp -> previous
        LOG.info("Completing previous upgrade for storage directory " 
                 + rootPath + ".");
        rename(getPreviousTmp(), getPreviousDir());
        return;
      case RECOVER_UPGRADE:   // mv previous.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from previous upgrade.");
        if (curDir.exists())
          deleteDir(curDir);
        rename(getPreviousTmp(), curDir);
        return;
      case COMPLETE_ROLLBACK: // rm removed.tmp
        LOG.info("Completing previous rollback for storage directory "
                 + rootPath + ".");
        deleteDir(getRemovedTmp());
        return;
      case RECOVER_ROLLBACK:  // mv removed.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from previous rollback.");
        rename(getRemovedTmp(), curDir);
        return;
      case COMPLETE_FINALIZE: // rm finalized.tmp
        LOG.info("Completing previous finalize for storage directory "
                 + rootPath + ".");
        deleteDir(getFinalizedTmp());
        return;
      case COMPLETE_CHECKPOINT: // mv lastcheckpoint.tmp -> previous.checkpoint
        LOG.info("Completing previous checkpoint for storage directory " 
                 + rootPath + ".");
        File prevCkptDir = getPreviousCheckpoint();
        if (prevCkptDir.exists())
          deleteDir(prevCkptDir);
        rename(getLastCheckpointTmp(), prevCkptDir);
        return;
      case RECOVER_CHECKPOINT:  // mv lastcheckpoint.tmp -> current
        LOG.info("Recovering storage directory " + rootPath
                 + " from failed checkpoint.");
        if (curDir.exists())
          deleteDir(curDir);
        rename(getLastCheckpointTmp(), curDir);
        return;
      default:
        throw new IOException("Unexpected FS state: " + curState);
      }
    }

    /**
     * Lock storage to provide exclusive access.
     * 
     * <p> Locking is not supported by all file systems.
     * E.g., NFS does not consistently support exclusive locks.
     * 
     * <p> If locking is supported we guarantee exculsive access to the
     * storage directory. Otherwise, no guarantee is given.
     * 
     * @throws IOException if locking fails
     */
    public void lock() throws IOException {
      this.lock = tryLock();
      if (lock == null) {
        String msg = "Cannot lock storage " + this.root 
          + ". The directory is already locked.";
        LOG.info(msg);
        throw new IOException(msg);
      }
    }

    /**
     * Attempts to acquire an exclusive lock on the storage.
     * 
     * @return A lock object representing the newly-acquired lock or
     * <code>null</code> if storage is already locked.
     * @throws IOException if locking fails.
     */
    FileLock tryLock() throws IOException {
      File lockF = new File(root, STORAGE_FILE_LOCK);
      lockF.deleteOnExit();
      RandomAccessFile file = new RandomAccessFile(lockF, "rws");
      FileLock res = null;
      try {
        res = file.getChannel().tryLock();
      } catch(OverlappingFileLockException oe) {
        file.close();
        return null;
      } catch(IOException e) {
        LOG.info(StringUtils.stringifyException(e));
        file.close();
        throw e;
      }
      return res;
    }

    /**
     * Unlock storage.
     * 
     * @throws IOException
     */
    public void unlock() throws IOException {
      if (this.lock == null)
        return;
      this.lock.release();
      lock.channel().close();
      lock = null;
    }
  }