/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;

/**
 * <p>
 * HDFSFileLockUtil class.</p>
 *
 * @author Matt Zhang <matt@datatorrent.com>
 * @since 3.7.0
 */
public class HDFSFileLockUtil implements Lock
{
  private static final long DEFAULT_LOCK_TIMEOUT = 60 * 1000;
  private static final long DEFAULT_LOCK_RETRY_INTERVAL = 500;

  private static final Logger LOG = LoggerFactory.getLogger(HDFSFileLockUtil.class);
  private static final String DEFAULT_FILE = "lockfile";
  private String lockFilePathStr = DEFAULT_FILE;
  private org.apache.hadoop.fs.Path lockFilePath = new org.apache.hadoop.fs.Path(lockFilePathStr);
  private FileSystem fs;
  private long lockTimeOut = DEFAULT_LOCK_TIMEOUT;
  private long lockRetryInterval = DEFAULT_LOCK_RETRY_INTERVAL;
  private long lockTime;

  /**
   * Get the path of the locking file
   * @return lockFilePath
   */
  public org.apache.hadoop.fs.Path getPath()
  {
    return lockFilePath;
  }

  /**
   * Set the path of the locking file
   * @param path
   */
  public void setPath(org.apache.hadoop.fs.Path path)
  {
    this.lockFilePath = new org.apache.hadoop.fs.Path(path, lockFilePathStr);
  }

  public void setPath(String path)
  {
    this.lockFilePath = new org.apache.hadoop.fs.Path(path, lockFilePathStr);
  }

  /**
   * get the actual lock time of the lock file from hdfs directly
   */
  public long getLockTime()
  {
    try {
      return fs.getFileStatus(lockFilePath).getModificationTime();
    } catch (java.io.IOException e) {
      return 0;
    }
  }

  /**
   * get the lock timeout of the lock file
   * @return lockTimeOut
   */
  public long getLockTimeOut()
  {
    return this.lockTimeOut;
  }

  /**
   * set the lockTimeOut
   * @param timeOut
   */
  public void setLockTimeOut(long timeOut)
  {
    this.lockTimeOut = timeOut;
  }

  public void setLockRetryInterval(long interval)
  {
    this.lockRetryInterval = interval;
  }

  public long getLockRetryInterval()
  {
    return this.lockRetryInterval;
  }


  /**
   * constructors
   * @param fs
   */
  public HDFSFileLockUtil(FileSystem fs)
  {
    this.fs = fs;
  }

  public HDFSFileLockUtil(FileSystem fs, org.apache.hadoop.fs.Path path)
  {
    this.fs = fs;
    setPath(path);
  }

  public HDFSFileLockUtil(FileSystem fs, String path)
  {
    this.fs = fs;
    setPath(path);
  }

  /**
   * Check if the lock file already exists
   * If the exiting lock file is too old, release it and return false;
   *
   * @return true if exists, false if not exists
   */
  public boolean isLocked()
  {
    try {
      if (fs.exists(lockFilePath)) {
        long fileLockTime = this.getLockTime();
        if (System.currentTimeMillis() - fileLockTime > getLockTimeOut()) {
          this.unlock();
          return false;
        } else {
          return true;
        }
      } else {
        return false;
      }
    } catch (IOException ex) {
      LOG.warn("Error checking lock file: {}", lockFilePath, ex);
      return true;
    }
  }

  public boolean isLockTimeOut()
  {
    return (lockTime == getLockTime()) ? false : true;
  }


  /**
   * Lock in blocking mode.
   * If creation of lock file failed, sleep for a retry interval and then try again
   */
  @Override
  public void lock()
  {
    for (;;) {
      try {
        if (!tryLock()) {
          Thread.sleep(getLockRetryInterval());
        } else {
          break;
        }
      } catch (IllegalStateException e) {
        LOG.warn("Obtaining lock failed with exception.", e);
        throw e;
      } catch (Exception e) {
        LOG.warn("Obtaining lock failed with exception.");
      }
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException
  {
    for (;;) {
      if (!tryLock()) {
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
        Thread.sleep(getLockRetryInterval());
      } else {
        break;
      }
    }
  }

  /**
   * Create the lock file, and setup lockTime
   * @return true on success, false on failure
   */
  @Override
  public boolean tryLock()
  {
    if (isLocked()) {
      return false;
    } else {
      try {
        if (fs.createNewFile(lockFilePath)) {
          lockTime = getLockTime();
          return true;
        } else {
          return false;
        }
      } catch (IOException ex) {
        LOG.warn("Error creating lock file: {}", lockFilePath, ex);
        throw new IllegalStateException(ex);
      }
    }
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
  {
    long nanoTimeout = unit.toNanos(time);
    long lastTime = System.nanoTime();
    for (;;) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }

      if (nanoTimeout < 0) {
        return false;
      }

      if (tryLock()) {
        return true;
      }

      long now = System.nanoTime();
      nanoTimeout -= now - lastTime;
      lastTime = now;
      Thread.sleep(getLockRetryInterval());
    }
  }

  /**
   * Delete the lock file
   */
  @Override
  public void unlock()
  {
    if (isLockTimeOut()) {
      throw new LockTimeOutException("Lock time out and acquired by others");
    }

    try {
      fs.delete(lockFilePath, false);
    } catch (IOException ex) {
      LOG.warn("Error deleting lock file: {}", lockFilePath, ex);
    }
  }

  public Condition newCondition()
  {
    throw new UnsupportedOperationException();
  }
}
