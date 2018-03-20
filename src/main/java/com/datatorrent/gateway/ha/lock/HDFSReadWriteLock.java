/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by mattzhang on 1/13/17.
 *
 * @since 3.8.0
 */
public class HDFSReadWriteLock implements ReadWriteLock
{
  private HDFSFileLockUtil fileLock;

  public HDFSReadWriteLock(FileSystem fs)
  {
    if (fileLock == null) {
      this.fileLock = new HDFSFileLockUtil(fs);
    }
  }

  public void setHdfsPath(String path)
  {
    fileLock.setPath(path);
  }

  public boolean isLockTimeOut()
  {
    return fileLock.isLockTimeOut();
  }

  public void setHdfsPath(Path path)
  {
    fileLock.setPath(path);
  }

  @Override
  public HDFSFileLockUtil readLock()
  {
    return this.fileLock;
  }

  @Override
  public HDFSFileLockUtil writeLock()
  {
    return this.fileLock;
  }

}
