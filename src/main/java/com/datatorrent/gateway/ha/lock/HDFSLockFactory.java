/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.fs.FileSystem;

/**
 * Created by mattzhang on 1/16/17.
 *
 * @since 3.8.0
 */
public class HDFSLockFactory extends LockFactory
{
  private FileSystem fs;

  public HDFSLockFactory(Object... params)
  {
    fs = (FileSystem)params[0];
  }

  @Override
  public ReadWriteLock getLock(Object... params)
  {
    String path = (String)params[0];
    HDFSReadWriteLock hdfsReadWriteLock = new HDFSReadWriteLock(fs);
    hdfsReadWriteLock.setHdfsPath(path);
    return hdfsReadWriteLock;
  }
}
