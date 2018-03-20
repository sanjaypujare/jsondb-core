/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.fs.FileSystem;

/**
 * Created by mattzhang on 1/17/17.
 *
 * @since 3.8.0
 */
public class SerialLockFactory extends LockFactory
{
  int stripe;
  FileSystem fs;

  public SerialLockFactory(Object... params)
  {
    stripe = (Integer)params[0];
    fs = (FileSystem)params[1];
  }

  @Override
  public ReadWriteLock getLock(Object... params)
  {
    String stripePath = String.valueOf(params[0]);
    String hdfsPath = String.valueOf(params[1]);
    SerialReadWriteLock serialReadWriteLock = new SerialReadWriteLock(stripe, fs);
    serialReadWriteLock.setup(stripePath, hdfsPath);
    return serialReadWriteLock;
  }
}
