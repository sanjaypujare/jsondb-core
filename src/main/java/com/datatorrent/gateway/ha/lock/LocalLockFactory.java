/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * Created by mattzhang on 1/16/17.
 *
 * @since 3.8.0
 */
public class LocalLockFactory extends LockFactory
{
  private Striped<ReadWriteLock> readWriteLockStriped;

  public LocalLockFactory(Object... params)
  {
    int stripes = (Integer)params[0];
    readWriteLockStriped = Striped.readWriteLock(stripes);
  }

  @Override
  public ReadWriteLock getLock(Object... params)
  {
    String path = String.valueOf(params[0]);
    return readWriteLockStriped.get(path);
  }

}
