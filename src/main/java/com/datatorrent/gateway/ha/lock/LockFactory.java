/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

import java.util.concurrent.locks.ReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * LockFactory class.</p>
 *
 * @author Matt Zhang <matt@datatorrent.com>
 * @since 3.7.0
 */
public abstract class LockFactory
{
  private static final Logger LOG = LoggerFactory.getLogger(LockFactory.class);
  public enum lockType
  {
    LOCAL, GLOBAL, GLOBAL_ONLY;
  }

  public LockFactory()
  {
  }

  public static LockFactory create(lockType type, Object... params)
  {
    switch (type) {
      case LOCAL:
        // params is (int stripe)
        return new LocalLockFactory(params);

      case GLOBAL_ONLY:
        // params is (FileSystem fs)
        return new HDFSLockFactory(params);

      case GLOBAL:
        // params are (int stripe, FileSystem fs)
        return new SerialLockFactory(params);

      default:
        LOG.error("Unrecognized lock type: " + type.toString());
        throw new IllegalArgumentException("Unrecognized lock type" + type.toString());
    }
  }

  public abstract ReadWriteLock getLock(Object... params);
}
