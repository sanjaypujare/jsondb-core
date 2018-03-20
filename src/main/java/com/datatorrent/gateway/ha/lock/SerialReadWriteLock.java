/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.hadoop.fs.FileSystem;

/**
 * Created by mattzhang on 1/12/17.
 *
 * @since 3.8.0
 */
public class SerialReadWriteLock implements ReadWriteLock
{
  private LockImpl serialLock;
  private boolean isReadLock;

  public SerialReadWriteLock(int stripes, FileSystem fs)
  {
    this.serialLock = new LockImpl(stripes, fs);
  }

  public void setup(String stripePath, String hdfsPath)
  {
    serialLock.setup(stripePath, hdfsPath);
  }

  @Override
  public Lock readLock()
  {
    isReadLock = true;
    return this.serialLock;
  }

  @Override
  public Lock writeLock()
  {
    isReadLock = false;
    return this.serialLock;
  }

  public boolean isHdfsLockTimeOut()
  {
    return serialLock.isHdfsLockTimeOut();
  }

  private class LockImpl implements Lock
  {
    ArrayList<ReadWriteLock> lockList;
    LocalLockFactory localLock;
    HDFSLockFactory hdfsLock;

    // setup the lockList
    public LockImpl(int stripes, FileSystem fs)
    {
      lockList = new ArrayList<ReadWriteLock>();
      localLock = new LocalLockFactory(stripes);
      hdfsLock = new HDFSLockFactory(fs);
    }

    public void setup(String stripePath, String hdfsPath)
    {
      ReadWriteLock readWriteLock = localLock.getLock(stripePath);
      lockList.add(readWriteLock);

      ReadWriteLock hdfsReadWriteLock = hdfsLock.getLock(hdfsPath);
      lockList.add(hdfsReadWriteLock);
    }

    public boolean isHdfsLockTimeOut()
    {
      HDFSReadWriteLock currentLock = (HDFSReadWriteLock)lockList.get(1);
      return currentLock.isLockTimeOut();
    }

    @Override
    public void lock()
    {
      ReadWriteLock currentLock;
      for (int i = 0; i < lockList.size(); i++) {
        if (isReadLock) {
          currentLock = lockList.get(i);
          currentLock.readLock().lock();
        } else {
          currentLock = lockList.get(i);
          currentLock.writeLock().lock();
        }
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException
    {
      ReadWriteLock currentLock;
      for (int i = 0; i < lockList.size(); i++) {
        if (isReadLock) {
          currentLock = lockList.get(i);
          currentLock.readLock().lockInterruptibly();
        } else {
          currentLock = lockList.get(i);
          currentLock.writeLock().lockInterruptibly();
        }
      }

    }

    @Override
    public boolean tryLock()
    {
      ReadWriteLock currentLock;
      boolean success = true;
      for (int i = 0; i < lockList.size(); i++) {
        if (isReadLock) {
          currentLock = lockList.get(i);
          success = success && currentLock.readLock().tryLock();
        } else {
          currentLock = lockList.get(i);
          success = success && currentLock.writeLock().tryLock();
        }
      }
      if (success == false) {
        unlock();
      }
      return success;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
    {
      ReadWriteLock currentLock;
      boolean success = true;
      for (int i = 0; i < lockList.size(); i++) {
        if (isReadLock) {
          currentLock = lockList.get(i);
          success = success && currentLock.readLock().tryLock(time, unit);
        } else {
          currentLock = lockList.get(i);
          success = success && currentLock.writeLock().tryLock(time, unit);
        }
      }
      if (success == false) {
        unlock();
      }
      return success;
    }

    @Override
    public void unlock()
    {
      ReadWriteLock currentLock;
      for (int i = 0; i < lockList.size(); i++) {
        if (isReadLock) {
          currentLock = lockList.get(i);
          currentLock.readLock().unlock();
        } else {
          currentLock = lockList.get(i);
          currentLock.writeLock().unlock();
        }
      }
    }

    @Override
    public Condition newCondition()
    {
      throw new UnsupportedOperationException();
    }
  }
}
