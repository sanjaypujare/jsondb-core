/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by sandesh on 9/23/16.
 *
 * @since 3.7.0
 */

// NOTE:
// 1. Locks are pre-allocated and not garbage collected till the end.
// 2. Interface & Implementation is stripped down version of Guava Stripped library
//     a. Remove this file when we update the Guava library containing Stripped class.
public abstract class Striped<L>
{
  private Striped()
  {
  }

  public abstract L get(Object var1);

  public static Striped<ReadWriteLock> readWriteLock(int stripes)
  {
    return new CompactStriped(stripes);
  }

  private static class CompactStriped extends Striped<ReadWriteLock>
  {
    private final ReadWriteLock[] array;

    private CompactStriped(int stripes)
    {
      array = new ReadWriteLock[stripes];

      for (int i = 0; i < stripes; ++i) {
        array[i] = new ReentrantReadWriteLock();
      }
    }

    public int size()
    {
      return this.array.length;
    }

    @Override
    public ReadWriteLock get(Object var1)
    {
      return array[Math.abs(var1.hashCode()) % array.length];
    }
  }
}
