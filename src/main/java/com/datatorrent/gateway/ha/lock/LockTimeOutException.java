/**
 *  Copyright (c) 2012-2017 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.gateway.ha.lock;

/**
 * Created by mattzhang on 1/17/17.
 *
 * @since 3.8.0
 */
public class LockTimeOutException extends RuntimeException
{
  public LockTimeOutException()
  {
    super();
  }

  public LockTimeOutException(String s)
  {
    super(s);
  }
}
