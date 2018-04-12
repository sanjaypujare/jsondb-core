/**
 * 
 */
package io.jsondb;

/**
 * This exception is thrown when an unknown collection name is used in a registry call
 *
 */
public class UnknownCollectionException extends IllegalArgumentException
{

  /**
   * 
   */
  public UnknownCollectionException()
  {

  }

  /**
   * @param s
   */
  public UnknownCollectionException(String s)
  {
    super(s);
  }

  /**
   * @param cause
   */
  public UnknownCollectionException(Throwable cause)
  {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public UnknownCollectionException(String message, Throwable cause)
  {
    super(message, cause);
  }

}
