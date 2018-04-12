/**
 *  Copyright (c) 2012-2018 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package io.jsondb;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.jettison.json.JSONObject;

/**
 * This encapsulates a collection of collections with semantics 
 * for transaction vs non-transaction mode.
 *
 */
public class CollectionRef
{
  protected AtomicReference<Map<String, Map<String, JSONObject>>> collectionMap = new AtomicReference<Map<String, Map<String, JSONObject>>>(new ConcurrentHashMap<String, Map<String, JSONObject>>());
  
  public boolean contains(String collectionName)
  {
    return collectionMap.get().containsKey(collectionName);
  }
  
  public Map<String, JSONObject> remove(String collectionName)
  {
    return collectionMap.get().remove(collectionName);
  }

  public void put(String collectionName, Map<String, JSONObject> collection)
  {
    collectionMap.get().put(collectionName, collection);
  }
  
  public Map<String, JSONObject> get(String collectionName)
  {
    return collectionMap.get().get(collectionName);
  }

  public Set<String> getCollectionNames() {
    return collectionMap.get().keySet();
  }
  
  /**
   * Get a collection that will be modified (possibly as part of a transaction)
   * 
   * @param collectionName
   * @return
   * @throws UnknownCollectionException
   */
  public Map<String, JSONObject> getCollectionToModify(String collectionName) throws UnknownCollectionException
  {
    // since this is non-transaction implementation just return the collection
    Map<String, JSONObject> collection = get(collectionName);
    Util.ifNullThrowUnknownCollectionException(collectionName, collection);
    return collection;
  }

  public Map<String, JSONObject> getCollectionToSearch(String collectionName)
  {
    // since this is non-transaction implementation just return the collection
    Map<String, JSONObject> collection = get(collectionName);
    Util.ifNullThrowUnknownCollectionException(collectionName, collection);
    return collection;
  }
  
  public boolean isTransactionMode()
  {
    return false;
  }

  /**
   * This is called at the end of a single collection update (insert/update) to record
   * successful completion: this way we know a collection has been modified so a transaction specific
   * copy needs to be retained.
   * 
   */
  public void completeUpdate()
  {
    // no-op for non-transaction mode
  }

  /**
   * typically invoked from the finally clause inside an insert/update which can signify a
   * potentially unsuccessful update to a collection. It can be used to remove a transaction
   * specific copy if it was created in this update
   * 
   */
  public void makeFinal()
  {
    // no op for non-transaction mode
  }

}
