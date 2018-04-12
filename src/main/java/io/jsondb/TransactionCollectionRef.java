/**
 *  Copyright (c) 2012-2018 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package io.jsondb;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates copy of in-memory data during the lifetime of a transaction
 *
 */
public class TransactionCollectionRef extends CollectionRef
{
  private Logger logger = LoggerFactory.getLogger(TransactionCollectionRef.class);
  private CollectionRef baseCollectionRef;
  private HashMap<String, Integer> hashValues = new HashMap<String, Integer>();
  
  /**
   * In a transaction at any point only one collection can be "in-update" status 
   * i.e. between the time an update is initiated and it is completed or aborted
   */
  private String currentCollectionInUpdate;
  
  public TransactionCollectionRef(CollectionRef baseCollectionRef)
  {
    this.baseCollectionRef = baseCollectionRef;
  }
  
  @Override
  public boolean isTransactionMode()
  {
    return true;
  }

  /**
   * Get a collection that will be modified (possibly as part of a transaction)
   * 
   * @param collectionName
   * @return
   * @throws UnknownCollectionException
   */
  @Override
  public synchronized Map<String, JSONObject> getCollectionToModify(String collectionName) throws UnknownCollectionException
  {
    // transaction implementation: check if we already have a transaction-copy
    Map<String, JSONObject> collection = super.get(collectionName);
    if (collection != null) {
      // we have a transaction-copy: make sure it is also not currentCollectionInUpdate
      if (collectionName.equals(currentCollectionInUpdate)) {
        throw new IllegalStateException("Collection " + collectionName + " is in illegal state.");
      }
      return collection;
    }
    // make a transaction-copy from baseCollectionRef
    collection = baseCollectionRef.get(collectionName);
    Map<String, JSONObject> copy = new LinkedHashMap<String, JSONObject>(collection);
    currentCollectionInUpdate = collectionName;
    hashValues.put(collectionName, collection.hashCode());
    super.put(collectionName, copy);
    return copy;
  }

  /**
   * Get a collection that will NOT be modified: so we should find a collection from base collection if necessary
   * 
   * @param collectionName
   * @return
   * @throws UnknownCollectionException
   */
  @Override
  public Map<String, JSONObject> getCollectionToSearch(String collectionName)
  {
    // transaction implementation: check if we already have a transaction-copy
    Map<String, JSONObject> collection = super.get(collectionName);
    if (collection != null) {
      // we have a transaction-copy: make sure it is also not currentCollectionInUpdate
      if (collectionName.equals(currentCollectionInUpdate)) {
        throw new IllegalStateException("Collection " + collectionName + " is in illegal state.");
      }
      return collection;
    }
    // just return from baseCollectionRef if found
    return baseCollectionRef.getCollectionToSearch(collectionName);
  }
  
  /**
   * This is called at the end of a single collection update (insert/update) to record
   * successful completion: this way we know a collection has been modified so a transaction specific
   * copy needs to be retained.
   * 
   */
  @Override
  public synchronized void completeUpdate()
  {
    currentCollectionInUpdate = null;
  }

  /**
   * typically invoked from the finally clause inside an insert/update which can signify a
   * potentially unsuccessful update to a collection. It can be used to remove a transaction
   * specific copy if it was created in this update
   * 
   */
  @Override
  public synchronized void makeFinal()
  {
    //  which means the update didn't go thru
    // and we need to remove the transaction-copy
    if (currentCollectionInUpdate != null) {
      // this was called without a call to completeUpdate() e.g. there was an exception or such
      logger.warn("removing transaction-copy of " + currentCollectionInUpdate);
      // remove the transaction-copy
      super.remove(currentCollectionInUpdate);
      currentCollectionInUpdate = null;
    }
  }
  
  public Set<String> commitCollections()
  {
    // transaction-copy to be copied to global-collection and then saved to disk
    HashSet<String> collectionSet = new HashSet<String>();
    for (Entry<String, Map<String, JSONObject>> entry : collectionMap.get().entrySet()) {
      String curName = entry.getKey();
      // verify hash value
      int origHash = hashValues.get(curName);
      Map<String, JSONObject> baseMap = baseCollectionRef.get(curName);
      if (origHash != baseMap.hashCode()) {
        throw new ConcurrentModificationException("Collection " + curName + " modified outside of transaction!");
      }
      // copy our transaction-copy to base collection
      baseCollectionRef.put(curName, entry.getValue());
      collectionSet.add(curName);
    }
    return collectionSet;
  }
  
}
