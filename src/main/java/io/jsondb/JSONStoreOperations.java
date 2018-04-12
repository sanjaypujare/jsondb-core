/**
 *  Copyright (c) 2012-2018 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package io.jsondb;

import java.util.List;
import java.util.Set;

import org.codehaus.jettison.json.JSONObject;

import io.jsondb.events.CollectionFileChangeListener;


/**
 * Interface that defines the methods available in JsonDBTemplate
 *
 * @author Farooq Khan
 * @version 1.0 21 Aug 2016
 */
public interface JSONStoreOperations {

  /**
   * Re-load the collections from dblocation folder.
   * This functionality is useful if you some other process is going to directly update
   * the collection files in dblocation
   */
  void reLoadDB();

  /**
   * Reloads a particular collection from dblocation directory
   * @param collectionName name of the collection to reload
   */
  void reloadCollection(String collectionName);

  /**
   * adds a CollectionFileChangeListener to db.
   *
   * NOTE: This method uses FileWatchers and on MAC we get now events for file changes so this does not work on Mac
   * @param listener actual listener to add
   */
  void addCollectionFileChangeListener(CollectionFileChangeListener listener);

  /**
   * removes a previously added CollectionFileChangeListener
   * @param listener actual listener to remove
   */
  void removeCollectionFileChangeListener(CollectionFileChangeListener listener);

  /**
   * a method to check if there are any registered CollectionFileChangeListener
   * @return true of there are any registered CollectionFileChangeListeners
   */
  boolean hasCollectionFileChangeListener();

  /**
   * Create an uncapped collection with the provided name.
   *
   * @param collectionName name of the collection
   */
  <T> void createCollection(String collectionName);

  /**
   * Drop the collection with the given name.
   *
   * @param collectionName name of the collection to drop/delete.
   */
  void dropCollection(String collectionName);

  /**
   * A set of collection names.
   *
   * @return list of collection names
   */
  Set<String> getCollectionNames();


  /**
   * Check to see if a collection with a given name exists.
   *
   * @param collectionName name of the collection
   * @return true if a collection with the given name is found, false otherwise.
   */
  boolean collectionExists(String collectionName);

  /**
   * is a collection readonly,
   * A collection can be readonly if its schema version does not match the actualSchema version
   *
   * @param entityClass class that determines the collection
   * @return true if collection is readonly
   */
  boolean isCollectionReadonly(String collectionName);

  /**
   * Query for a list of objects of type T from the specified collection.
   *
   * @param collectionName name of the collection to retrieve the objects from
   * @param collectionRef TODO
   * @return the found collection
   */
  List<JSONObject> findAll(String collectionName, CollectionRef collectionRef);


  /**
   * Returns the document with the given id from the given collection mapped onto the given target class.
   *
   * @param id the id of the document to return
   * @param collectionName the collection to query for the document
   * @param collectionRef TODO
   * @return object searched within the collection
   */
  JSONObject findById(String id, String collectionName, CollectionRef collectionRef);


  /**
   * Map the results of the jxQuery on the collection for the entity type to a single
   * instance of an object of the specified type. The first document that matches the query
   * is returned and also removed from the collection in the database.
   *
   * Both the find and remove operation is done atomically
   *
   * @param jxQuery  JxPath query string
   * @param collectionName  name of the collection to update the objects from
   * @param <T> Type annotated with {@link io.jsondb.annotation.Document} annotation
   *            and member of the baseScanPackage
   * @return the removed object or null
   * <T> T findAndRemove(String jxQuery, String collectionName);
   */


  /**
   * Returns and removes all documents matching the given query form the collection used to store the entityClass.
   *
   * Both the find and remove operation is done atomically
   *
   * @param jxQuery  JxPath query string
   * @param collectionName  name of the collection to update the objects from
   * @param <T> Type annotated with {@link io.jsondb.annotation.Document} annotation
   *            and member of the baseScanPackage
   * @return the list of removed objects or null
   *   <T> List<T> findAllAndRemove(String jxQuery, String collectionName);
   * 
   */


  /**
   * Triggers findAndModify to apply provided Update on the first document matching Criteria of given Query.
   *
   * Both the find and remove operation is done atomically
   *
   * @param jxQuery  JxPath query string
   * @param update  The Update operation to perform
   * @param entityClass  class that determines the collection to use
   * @param <T> Type annotated with {@link io.jsondb.annotation.Document} annotation
   *            and member of the baseScanPackage
   * @return first object that was modified or null
   *   <T> T findAndModify(String jxQuery, Update update, Class<T> entityClass);
   */


  /**
   * Triggers findAndModify to apply provided Update on the first document matching Criteria of given Query.
   *
   * Both the find and remove operation is done atomically
   *
   * @param jxQuery  JxPath query string
   * @param update  The Update operation to perform
   * @param collectionName  name of the collection to update the objects from
   * @param <T> Type annotated with {@link io.jsondb.annotation.Document} annotation
   *            and member of the baseScanPackage
   * @return first object that was modified or null

  <T> T findAndModify(String jxQuery, Update update, String collectionName);

  <T> List<T> findAllAndModify(String jxQuery, Update update, Class<T> entityClass);
  <T> List<T> findAllAndModify(String jxQuery, Update update, String collectionName);
   */

  /**
   * This method backs up JSONDB collections to specified backup path
   *
   * @param backupPath location at which to backup the database contents
   */
  void backup(String backupPath);

  /**
   * This method restores JSONDB collections from specified restore path.
   * if merge flag is set to true restore operation will merge collections from restore location
   * and if it is set to false it will replace existing collections with collections being
   * restored
   *
   * @param restorePath path were backup jsondb files are present
   * @param merge whether to merge data from restore location
   */
  void restore(String restorePath, boolean merge);

  /**
   * Update the object in the collection for the entity type of the object to save.
   * This will throw a exception if the object is not already present.
   * This is a not same as MongoDB behaviour
   *
   * @param objectToSave  the object to store in the collection
   * @param collectionName  name of the collection to store the object in
   * @param collectionRef TODO
   */
  JSONObject update(JSONObject objectToSave, String collectionName, CollectionRef collectionRef);

  /**
   * Remove the given object from the collection by id.
   * @param collectionName  name of the collection to remove the object from
   * @param collectionRef TODO
   * @param objectToRemove  the object to remove from the collection
   * @return  the object that was actually removed or null
   */
  JSONObject remove(String id, String collectionName, CollectionRef collectionRef);

  JSONObject insert(JSONObject objectToSave, String collectionName, CollectionRef collectionRef);

  List<JSONObject> search(String jxQuery, String collectionName, CollectionRef collectionRef);

  List<JSONObject> searchAndDelete(String jxQuery, String collectionName, CollectionRef collectionRef);

}
