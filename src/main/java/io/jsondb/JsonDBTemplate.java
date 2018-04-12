/*
 * Copyright (c) 2016 Farooq Khan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package io.jsondb;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.JXPathIntrospector;
import org.apache.commons.jxpath.JXPathInvalidSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.datatorrent.library.lock.LockFactory;

import io.jsondb.events.CollectionFileChangeListener;
import io.jsondb.events.EventListenerList;
import io.jsondb.io.JsonFileLockException;
import io.jsondb.io.JsonReader;
import io.jsondb.io.JsonWriter;

/**
 * @version 1.0 25-Sep-2016
 */
public class JsonDBTemplate implements JSONStoreOperations {
  private Logger logger = LoggerFactory.getLogger(JsonDBTemplate.class);

  private JsonDBConfig dbConfig = null;
  private Path lockFilesLocation;
  private EventListenerList eventListenerList;

  private Map<String, CollectionMetaData> cmdMap;
  private AtomicReference<Map<String, Path>> fileObjectsRef = new AtomicReference<Map<String, Path>>(new ConcurrentHashMap<String, Path>());
  private CollectionRef globalCollectionsRef;
  //private AtomicReference<Map<String, JXPathContext>> contextsRef = new AtomicReference<Map<String, JXPathContext>>(new ConcurrentHashMap<String, JXPathContext>());
  private ReadWriteLock globalTransactionLock;
  boolean haEnabled;

  static {
    JXPathIntrospector.registerDynamicClass(JSONObject.class, JSONObjectDynamicPropertyHandler.class);
  }
  
  public JsonDBTemplate(String dbFilesLocationString) {
    this(dbFilesLocationString, null);
  }

  public JsonDBTemplate(String dbFilesLocationString, Comparator<String> schemaComparator)
  {
    this(dbFilesLocationString, schemaComparator, new Configuration());
  }

  public JsonDBTemplate(String dbFilesLocationString, Comparator<String> schemaComparator, Configuration conf) {
    dbConfig = new JsonDBConfig(dbFilesLocationString, schemaComparator, conf);
    haEnabled = Boolean.parseBoolean(conf.get("dt.gateway.ha.enable"));
    globalCollectionsRef = new CollectionRef();
    initialize();
    eventListenerList = new EventListenerList(dbConfig, cmdMap);
  }

  private void initialize(){
    this.lockFilesLocation = new Path(dbConfig.getDbFilesLocation(), "lock");
    try {
      if(!dbConfig.getDbFileSystem().exists(lockFilesLocation)) {
        dbConfig.getDbFileSystem().mkdirs(lockFilesLocation);
      }
      if (!dbConfig.getDbFileSystem().exists(dbConfig.getDbFilesLocation())) {
        dbConfig.getDbFileSystem().mkdirs(dbConfig.getDbFilesPath());
      } else if (dbConfig.getDbFileSystem().isFile(dbConfig.getDbFilesLocation())) {
        throw new InvalidJsonDbApiUsageException("Specified DbFiles directory is actually a file cannot use it as a directory");
      }
      cmdMap = CollectionMetaData.builder(dbConfig);
      loadDB();
    } catch (IOException e) {
      logger.error("IOException {}", e);
      throw new InvalidJsonDbApiUsageException("IOException " + dbConfig.getDbFilesLocationString());
    }
    Path lockFile = new Path(lockFilesLocation, "globalTransaction.lock");
    if (haEnabled) {
      globalTransactionLock = LockFactory.create(LockFactory.lockType.GLOBAL, 32, dbConfig.getDbFileSystem()).getLock(lockFile.toString(), lockFile.toString());
    } else {
      globalTransactionLock = LockFactory.create(LockFactory.lockType.LOCAL, 32).getLock(lockFile.toString());
    }
    // Auto-cleanup at shutdown
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        eventListenerList.shutdown();
      }
    });
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#reLoadDB()
   */
  @Override
  public void reLoadDB() {
    try {
      cmdMap = CollectionMetaData.builder(dbConfig);
      loadDB();
    } catch (IOException e) {
      logger.error("reLoadDB failed", e);
    }
  }

  private synchronized void loadDB() throws IOException {
    for(String collectionName : cmdMap.keySet()) {
      Path collectionFile = new Path(dbConfig.getDbFilesLocation(), collectionName + ".json");
      if(dbConfig.getDbFileSystem().exists(collectionFile)) {
        reloadCollection(collectionName);
      } else if (globalCollectionsRef.contains(collectionName)){
        //this probably is a reload attempt after a collection .json was deleted.
        //that is the reason even though the file does not exist a entry into collectionsRef still exists.
        //contextsRef.get().remove(collectionName);
        globalCollectionsRef.remove(collectionName);
      }
    }
  }

  /**
   * @return the globalTransactionLock
   */
  public ReadWriteLock getGlobalTransactionLock()
  {
    return globalTransactionLock;
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#reloadCollection(java.lang.String)
   */
  @Override
  public void reloadCollection(String collectionName) {
    CollectionMetaData cmd = cmdMap.get(collectionName);
    cmd.getCollectionLock().writeLock().lock();
    try {
      Path collectionFile = fileObjectsRef.get().get(collectionName);
      if(null == collectionFile) {
        // Lets create a file now
        collectionFile = new Path(dbConfig.getDbFilesLocation(), collectionName + ".json");
        try {
          if(!dbConfig.getDbFileSystem().exists(collectionFile)) {
            throw new InvalidJsonDbApiUsageException("Collection by name '" + collectionName + "' cannot be found at " + collectionFile);
          }
        } catch (IOException e) {
          throw new JsonDBException("Exception in accessing Collection by name '" + collectionName + "' at " + collectionFile, e);
        }
        Map<String, Path> fileObjectMap = fileObjectsRef.get();
        Map<String, Path> newFileObjectmap = new ConcurrentHashMap<String, Path>(fileObjectMap);
        newFileObjectmap.put(collectionName, collectionFile);
        fileObjectsRef.set(newFileObjectmap);
      }
      if (null != cmd && null != collectionFile) {
        Map<String, JSONObject> collection = loadCollection(collectionFile, collectionName, cmd);
        if (null != collection) {
          //JXPathContext newContext = JXPathContext.newContext(collection.values());
          //contextsRef.get().put(collectionName, newContext);
          globalCollectionsRef.put(collectionName, collection);
        } else {
          //Since this is a reload attempt its possible the .json files have disappeared in the interim a very rare thing
          //contextsRef.get().remove(collectionName);
          globalCollectionsRef.remove(collectionName);
        }
      }
    } finally {
      cmd.getCollectionLock().writeLock().unlock();
    }
  }

  private Map<String, JSONObject> loadCollection(Path collectionFile, String collectionName, CollectionMetaData cmd) {
    @SuppressWarnings("unchecked")

    JsonReader jr = null;
    Map<String, JSONObject> collection = new LinkedHashMap<String, JSONObject>();

    String line = null;
    int lineNo = 1;
    try {
      jr = new JsonReader(dbConfig, collectionFile);

      while ((line = jr.readLine()) != null) {
        if (lineNo == 1) {
          JSONObject verJson = new JSONObject(line);
          cmd.setActualSchemaVersion(verJson.getString("schemaVersion"));
        } else {
          JSONObject json = new JSONObject(line);
          String id = Util.getIdForEntity(json);
          collection.put(id, json);
        }
        lineNo++;
      }
    } catch (CharacterCodingException ce) {
      logger.error("Unsupported Character Encoding in file {} expected Encoding {}",
          collectionFile.getName(), dbConfig.getCharset().displayName(), ce);
      return null;
    } catch (JsonFileLockException jfe) {
      logger.error("Failed to acquire lock for collection file {}", collectionFile.getName(), jfe);
      return null;
    } catch (FileNotFoundException fe) {
      logger.error("Collection file {} not found", collectionFile.getName(), fe);
      return null;
    } catch (IOException e) {
      logger.error("Some IO Exception reading the Json File {}", collectionFile.getName(), e);
      return null;
    } catch(Throwable t) {
      logger.error("Throwable Caught ", collectionFile.getName(), t);
      return null;
    } finally {
      if (null != jr) {
        jr.close();
      }
    }
    return collection;
  }

  /* (non-Javadoc)
   * @see org.jsondb.JsonDBOperations#addCollectionFileChangeListener(org.jsondb.CollectionFileChangeListener)
   */
  @Override
  public void addCollectionFileChangeListener(CollectionFileChangeListener listener) {
    eventListenerList.addCollectionFileChangeListener(listener);
  }

  /* (non-Javadoc)
   * @see org.jsondb.JsonDBOperations#removeCollectionFileChangeListener(org.jsondb.CollectionFileChangeListener)
   */
  @Override
  public void removeCollectionFileChangeListener(CollectionFileChangeListener listener) {
    eventListenerList.removeCollectionFileChangeListener(listener);
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#hasCollectionFileChangeListener()
   */
  @Override
  public boolean hasCollectionFileChangeListener() {
    return eventListenerList.hasCollectionFileChangeListener();
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#createCollection(java.lang.String)
   */
  @Override
  public <T> void createCollection(String collectionName) {
    // TODO: use a global lock before checking this
    CollectionMetaData cmd = cmdMap.get(collectionName);
    if (null != cmd) {
      throw new InvalidJsonDbApiUsageException("Collection by name '" + collectionName + "' already exists.");
    }
    cmd = new CollectionMetaData(collectionName, dbConfig.getSchemaVersion(), dbConfig.getSchemaComparator());
    cmdMap.put(collectionName, cmd);
    @SuppressWarnings("unchecked")
    Map<String, JSONObject> collection = globalCollectionsRef.get(collectionName);
    if (null != collection) {
      throw new InvalidJsonDbApiUsageException("Collection by name '" + collectionName + "' already exists.");
    }

    cmd.getCollectionLock().writeLock().lock();

    // Some other thread might have created same collection when this thread reached this point
    if(globalCollectionsRef.get(collectionName) != null) {
      return;
    }

    try {
      String collectionFileName = collectionName + ".json";
      Path fileObject = new Path(dbConfig.getDbFilesLocation(), collectionFileName);
      FSDataOutputStream fsOut;
      try {
        fsOut = dbConfig.getDbFileSystem().create(fileObject);
      } catch (IOException e) {
        logger.error("IO Exception creating the collection file {}", collectionFileName, e);
        throw new InvalidJsonDbApiUsageException("Unable to create a collection file for collection: " + collectionName);
      }

      if (Util.stampVersion(dbConfig, fsOut, cmd.getSchemaVersion(), fileObject.toString())) {
        collection = new LinkedHashMap<String, JSONObject>();
        globalCollectionsRef.put(collectionName, collection);
        fileObjectsRef.get().put(collectionName, fileObject);
        cmd.setActualSchemaVersion(cmd.getSchemaVersion());
      } else {
        dbConfig.getDbFileSystem().delete(fileObject, false);
        throw new JsonDBException("Failed to stamp version for collection: " + collectionName);
      }
    } catch (IOException e) {
      logger.error("IO Exception deleting the collection file {}", collectionName, e);
      throw new InvalidJsonDbApiUsageException("Unable to create a collection file for collection: " + collectionName);
    } finally {
      cmd.getCollectionLock().writeLock().unlock();
    }
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#dropCollection(java.lang.String)
   */
  @Override
  public void dropCollection(String collectionName) {
    CollectionMetaData cmd = cmdMap.get(collectionName);
    if((null == cmd) || (!globalCollectionsRef.contains(collectionName))) {
      throw new InvalidJsonDbApiUsageException("Collection by name '" + collectionName + "' not found. Create collection first.");
    }
    cmd.getCollectionLock().writeLock().lock();
    try {
      Path toDelete = fileObjectsRef.get().get(collectionName);
      try {
        dbConfig.getDbFileSystem().delete(toDelete, false);
      } catch (IOException e) {
        logger.error("IO Exception deleting the collection file {}", toDelete.getName(), e);
        throw new InvalidJsonDbApiUsageException("Unable to create a collection file for collection: " + collectionName);
      }
      //cmdMap.remove(collectionName); //Do not remove it from the CollectionMetaData Map.
      //Someone might want to re insert a new collection of this type.
      fileObjectsRef.get().remove(collectionName);
      globalCollectionsRef.remove(collectionName);
      //contextsRef.get().remove(collectionName);
    } finally {
      cmd.getCollectionLock().writeLock().unlock();
    }
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#getCollectionNames()
   */
  @Override
  public Set<String> getCollectionNames() {
    return globalCollectionsRef.getCollectionNames();
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#collectionExists(java.lang.String)
   */
  @Override
  public boolean collectionExists(String collectionName) {
    CollectionMetaData collectionMeta = cmdMap.get(collectionName);
    if(null == collectionMeta) {
      return false;
    }
    collectionMeta.getCollectionLock().readLock().lock();
    try {
      return globalCollectionsRef.contains(collectionName);
    } finally {
      collectionMeta.getCollectionLock().readLock().unlock();
    }
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#isCollectionReadonly(java.lang.String)
   */
  @Override
  public boolean isCollectionReadonly(String collectionName) {
    CollectionMetaData cmd = cmdMap.get(collectionName);
    return cmd.isReadOnly();
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#find(java.lang.String, java.lang.String)
   */
  /*
  @SuppressWarnings("unchecked")
  @Override
  public <T> List<T> find(String jxQuery, String collectionName) {
    CollectionMetaData cmd = cmdMap.get(collectionName);
    Map<String, JSONObject> collection = (Map<String, JSONObject>) collectionsRef.get().get(collectionName);
    if((null == cmd) || (null == collection)) {
      throw new InvalidJsonDbApiUsageException("Collection by name '" + collectionName + "' not found. Create collection first.");
    }
    cmd.getCollectionLock().readLock().lock();
    try {
      JXPathContext context = contextsRef.get().get(collectionName);
      Iterator<T> resultItr = context.iterate(jxQuery);
      List<T> newCollection = new ArrayList<T>();
      while (resultItr.hasNext()) {
        T document = resultItr.next();
        Object obj = Util.deepCopy(document);
        if(encrypted && cmd.hasSecret() && null != obj) {
          CryptoUtil.decryptFields(obj, cmd, dbConfig.getCipher());
        }
        newCollection.add((T) obj);
      }
      return newCollection;
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      logger.error("Error when decrypting value for a @Secret annotated field for entity: " + collectionName, e);
      throw new JsonDBException("Error when decrypting value for a @Secret annotated field for entity: " + collectionName, e);
    } finally {
      cmd.getCollectionLock().readLock().unlock();
    }
  }
  */


  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#findAll(java.lang.String)
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<JSONObject> findAll(String collectionName, CollectionRef collectionRef) {
    CollectionMetaData cmd = cmdMap.get(collectionName);
    Map<String, JSONObject> collection = collectionRef.getCollectionToSearch(collectionName);
    cmd.getCollectionLock().readLock().lock();
    try {
      List<JSONObject> newCollection = new ArrayList<JSONObject>();
      for (JSONObject document : collection.values()) {
          newCollection.add(document);
      }
      return newCollection;
    } catch (IllegalArgumentException  e) {
      logger.error("Error when decrypting value for a @Secret annotated field for entity: " + collectionName, e);
      throw new JsonDBException("Error when decrypting value for a @Secret annotated field for entity: " + collectionName, e);
    } finally {
      cmd.getCollectionLock().readLock().unlock();
    }
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#findById(java.lang.Object, java.lang.String)
   */
  @SuppressWarnings("unchecked")
  @Override
  public JSONObject findById(String id, String collectionName, CollectionRef collectionRef) {
    CollectionMetaData cmd = cmdMap.get(collectionName);
    Map<String, JSONObject> collection = collectionRef.getCollectionToSearch(collectionName);
    cmd.getCollectionLock().readLock().lock();
    try {
      JSONObject obj = collection.get(id);
      return obj;
    } catch (IllegalArgumentException  e) {
      logger.error("Error when decrypting value for a @Secret annotated field for entity: " + collectionName, e);
      throw new JsonDBException("Error when decrypting value for a @Secret annotated field for entity: " + collectionName, e);
    } finally {
      cmd.getCollectionLock().readLock().unlock();
    }
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#findOne(java.lang.String, java.lang.String)
   */
  /*
  @SuppressWarnings("unchecked")
  @Override
  public <T> T findOne(String jxQuery, String collectionName) {
    CollectionMetaData collectionMeta = cmdMap.get(collectionName);
    if((null == collectionMeta) || (!collectionsRef.get().containsKey(collectionName))) {
      throw new InvalidJsonDbApiUsageException("Collection by name '" + collectionName + "' not found. Create collection first");
    }
    collectionMeta.getCollectionLock().readLock().lock();
    try {
      JXPathContext context = contextsRef.get().get(collectionName);
      Iterator<T> resultItr = context.iterate(jxQuery);
      while (resultItr.hasNext()) {
        T document = resultItr.next();
        Object obj = Util.deepCopy(document);
        if(encrypted && collectionMeta.hasSecret() && null!= obj){
          CryptoUtil.decryptFields(obj, collectionMeta, dbConfig.getCipher());
        }
        return (T) obj; // Return the first element we find.
      }
      return null;
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      logger.error("Error when decrypting value for a @Secret annotated field for entity: " + collectionName, e);
      throw new JsonDBException("Error when decrypting value for a @Secret annotated field for entity: " + collectionName, e);
    } finally {
      collectionMeta.getCollectionLock().readLock().unlock();
    }
  }
  */

  /**
   * This is for JSON Object to save
   * @param collectionName
   * @param JSON objectToSave
   */
  @Override
  public JSONObject insert(JSONObject objectToSave, String collectionName, CollectionRef collectionRef) {
    if (null == objectToSave) {
      throw new InvalidJsonDbApiUsageException("Null Object cannot be inserted into DB");
    }
    CollectionMetaData cmd = getCollectionMetaDataAndValidate(collectionName);
    cmd.getCollectionLock().writeLock().lock();
    try {
      Map<String, JSONObject> collection = collectionRef.getCollectionToModify(collectionName);
      String id = Util.getIdForEntity(objectToSave);

      if (null == id) {
        id = Util.setIdForEntity(objectToSave);
      } else if (collection.containsKey(id)) {
        throw new InvalidJsonDbApiUsageException("Object already present in Collection. Use Update or Upsert operation instead of Insert");
      }

      boolean appendResult = true;
      if (!collectionRef.isTransactionMode()) {
        JsonWriter jw;
        try {
          jw = new JsonWriter(dbConfig, cmd, collectionName, fileObjectsRef.get().get(collectionName));
        } catch (IOException ioe) {
          logger.error("Failed to obtain writer for " + collectionName, ioe);
          throw new JsonDBException("Failed to save " + collectionName, ioe);
        }
        appendResult = jw.appendToJsonFile(collection.values(), objectToSave);
      }

      if(appendResult) {
        collection.put(id, objectToSave);
        collectionRef.completeUpdate();
        return objectToSave;
      }
    } catch (IllegalArgumentException | JSONException  e) {
      logger.error("Error when encrypting value for a @Secret annotated field for entity: " + collectionName, e);
      throw new JsonDBException("Error when encrypting value for a @Secret annotated field for entity: " + collectionName, e);
    } finally {
      cmd.getCollectionLock().writeLock().unlock();
      collectionRef.makeFinal();
    }
    return null;
  }

  /**
   * @param collectionName
   * @return
   * @throws UnknownCollectionException
   */
  private Map<String, JSONObject> getCollectionAndValidate(String collectionName) throws UnknownCollectionException
  {
    Map<String, JSONObject> collection = globalCollectionsRef.get(collectionName);
    Util.ifNullThrowUnknownCollectionException(collectionName, collection);
    return collection;
  }

  /**
   * @param collectionName
   * @return
   * @throws UnknownCollectionException
   */
  private CollectionMetaData getCollectionMetaDataAndValidate(String collectionName) throws UnknownCollectionException
  {
    CollectionMetaData cmd = cmdMap.get(collectionName);
    Util.ifNullThrowUnknownCollectionException(collectionName, cmd);
    return cmd;
  }

  /**
   * @return the collectionsRef
   */
  public CollectionRef getCollectionsRef()
  {
    return globalCollectionsRef;
  }

  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#save(java.lang.Object, java.lang.String)
   */
  @Override
  public JSONObject update(JSONObject objToSave, String collectionName, CollectionRef collectionRef) {
    if (null == objToSave) {
      throw new InvalidJsonDbApiUsageException("Null Object cannot be updated into DB");
    }
    CollectionMetaData collectionMeta = getCollectionMetaDataAndValidate(collectionName);
    collectionMeta.getCollectionLock().writeLock().lock();
    try {
      @SuppressWarnings("unchecked")
      Map<String, JSONObject> collection = collectionRef.getCollectionToModify(collectionName);

      CollectionMetaData cmd = cmdMap.get(collectionName);
      String id = Util.getIdForEntity(objToSave);

      JSONObject existingObject = collection.get(id);
      if (null == existingObject) {
        throw new NoSuchElementException(String.format("Object with _id %s not found in collection %s", id, collectionName));
      }
      @SuppressWarnings("unchecked")
      boolean updateResult = true;
      if (!collectionRef.isTransactionMode()) {
        JsonWriter jw = null;
        try {
          jw = new JsonWriter(dbConfig, cmd, collectionName, fileObjectsRef.get().get(collectionName));
        } catch (IOException ioe) {
          logger.error("Failed to obtain writer for " + collectionName, ioe);
          throw new JsonDBException("Failed to save " + collectionName, ioe);
        }
        updateResult = jw.updateInJsonFile(collection, id, objToSave);
      }
      if (updateResult) {
        collection.put(id, objToSave);
        collectionRef.completeUpdate();
        return objToSave;
      }
    } catch (IllegalArgumentException | JSONException  e) {
      logger.error("Error when encrypting value for a @Secret annotated field for entity: " + collectionName, e);
      throw new JsonDBException("Error when encrypting value for a @Secret annotated field for entity: " + collectionName, e);
    } finally {
      collectionMeta.getCollectionLock().writeLock().unlock();
      collectionRef.makeFinal();
    }
    return null;
  }

  /* (non-Javadoc)
   * @see org.jsondb.JsonDBOperations#remove(java.lang.Object, java.lang.String)
   */
  @Override
  public JSONObject remove(String id, String collectionName, CollectionRef collectionRef) {
    CollectionMetaData collectionMeta = getCollectionMetaDataAndValidate(collectionName);
    collectionMeta.getCollectionLock().writeLock().lock();
    try {
      @SuppressWarnings("unchecked")
      Map<String, JSONObject> collection = collectionRef.getCollectionToModify(collectionName);

      if (!collection.containsKey(id)) {
        throw new NoSuchElementException(String.format("Object with _id %s not found in collection %s", id, collectionName));
      }
      boolean substractResult = true;
      if (!collectionRef.isTransactionMode()) {
        JsonWriter jw;
        try {
          jw = new JsonWriter(dbConfig, collectionMeta, collectionName, fileObjectsRef.get().get(collectionName));
        } catch (IOException ioe) {
          logger.error("Failed to obtain writer for " + collectionName, ioe);
          throw new JsonDBException("Failed to save " + collectionName, ioe);
        }
        substractResult = jw.removeFromJsonFile(collection, id);
      }
      if(substractResult) {
        JSONObject objectRemoved = collection.remove(id);
        collectionRef.completeUpdate();
        return objectRemoved;
      } else {
        return null;
      }
    } finally {
      collectionMeta.getCollectionLock().writeLock().unlock();
      collectionRef.makeFinal();
    }
  }

  /* (non-Javadoc)
   * @see org.jsondb.JsonDBOperations#findAllAndRemove(java.lang.String, java.lang.String)
   */
  @Override
  public List<JSONObject> searchAndDelete(String jxQuery, String collectionName, CollectionRef collectionRef) {
    CollectionMetaData cmd = getCollectionMetaDataAndValidate(collectionName);
    @SuppressWarnings("unchecked")
    Map<String, JSONObject> collection = collectionRef.getCollectionToModify(collectionName);
    cmd.getCollectionLock().writeLock().lock();
    try {
      JXPathContext context = JXPathContext.newContext(collection.values());
      Iterator<JSONObject> resultItr = context.iterate(jxQuery);
      List<String> removeIds = new ArrayList<String>();
      while (resultItr.hasNext()) {
        JSONObject objectToRemove = resultItr.next();
        String idToRemove = Util.getIdForEntity(objectToRemove);
        removeIds.add(idToRemove);
      }

      if(removeIds.size() < 1) {
        return null;
      }

      boolean substractResult = true;
      if (!collectionRef.isTransactionMode()) {
        JsonWriter jw;
        try {
          jw = new JsonWriter(dbConfig, cmd, collectionName, fileObjectsRef.get().get(collectionName));
        } catch (IOException ioe) {
          logger.error("Failed to obtain writer for " + collectionName, ioe);
          throw new JsonDBException("Failed to save " + collectionName, ioe);
        }
        substractResult = jw.removeFromJsonFile(collection, removeIds);
      }

      List<JSONObject> removedObjects = null;
      if(substractResult) {
        removedObjects = new ArrayList<JSONObject>();
        for (Object id : removeIds) {
          // Don't need to clone it, this object no more exists in the collection
          removedObjects.add(collection.remove(id));
        }
      }
      collectionRef.completeUpdate();
      return removedObjects;
    } catch (JXPathInvalidSyntaxException e) {
      logger.error("Invalid JXPath for : " + collectionName, e);
      throw new IllegalArgumentException(e.getMessage());
    } catch (JSONException e) {
      logger.error("searchAndDelete for  " + collectionName, e);
      throw new JsonDBException("searchAndDelete for " + collectionName, e);
    } finally {
      cmd.getCollectionLock().writeLock().unlock();
      collectionRef.makeFinal();
    }
  }
  
  /* (non-Javadoc)
   * @see io.jsondb.JsonDBOperations#find(java.lang.String, java.lang.String)
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<JSONObject> search(String jxQuery, String collectionName, CollectionRef collectionRef) {
    CollectionMetaData cmd = getCollectionMetaDataAndValidate(collectionName);
    Map<String, JSONObject> collection = collectionRef.getCollectionToSearch(collectionName);
    cmd.getCollectionLock().readLock().lock();
    try {
      JXPathContext context = JXPathContext.newContext(collection.values());
      Iterator<JSONObject> resultItr = context.iterate(jxQuery);
      List<JSONObject> newCollection = new ArrayList<JSONObject>();
      while (resultItr.hasNext()) {
        Object cur = resultItr.next();
        if (cur instanceof JSONObject) {
          newCollection.add((JSONObject)cur);
        }
      }
      return newCollection;
    } catch (JXPathInvalidSyntaxException e) {
      logger.error("Invalid JXPath for : " + collectionName, e);
      throw new IllegalArgumentException(e.getMessage());
    } finally {
      cmd.getCollectionLock().readLock().unlock();
    }
  }

  /* (non-Javadoc)
   * @see org.jsondb.JsonDBOperations#backup(java.lang.String)
   */
  @Override
  public void backup(String backupPath) {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see org.jsondb.JsonDBOperations#restore(java.lang.String, boolean)
   */
  @Override
  public void restore(String restorePath, boolean merge) {
    throw new UnsupportedOperationException();
  }

  public void saveCollectionsToDisk(Set<String> commitedSet) throws IOException
  {
    for (String collectionName : commitedSet) {
      saveCollectionToDisk(collectionName);
    }
  }

  private void saveCollectionToDisk(String collectionName) throws IOException
  {
    CollectionMetaData cmd = getCollectionMetaDataAndValidate(collectionName);
    Map<String, JSONObject> collection = globalCollectionsRef.getCollectionToModify(collectionName);
    JsonWriter jw = new JsonWriter(dbConfig, cmd, collectionName, fileObjectsRef.get().get(collectionName), collection.values());    
  }
}
