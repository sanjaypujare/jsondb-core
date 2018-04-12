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
package io.jsondb.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.library.lock.LockFactory;

import io.jsondb.CollectionMetaData;
import io.jsondb.InvalidJsonDbApiUsageException;
import io.jsondb.JsonDBConfig;
import io.jsondb.SchemaVersion;
import io.jsondb.Util;

/**
 * A special File Writer to write to the .json DB files that ensures
 * proper character encoding is used and the necessary File Locks are created.
 *
 * @author Farooq Khan
 * @version 1.0 25-Sep-2016
 */
public class JsonWriter {

  private Logger logger = LoggerFactory.getLogger(JsonWriter.class);

  private Path dbFilesLocation;
  private String collectionName;
  private Path collectionFile;
  private Charset charset;
  private SchemaVersion schemaVersion;
  private CollectionMetaData cmd;

  private Path lockFilesLocation;
  private Path fileLockLocation;
  
  private ReadWriteLock lock;
  private FileSystem fileSystem;
    
  public JsonWriter(JsonDBConfig dbConfig, CollectionMetaData cmd, String collectionName, Path collectionFile) throws IOException {
    commonInit(dbConfig, cmd, collectionName, collectionFile);
    this.lockFilesLocation = new Path(collectionFile.getParent(), "lock");
    this.fileLockLocation = new Path(lockFilesLocation, collectionFile.getName() + ".lock");
    
    if(!dbConfig.getDbFileSystem().exists(lockFilesLocation)) {
      dbConfig.getDbFileSystem().mkdirs(lockFilesLocation);
    }
    if(!dbConfig.getDbFileSystem().exists(fileLockLocation)) {
      dbConfig.getDbFileSystem().mkdirs(fileLockLocation);
    }
    
    lock = LockFactory.create(LockFactory.lockType.GLOBAL_ONLY, dbConfig.getDbFileSystem()).getLock(fileLockLocation.toString());
  }

  public JsonWriter(JsonDBConfig dbConfig, CollectionMetaData cmd, String collectionName, Path collectionFile,
      Collection<JSONObject> collection) throws IOException
  {
    commonInit(dbConfig, cmd, collectionName, collectionFile);
    writeJsonFile(collection, null);
  }
  
  /**
   * @param dbConfig
   * @param cmd
   * @param collectionName
   * @param collectionFile
   */
  private void commonInit(JsonDBConfig dbConfig, CollectionMetaData cmd, String collectionName, Path collectionFile)
  {
    this.dbFilesLocation = dbConfig.getDbFilesLocation();
    this.collectionName = collectionName;
    this.collectionFile = collectionFile;
    this.charset = dbConfig.getCharset();
    this.schemaVersion = new SchemaVersion(cmd.getSchemaVersion());
    this.cmd = cmd;
    this.fileSystem = dbConfig.getDbFileSystem();
  }
  
  private Lock acquireLock() throws IOException {
    Lock ret = lock.writeLock();
    ret.lock();
    return ret;
  }
  
  private void releaseLock(Lock lock) {
    if (lock != null) {
      lock.unlock();
    }
  }

  /**
   * A utility method that appends the provided collection of objects to the end of collection
   * file in a atomic way
   *
   * @param collection existing collection
   * @param batchToSave collection of objects to append.
   * @return true if success
   */
  public boolean appendToJsonFile(Collection<JSONObject> collection, JSONObject json) {
    if (cmd.isReadOnly()) {
      throw new InvalidJsonDbApiUsageException("Failed to modify collection, Collection is loaded as readonly");
    }
    Lock lock = null;
    try {
      try {
        lock = acquireLock();
      } catch (IOException e) {
        logger.error("Failed to acquire lock for collection file {}", collectionFile.getName(), e);
        return false; 
      }
      return writeJsonFile(collection, json);
    } catch (IOException e) {
      logger.error("Failed to create temporary file or append object to temporary collection file", e);
      return false;
    } finally {
      releaseLock(lock);
    }
  }

  /**
   * This creates/writes to a Json file without creating locks
   * 
   * @param collection
   * @param json
   * @return
   * @throws IllegalArgumentException
   * @throws IOException 
   */
  private boolean writeJsonFile(Collection<JSONObject> collection, JSONObject json) throws IllegalArgumentException, IOException
  {
    File tFile = File.createTempFile(collectionName, null, null);
    String tFileName = tFile.getName();

    FileOutputStream fos = null;
    OutputStreamWriter osr = null;
    BufferedWriter writer = null;
    try {
      fos = new FileOutputStream(tFile);
      osr = new OutputStreamWriter(fos, charset);
      writer = new BufferedWriter(osr);
      writeInitial(collection, writer);
      if (json != null) {
        writer.write(json.toString());
      }
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        logger.error("Failed to close BufferedWriter for temporary collection file {}", tFileName, e);
      }
      try {
        osr.close();
      } catch (IOException e) {
        logger.error("Failed to close OutputStreamWriter for temporary collection file {}", tFileName, e);
      }
      try {
        fos.close();
      } catch (IOException e) {
        logger.error("Failed to close FileOutputStream for temporary collection file {}", tFileName, e);
      }
    }

    try {
      fileSystem.moveFromLocalFile(new Path(tFile.getAbsolutePath()), collectionFile); // TODO indicate StandardCopyOption.ATOMIC_MOVE
    } catch (IOException e) {
      logger.error("Failed to move temporary collection file {} to collection file {}", tFileName, collectionFile.getName(), e);
    }
    return true;
  }

  /**
   * @param collection
   * @param writer
   * @throws IOException
   */
  private void writeInitial(Collection<JSONObject> collection, BufferedWriter writer) throws IOException
  {
    //Stamp version first
    stampVersion(writer);
    
    for (JSONObject o : collection) {
      writer.write(o.toString());
      writer.newLine();
    }
  }

  private void stampVersion(BufferedWriter writer) throws IOException
  {
    Util.stampVersion(writer, schemaVersion.getSchemaVersion());
  }
  
  /**
   * A utility method that substracts the provided Ids and writes rest of the collection to
   * file in a atomic way
   *
   * @param collection existing collection
   * @param id id of objects to be removed.
   * @return true if success
   */
  public boolean removeFromJsonFile(Map<String, JSONObject> collection, String id) {
    if (cmd.isReadOnly()) {
      throw new InvalidJsonDbApiUsageException("Failed to modify collection, Collection is loaded as readonly");
    }
    Lock lock = null;
    try {
      try {
        lock = acquireLock();
      } catch (IOException e) {
        logger.error("Failed to acquire lock for collection file {}", collectionFile.getName(), e);
        return false; 
      }
      
      File tFile;
      try {
        tFile = File.createTempFile(collectionName, null, null);
      } catch (IOException e) {
        logger.error("Failed to create temporary file for append", e);
        return false;
      }
      String tFileName = tFile.getName();

      FileOutputStream fos = null;
      OutputStreamWriter osr = null;
      BufferedWriter writer = null;
      try {
        fos = new FileOutputStream(tFile);
        osr = new OutputStreamWriter(fos, charset);
        writer = new BufferedWriter(osr);

        //Stamp version first
        stampVersion(writer);
        
        for (Entry<String, JSONObject> entry : collection.entrySet()) {
          if (!entry.getKey().equals(id)) {
          writer.write(entry.getValue().toString());
          writer.newLine();
          }
        }
      } catch (IOException e) {
        logger.error("Failed to append object to temporary collection file {}", tFileName, e);
        return false;
      } finally {
        try {
          writer.close();
        } catch (IOException e) {
          logger.error("Failed to close BufferedWriter for temporary collection file {}", tFileName, e);
        }
        try {
          osr.close();
        } catch (IOException e) {
          logger.error("Failed to close OutputStreamWriter for temporary collection file {}", tFileName, e);
        }
        try {
          fos.close();
        } catch (IOException e) {
          logger.error("Failed to close FileOutputStream for temporary collection file {}", tFileName, e);
        }
      }

      try {
        fileSystem.moveFromLocalFile(new Path(tFile.getAbsolutePath()), collectionFile); // TODO indicate StandardCopyOption.ATOMIC_MOVE
      } catch (IOException e) {
        logger.error("Failed to move temporary collection file {} to collection file {}", tFileName, collectionFile.getName(), e);
      }
      return true;
      
    } finally {
      releaseLock(lock);
    }
  }

  /**
   * A utility method that subtracts the provided Ids and writes rest of the collection to
   * file in a atomic way
   *
   * @param collection existing collection
   * @param removeIds ids of objects to be removed.
   * @return true if success
   */
  public boolean removeFromJsonFile(Map<String, JSONObject> collection, Collection<String> removeIds) {
    if (cmd.isReadOnly()) {
      throw new InvalidJsonDbApiUsageException("Failed to modify collection, Collection is loaded as readonly");
    }
    Lock lock = null;
    try {
      try {
        lock = acquireLock();
      } catch (IOException e) {
        logger.error("Failed to acquire lock for collection file {}", collectionFile.getName(), e);
        return false; 
      }
      
      File tFile;
      try {
        tFile = File.createTempFile(collectionName, null, null);
      } catch (IOException e) {
        logger.error("Failed to create temporary file for append", e);
        return false;
      }
      String tFileName = tFile.getName();

      FileOutputStream fos = null;
      OutputStreamWriter osr = null;
      BufferedWriter writer = null;
      try {
        fos = new FileOutputStream(tFile);
        osr = new OutputStreamWriter(fos, charset);
        writer = new BufferedWriter(osr);

        //Stamp version first
        stampVersion(writer);
        
        for (Entry<String, JSONObject> entry : collection.entrySet()) {
          if (!removeIds.contains(entry.getKey())) {
          writer.write(entry.getValue().toString());
          writer.newLine();
          }
        }
      } catch (IOException e) {
        logger.error("Failed to append object to temporary collection file {}", tFileName, e);
        return false;
      } finally {
        try {
          writer.close();
        } catch (IOException e) {
          logger.error("Failed to close BufferedWriter for temporary collection file {}", tFileName, e);
        }
        try {
          osr.close();
        } catch (IOException e) {
          logger.error("Failed to close OutputStreamWriter for temporary collection file {}", tFileName, e);
        }
        try {
          fos.close();
        } catch (IOException e) {
          logger.error("Failed to close FileOutputStream for temporary collection file {}", tFileName, e);
        }
      }

      try {
        fileSystem.moveFromLocalFile(new Path(tFile.getAbsolutePath()), collectionFile); // TODO indicate StandardCopyOption.ATOMIC_MOVE
      } catch (IOException e) {
        logger.error("Failed to move temporary collection file {} to collection file {}", tFileName, collectionFile.getName(), e);
      }
      return true;
      
    } finally {
      releaseLock(lock);
    }
  }

  /**
   * A utility method that updates the provided collection of objects into the existing collection
   * file in a atomic way
   *
   * @param collection existing collection
   * @param id the id of object to save
   * @param objectToSave the actual object to save.
   * @return true if success
   */
  public boolean updateInJsonFile(Map<String, JSONObject> collection, String id, JSONObject objectToSave) {
    if (cmd.isReadOnly()) {
      throw new InvalidJsonDbApiUsageException("Failed to modify collection, Collection is loaded as readonly");
    }
    Lock lock = null;
    try {
      try {
        lock = acquireLock();
      } catch (IOException e) {
        logger.error("Failed to acquire lock for collection file {}", collectionFile.getName(), e);
        return false; 
      }
      
      File tFile;
      try {
        tFile = File.createTempFile(collectionName, null, null);
      } catch (IOException e) {
        logger.error("Failed to create temporary file for append", e);
        return false;
      }
      String tFileName = tFile.getName();

      FileOutputStream fos = null;
      OutputStreamWriter osr = null;
      BufferedWriter writer = null;
      try {
        fos = new FileOutputStream(tFile);
        osr = new OutputStreamWriter(fos, charset);
        writer = new BufferedWriter(osr);

        //Stamp version first
        stampVersion(writer);
        
        for (Entry<String, JSONObject> entry : collection.entrySet()) {
          JSONObject o = null;
          if (entry.getKey().equals(id)) {
            o = objectToSave;
          } else {
            o = entry.getValue();
          }
          writer.write(o.toString());
          writer.newLine();
        }
      } catch (IOException e) {
        logger.error("Failed to append object to temporary collection file {}", tFileName, e);
        return false;
      } finally {
        try {
          writer.close();
        } catch (IOException e) {
          logger.error("Failed to close BufferedWriter for temporary collection file {}", tFileName, e);
        }
        try {
          osr.close();
        } catch (IOException e) {
          logger.error("Failed to close OutputStreamWriter for temporary collection file {}", tFileName, e);
        }
        try {
          fos.close();
        } catch (IOException e) {
          logger.error("Failed to close FileOutputStream for temporary collection file {}", tFileName, e);
        }
      }

      try {
        fileSystem.moveFromLocalFile(new Path(tFile.getAbsolutePath()), collectionFile); // TODO indicate StandardCopyOption.ATOMIC_MOVE
      } catch (IOException e) {
        logger.error("Failed to move temporary collection file {} to collection file {}", tFileName, collectionFile.getName(), e);
      }
      return true;
      
    } finally {
      releaseLock(lock);
    }
  }

}
