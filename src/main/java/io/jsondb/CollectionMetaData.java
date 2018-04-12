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

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;


/**
 * @version 1.0 25-Sep-2016
 */
public class CollectionMetaData {
  private static Logger logger = LoggerFactory.getLogger(CollectionMetaData.class);
  private String collectionName;
  private String schemaVersion;
  private String actualSchemaVersion;
  private Comparator<String> schemaComparator;

  private final ReentrantReadWriteLock collectionLock;

  private boolean readonly;

  public CollectionMetaData(String collectionName, String schemaVersion, Comparator<String> schemaComparator) {
    super();
    this.collectionName = collectionName;
    this.schemaVersion = schemaVersion;
    this.schemaComparator = schemaComparator;

    this.collectionLock = new ReentrantReadWriteLock();

  }

  protected ReentrantReadWriteLock getCollectionLock() {
    return collectionLock;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }
  public String getActualSchemaVersion() {
    return actualSchemaVersion;
  }
  public void setActualSchemaVersion(String actualSchemaVersion) {
    this.actualSchemaVersion = actualSchemaVersion;
    int compareResult = schemaComparator.compare(schemaVersion, actualSchemaVersion);
    if (compareResult != 0) {
      readonly = true;
    } else {
      readonly = false;
    }
  }

  public boolean isReadOnly() {
    return readonly;
  }


  /**
   * A utility builder method to scan through the specified package and find all classes/POJOs
   * that are annotated with the @Document annotation.
   *
   * @param dbConfig the object that holds all the baseScanPackage and other settings.
   * @return A Map of collection classes/POJOs
   */
  public static Map<String, CollectionMetaData> builder(JsonDBConfig dbConfig) {
    Map<String, CollectionMetaData> collectionMetaData = new LinkedHashMap<String, CollectionMetaData>();
    // generate the list of collection from files
    try {
      RemoteIterator<LocatedFileStatus> iterator = dbConfig.getDbFileSystem().listFiles(dbConfig.getDbFilesPath(), false);
      while (iterator.hasNext()) {
        LocatedFileStatus file = iterator.next();
        String name = file.getPath().getName();
        if (name.endsWith(".json")) {
         name = name.substring(0, name.length() - 5);
         CollectionMetaData cmd = new CollectionMetaData(name, dbConfig.getSchemaVersion(), dbConfig.getSchemaComparator());
         collectionMetaData.put(name, cmd);
        }
      }
    } catch (IOException e) {
      logger.error("in builder()", e);
    }
    return collectionMetaData;
  }
}
