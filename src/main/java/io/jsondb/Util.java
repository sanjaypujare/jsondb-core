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


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Farooq Khan
 * @version 1.0 25-Sep-2016
 */
public class Util {
  private static Logger logger = LoggerFactory.getLogger(Util.class);
  
  public static final String ID_FIELD_KEY = "_id";
  
  /**
   * A utility method to extract the id
   * 
   * @param JSON object we want the id of
   * @return the actual Id or if none exists then a new random UUID
   * @throws JSONException 
   */
  protected static String getIdForEntity(JSONObject json) throws JSONException {
    return json.optString(ID_FIELD_KEY, null);
  }

  
  /**
   * A utility method to set the value of field marked by the @Id annotation using its
   * setter/mutator method.
   * TODO: Some day we want to support policies for generation of ID like AutoIncrement etc.
   *
   * @param document the actual Object representing the POJO we want the Id to be set for.
   * @param setterMethodForId the Method that is the mutator for the attributed with @Id annotation
   * @return the Id that was generated and set
   */
  protected static Object setIdForEntity(Object document, Method setterMethodForId) {
    Object id = UUID.randomUUID().toString();
    if (null != setterMethodForId) {
      try {
        id = setterMethodForId.invoke(document, id);
      } catch (IllegalAccessException e) {
        logger.error("Failed to invoke setter method for a idAnnotated field due to permissions", e);
        throw new InvalidJsonDbApiUsageException("Failed to invoke setter method for a idAnnotated field due to permissions", e);
      } catch (IllegalArgumentException e) {
        logger.error("Failed to invoke setter method for a idAnnotated field due to wrong arguments", e);
        throw new InvalidJsonDbApiUsageException("Failed to invoke setter method for a idAnnotated field due to wrong arguments", e);
      } catch (InvocationTargetException e) {
        logger.error("Failed to invoke setter method for a idAnnotated field, the method threw a exception", e);
        throw new InvalidJsonDbApiUsageException("Failed to invoke setter method for a idAnnotated field, the method threw a exception", e);
      }
    }
    return id;
  }

  /**
   * A utility method to set the value of field marked by the @Id annotation using its
   * setter/mutator method.
   * TODO: Some day we want to support policies for generation of ID like AutoIncrement etc.
   *
   * @param document the actual Object representing the POJO we want the Id to be set for.
   * @param setterMethodForId the Method that is the mutator for the attributed with @Id annotation
   * @return the Id that was generated and set
   * @throws JSONException 
   */
  protected static String setIdForEntity(JSONObject document) throws JSONException {
    String id = UUID.randomUUID().toString();
    document.put(ID_FIELD_KEY, id);
    return id;
  }

  /**
   * Utility to stamp the version into a newly created .json File
   * This method is expected to be invoked on a newly created .json file before it is usable.
   * So no locking code required.
   * 
   * @param dbConfig  all the settings used by Json DB
   * @param f  the target .json file on which to stamp the version
   * @param version  the actual version string to stamp
   * @return true if success.
   */
  public static boolean stampVersion(JsonDBConfig dbConfig, File f, String version) {
    FileOutputStream fos;
    try {
      fos = new FileOutputStream(f);
      return stampVersion(dbConfig, fos, version, f.getAbsolutePath());
    } catch (FileNotFoundException e) {
      logger.error("FileNotFoundException new .json file {}", f, e);
      return false;
    }

  }

  public static void stampVersion(BufferedWriter writer, String schemaVersion) throws IOException
  {
    String version = String.format("{\"schemaVersion\":\"%s\"}", schemaVersion);
    writer.write(version);
    writer.newLine();
  }
  
  /**
   * Utility to stamp the version into a newly created .json File
   * This method is expected to be invoked on a newly created .json file before it is usable.
   * So no locking code required.
   * 
   * @param dbConfig  all the settings used by Json DB
   * @param f  the target .json file on which to stamp the version
   * @param version  the actual version string to stamp
   * @return true if success.
   */
  public static boolean stampVersion(JsonDBConfig dbConfig, OutputStream fos, String version, String f) {
    OutputStreamWriter osr = null;
    BufferedWriter writer = null;
    try {
      osr = new OutputStreamWriter(fos, dbConfig.getCharset());
      writer = new BufferedWriter(osr);

      Util.stampVersion(writer, version);
    } catch (IOException e) {
      logger.error("Failed to write SchemaVersion to the new .json file {}", f, e);
      return false;
    } finally {
      try {
        writer.close();
      } catch (IOException e) {
        logger.error("Failed to close BufferedWriter for new collection file {}", f, e);
      }
      try {
        osr.close();
      } catch (IOException e) {
        logger.error("Failed to close OutputStreamWriter for new collection file {}", f, e);
      }
      try {
        fos.close();
      } catch (IOException e) {
        logger.error("Failed to close FileOutputStream for new collection file {}", f, e);
      }
    }
    return true;
  }

  
  /**
   * Utility to delete directory recursively
   * @param f  File object representing the directory to recursively delete
   */
  public static void delete(File f) {
    if (f.isDirectory()) {
      for (File c : f.listFiles()) {
        delete(c);
      }
    }
    f.delete();
  }

  /**
   * @param collectionName
   * @param collection
   * @throws UnknownCollectionException
   */
  public static void ifNullThrowUnknownCollectionException(String collectionName, Object collection)
      throws UnknownCollectionException
  {
    if (null == collection) {
      throw new UnknownCollectionException("Collection by name '" + collectionName + "' not found.");
    }
  }
}
