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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * A POJO that has settings for the functioning of DB.
 * @author Farooq Khan
 * @version 1.0 25-Sep-2016
 */
public class JsonDBConfig {
  //Settings
  private Charset charset;
  private String dbFilesLocationString;
  private Path dbFilesLocation;
  private Path dbFilesPath;  // used to be nio.Path: TODO eliminate dup
  private FileSystem dbFileSystem;
  private String schemaVersion = "1.0";

  //References
  private Comparator<String> schemaComparator;
  private Configuration configuration;

  public JsonDBConfig(String dbFilesLocationString,
      Comparator<String> schemaComparator) {
    this(dbFilesLocationString, schemaComparator, new Configuration());
  }
  
  public JsonDBConfig(String dbFilesLocationString,
      Comparator<String> schemaComparator, Configuration conf) {

    this.charset = Charset.forName("UTF-8");
    this.dbFilesLocationString = dbFilesLocationString;
    this.dbFilesLocation = new Path(dbFilesLocationString);
    this.dbFilesPath = dbFilesLocation;

    if (null == schemaComparator) {
      this.schemaComparator = new DefaultSchemaVersionComparator();
    } else {
      this.schemaComparator = schemaComparator;
    }
    this.configuration = conf;
    try {
      dbFileSystem = FileSystem.newInstance(new URI(dbFilesLocationString), conf);
    } catch (IOException| URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  public Charset getCharset() {
    return charset;
  }
  public void setCharset(Charset charset) {
    this.charset = charset;
  }
  public String getDbFilesLocationString() {
    return dbFilesLocationString;
  }
  public void setDbFilesLocationString(String dbFilesLocationString) {
    this.dbFilesLocationString = dbFilesLocationString;
    this.dbFilesLocation = new Path(dbFilesLocationString);
    this.dbFilesPath = dbFilesLocation;
  }
  public Path getDbFilesLocation() {
    return dbFilesLocation;
  }
  public Path getDbFilesPath() {
    return dbFilesPath;
  }

  public FileSystem getDbFileSystem()
  {
    return dbFileSystem;
  }

  public Comparator<String> getSchemaComparator() {
    return schemaComparator;
  }

  public Configuration getConfiguration()
  {
    return configuration;
  }

  /**
   * @return the schemaVersion
   */
  public String getSchemaVersion()
  {
    return schemaVersion;
  }

  /**
   * @param schemaVersion the schemaVersion to set
   */
  public void setSchemaVersion(String schemaVersion)
  {
    this.schemaVersion = schemaVersion;
  }
}
