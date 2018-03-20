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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.datatorrent.gateway.ha.lock.LockFactory;

import io.jsondb.JsonDBConfig;

/**
 * A special File Reader to read the .json DB files that ensures
 * proper character encoding is used and the necessary File Locks are created.
 *
 * @author Farooq Khan
 * @version 1.0 25-Sep-2016
 */
public class JsonReader {

  private Logger logger = LoggerFactory.getLogger(JsonReader.class);

  private Path collectionFile;

  private FSDataInputStream fis;
  private InputStreamReader isr;
  private BufferedReader reader;
  private Lock  lock;
  private Path lockFilesLocation;
  private Path fileLockLocation;

  public JsonReader(JsonDBConfig dbConfig, Path collectionFile) throws IOException {
    this.collectionFile = collectionFile;
    this.lockFilesLocation = new Path(collectionFile.getParent(), "lock");
    this.fileLockLocation = new Path(lockFilesLocation, collectionFile.getName() + ".lock");
    
    if(!dbConfig.getDbFileSystem().exists(lockFilesLocation)) {
      dbConfig.getDbFileSystem().mkdirs(lockFilesLocation);
    }
    if(!dbConfig.getDbFileSystem().exists(fileLockLocation)) {
      dbConfig.getDbFileSystem().mkdirs(fileLockLocation);
    }

    CharsetDecoder decoder = dbConfig.getCharset().newDecoder();
    decoder.onMalformedInput(CodingErrorAction.REPORT);
    decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    
    lock = LockFactory.create(LockFactory.lockType.GLOBAL_ONLY, dbConfig.getDbFileSystem()).getLock(fileLockLocation.toString()).readLock();
    lock.lock();
    fis = dbConfig.getDbFileSystem().open(collectionFile);
    isr = new InputStreamReader(fis, decoder);
    reader = new BufferedReader(isr);
  }
  
  /**
   * A utility method that reads the next line and returns it.
   * Since we use a BufferedReader this method may often read more
   * than the next line to determine if the line ended.
   * @return the content of the line just read
   * @throws IOException if an I/O error occurs
   */
  public String readLine() throws IOException {
    return reader.readLine();
  }

  public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      logger.error("Failed to close BufferedReader for collection file {}", collectionFile.getName(), e);
    }
    try {
      isr.close();
    } catch (IOException e) {
      logger.error("Failed to close InputStreamReader for collection file {}", collectionFile.getName(), e);
    }
    if(lock != null) {
      lock.unlock();
    }
    try {
      fis.close();
    } catch (IOException e) {
      logger.error("Failed to close FileInputStream for collection file {}", collectionFile.getName(), e);
    }
  }
}
