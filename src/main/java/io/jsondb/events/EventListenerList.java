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
package io.jsondb.events;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.AppendEvent;
import org.apache.hadoop.hdfs.inotify.Event.CloseEvent;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent.INodeType;
import org.apache.hadoop.hdfs.inotify.Event.RenameEvent;
import org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.jsondb.CollectionMetaData;
import io.jsondb.JsonDBConfig;
import io.jsondb.JsonDBException;

/**
 * A class that holds a list of CollectionFileChangeListeners.
 * @version 1.0 15-Oct-2016
 */
public class EventListenerList {
  private Logger logger = LoggerFactory.getLogger(EventListenerList.class);

  private JsonDBConfig dbConfig = null;
  private Map<String, CollectionMetaData> cmdMap;

  private List<CollectionFileChangeListener> listeners;
  private ExecutorService collectionFilesWatcherExecutor;
  //private WatchService watcher = null;
  private boolean stopWatcher;
  private DFSInotifyEventInputStream dfsNotifyStream;

  public EventListenerList(JsonDBConfig dbConfig, Map<String, CollectionMetaData> cmdMap) {
    this.dbConfig = dbConfig;
    this.cmdMap = cmdMap;
  }

  public void addCollectionFileChangeListener(CollectionFileChangeListener listener) {
    if (null == listeners) {
      listeners = new ArrayList<CollectionFileChangeListener>();

      listeners.add(listener);

      collectionFilesWatcherExecutor = Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder().setNameFormat("jsondb-files-watcher-thread-%d").build());

      try {
        /*
        UserGroupInformation ugi = UserGroupInformation.createProxyUser("hdfs", UserGroupInformation.getLoginUser());
        ugi.doAs(new PrivilegedExceptionAction<Void>() {

          @Override
          public Void run() throws Exception
          {
            HdfsAdmin admin = new HdfsAdmin(URI.create(dbConfig.getDbFilesLocationString()), dbConfig.getConfiguration());
            dfsNotifyStream = admin.getInotifyEventStream();
            return null;
          }
        }); */
        HdfsAdmin admin = new HdfsAdmin(URI.create(dbConfig.getDbFilesLocationString()), dbConfig.getConfiguration());
        dfsNotifyStream = admin.getInotifyEventStream();
      } catch (IOException e) {
        logger.error("Failed to create the WatchService for the dbFiles location", e);
        throw new JsonDBException("Failed to create the WatchService for the dbFiles location", e);
      }
      stopWatcher = false;
      collectionFilesWatcherExecutor.execute(new CollectionFilesWatcherRunnable());
    } else {
      listeners.add(listener);
    }
  }

  public void removeCollectionFileChangeListener(CollectionFileChangeListener listener) {
    if (null != listeners) {
      listeners.remove(listener);
    }
    // removed code that was incorrectly stopping the watcher thread
  }

  public boolean hasCollectionFileChangeListener() {
    if ((null != listeners) && (listeners.size() > 0)) {
      return true;
    }
    return false;
  }

  public void shutdown() {
    if (null != listeners && listeners.size() > 0) {
      stopWatcher = true;
      collectionFilesWatcherExecutor.shutdownNow();
      listeners.clear();
    }
  }

  private class CollectionFilesWatcherRunnable implements Runnable {
    
    private String getMonitoredFile(String path)
    {
      if (path.startsWith(dbConfig.getDbFilesLocationString())) {
        String collectionName = getFileName(path);
        if (collectionName.endsWith(".json") && (cmdMap.containsKey(collectionName))) {
          return collectionName;
        }
      }
      return null; 
    }
    
    private void sendFileAddedEvent(String createdPath)
    {
      String collectionName = getMonitoredFile(createdPath);
      if (collectionName != null) {
        for (CollectionFileChangeListener listener : listeners) {
          listener.collectionFileAdded(collectionName);
        }
      }
    }
    
    private void sendFileDeletedEvent(String deletedPath)
    {
      String collectionName = getMonitoredFile(deletedPath);
      if (collectionName != null) {
        for (CollectionFileChangeListener listener : listeners) {
          listener.collectionFileDeleted(collectionName);
        }
      }      
    }
    
    private void sendFileModifiedEvent(String modifiedPath)
    {
      String collectionName = getMonitoredFile(modifiedPath);
      if (collectionName != null) {
        for (CollectionFileChangeListener listener : listeners) {
          listener.collectionFileModified(collectionName);
        }
      }
    }
    
    @Override
    public void run()
    {
      while (!stopWatcher) {
        Event event = null;
        try {
           event = dfsNotifyStream.take();
        } catch (IOException | InterruptedException | MissingEventsException e) {
          logger.debug("The watcher service thread was interrupted", e);
          return;
        }
        if (event == null) {
          continue;
        }
        switch (event.getEventType()) {
          case CREATE:
            CreateEvent createEvent = (CreateEvent)event;
            String createdPath = createEvent.getPath();
            if (createEvent.getiNodeType() == INodeType.FILE) {
              sendFileAddedEvent(createdPath);
            }
            break;
          case APPEND:
            AppendEvent appendEvent = (AppendEvent)event;
            sendFileModifiedEvent(appendEvent.getPath());
            break;
          case UNLINK:
            UnlinkEvent unlinkEvent = (UnlinkEvent)event;
            sendFileDeletedEvent(unlinkEvent.getPath());
            break;
          case RENAME: // treat dst as added and src as deleted
            RenameEvent renameEvent = (RenameEvent)event;
            sendFileAddedEvent(renameEvent.getDstPath());
            sendFileDeletedEvent(renameEvent.getSrcPath());
            break;
          case CLOSE:  // Sent when a file is closed after append or create
            CloseEvent closeEvent = (CloseEvent)event;
            if (closeEvent.getFileSize() == -1) {
              // check the doc org.apache.hadoop.hdfs.inotify.Event.CloseEvent.getFileSize()
              sendFileModifiedEvent(closeEvent.getPath());
            } else {
              sendFileAddedEvent(closeEvent.getPath());
            }
            break;
          case METADATA:

        }
      }
    }
    
    private String getFileName(String path)
    {
      return new Path(path).getName();
    }
    
  }
}
