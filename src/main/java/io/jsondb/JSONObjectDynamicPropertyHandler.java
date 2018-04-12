/**
 *  Copyright (c) 2012-2018 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package io.jsondb;

import java.util.ArrayList;
import java.util.Iterator;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.jxpath.DynamicPropertyHandler;

/**
 * This class allows JSONObject to be supported as a dynamic collection of name/value pairs
 * in the org.apache.commons.jxpath framework
 *
 */
public class JSONObjectDynamicPropertyHandler implements DynamicPropertyHandler
{
  private Logger logger = LoggerFactory.getLogger(JSONObjectDynamicPropertyHandler.class);

  /* (non-Javadoc)
   * @see org.apache.commons.jxpath.DynamicPropertyHandler#getPropertyNames(java.lang.Object)
   */
  @Override
  public String[] getPropertyNames(Object object)
  {
    if (object instanceof JSONObject) {
      JSONObject json = (JSONObject)object;
      Iterator<String>  keys = json.keys();
      ArrayList<String> list = new ArrayList<String>();
      while (keys.hasNext()) {
        list.add(keys.next());
      }
      return list.toArray(new String[0]);
    }
    return new String[0];
  }

  /* (non-Javadoc)
   * @see org.apache.commons.jxpath.DynamicPropertyHandler#getProperty(java.lang.Object, java.lang.String)
   */
  @Override
  public Object getProperty(Object object, String propertyName)
  {
    if (object instanceof JSONObject) {
      JSONObject json = (JSONObject)object;
      Object value = json.opt(propertyName);
      if (value instanceof JSONArray) {
        JSONArray jarray = (JSONArray)value;
        ArrayList<Object> aList = new ArrayList<Object>(jarray.length());
        for (int i = 0; i < jarray.length(); i++) {
          try {
            aList.add(jarray.get(i));
          } catch (JSONException e) {
            logger.error("getProperty", e);
            aList.add(null);
          }
        }
        return aList;
      }
      return value;
    }
    return null;
  }

  /* (non-Javadoc)
   * @see org.apache.commons.jxpath.DynamicPropertyHandler#setProperty(java.lang.Object, java.lang.String, java.lang.Object)
   */
  @Override
  public void setProperty(Object object, String propertyName, Object value)
  {
    if (object instanceof JSONObject) {
      JSONObject json = (JSONObject)object;
      try {
        json.put(propertyName, value);
      } catch (JSONException e) {
        logger.error("setProperty", e);
      }
    }

  }

}
