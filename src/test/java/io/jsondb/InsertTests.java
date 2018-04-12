/**
 *  Copyright (c) 2012-2018 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package io.jsondb;


import java.io.File;
import java.io.IOException;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONAssert;

import com.google.common.io.Files;


/**
 * Junit Tests for the insert() apis
 */
public class InsertTests
{

  private String dbFilesLocation = "src/test/resources/dbfiles/insertTests";
  private File dbFilesFolder = new File(dbFilesLocation);
  private File instancesJson = new File(dbFilesFolder, "instances.json");

  private JsonDBTemplate jsonDBTemplate = null;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception
  {
    dbFilesFolder.mkdir();
    
    jsonDBTemplate = new JsonDBTemplate(dbFilesLocation);
  }

  @After
  public void tearDown() throws Exception
  {
    Util.delete(dbFilesFolder);
  }

  @Test
  public void testReadExistingFile() throws JSONException, IOException
  {
    Files.copy(new File("src/test/resources/dbfiles/instances.json"), instancesJson);
    jsonDBTemplate.reLoadDB();
    // make sure all 6 are there
    List<JSONObject> list = jsonDBTemplate.findAll("instances", jsonDBTemplate.getCollectionsRef());
    Assert.assertEquals(6, list.size());
    JSONObject json = list.get(0);
    Assert.assertNotNull(json);
    Assert.assertEquals("01", json.getString(Util.ID_FIELD_KEY));
    Assert.assertEquals("ec2-54-191-01", json.getString("hostname"));
    json = list.get(4);
    Assert.assertNotNull(json);
    Assert.assertEquals("05", json.getString(Util.ID_FIELD_KEY));
    Assert.assertEquals("d3aa045f71bf4d1dffd2c5f485a4bc1d", json.getString("publicKey"));
  }

  /**
   * Test to insert a new object into a known collection type which has some data.
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testInsert_NewObject() throws JSONException, org.json.JSONException
  {
    jsonDBTemplate.createCollection("instances");
    JSONObject json = new JSONObject(JSON1);
    JSONObject json1 = jsonDBTemplate.insert(json, "instances", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(json1.getString(Util.ID_FIELD_KEY));
    Assert.assertEquals(36, json1.getString(Util.ID_FIELD_KEY).length());
    JSONObject json2 = jsonDBTemplate.findById(json1.getString(Util.ID_FIELD_KEY), "instances", jsonDBTemplate.getCollectionsRef());
    JSONAssert.assertEquals("" + json1, "" + json2, true);
  }

  private static final String JSON1 =
      "{\"hostname\":\"ec2-54-191-01\",\"privateKey\":\"Zf9vl5K6WV6BA3eL7JbnrfPMjfJxc9Rkoo0zlROQlgTslmcp9iFzos+MP93GZqop\",\"publicKey\":\"d3aa045f71bf4d1dffd2c5f485a4bc1d\"}";
  
  /**
   * Same as above with _id set
   *
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testInsert_NewObject_IdSet() throws JSONException, org.json.JSONException
  {
    jsonDBTemplate.createCollection("instances");
    JSONObject json = new JSONObject(JSON2);
    JSONObject json1 = jsonDBTemplate.insert(json, "instances", jsonDBTemplate.getCollectionsRef());
    Assert.assertEquals("01", json1.getString(Util.ID_FIELD_KEY));
    JSONObject json2 = jsonDBTemplate.findById("01", "instances", jsonDBTemplate.getCollectionsRef());
    JSONAssert.assertEquals("" + json1, "" + json2, true);
  }
  
  private static final String JSON2 =
      "{\"_id\":\"01\",\"hostname\":\"ec2-54-191-01\",\"privateKey\":\"Zf9vl5K6WV6BA3eL7JbnrfPMjfJxc9Rkoo0zlROQlgTslmcp9iFzos+MP93GZqop\",\"publicKey\":\"d3aa045f71bf4d1dffd2c5f485a4bc1d\"}";

}

