/**
 *  Copyright (c) 2012-2018 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package io.jsondb;


import java.io.File;
import java.util.NoSuchElementException;

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
 * Junit Tests for the update() apis
 */
public class UpdateTests {

  private String dbFilesLocation = "src/test/resources/dbfiles/insertTests";
  private File dbFilesFolder = new File(dbFilesLocation);
  private File instancesJson = new File(dbFilesFolder, "instances.json");

  private JsonDBTemplate jsonDBTemplate = null;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    dbFilesFolder.mkdir();    
    jsonDBTemplate = new JsonDBTemplate(dbFilesLocation);
    Files.copy(new File("src/test/resources/dbfiles/instances.json"), instancesJson);
    jsonDBTemplate.reLoadDB();
  }

  @After
  public void tearDown() throws Exception {
    Util.delete(dbFilesFolder);
  }

  /**
   * Update a single record
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testUpdateOneObject() throws JSONException, org.json.JSONException
  {
    JSONObject json2 = jsonDBTemplate.findById("03", "instances", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(json2);
    Assert.assertEquals("ec2-54-191-03", json2.getString("hostname"));
    JSONObject json = new JSONObject(JSON1);
    JSONObject json1 = jsonDBTemplate.update(json, "instances", jsonDBTemplate.getCollectionsRef());
    JSONAssert.assertEquals("" + json, "" + json1, true);
    json2 = jsonDBTemplate.findById("03", "instances", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(json2);
    Assert.assertEquals("ec2-54-191-01", json2.getString("hostname"));
    Assert.assertEquals("d3aa045f71bf4d1dffd2c5f485a4bc1d", json2.getString("publicKey"));
  }

  private static final String JSON1 =
      "{\"_id\":\"03\",\"hostname\":\"ec2-54-191-01\",\"privateKey\":\"Zf9vl5K6WV6BA3eL7JbnrfPMjfJxc9Rkoo0zlROQlgTslmcp9iFzos+MP93GZqop\",\"publicKey\":\"d3aa045f71bf4d1dffd2c5f485a4bc1d\"}";
  
  /**
   * Update a single record
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testUpdateUnknownObject() throws JSONException, org.json.JSONException
  {
    thrown.expect(NoSuchElementException.class);
    thrown.expectMessage("Object with _id 09 not found in collection instances");
    JSONObject json = new JSONObject(JSON2);
    JSONObject json1 = jsonDBTemplate.update(json, "instances", jsonDBTemplate.getCollectionsRef());
  }
  
  private static final String JSON2 =
      "{\"_id\":\"09\",\"hostname\":\"ec2-54-191-01\",\"privateKey\":\"Zf9vl5K6WV6BA3eL7JbnrfPMjfJxc9Rkoo0zlROQlgTslmcp9iFzos+MP93GZqop\",\"publicKey\":\"d3aa045f71bf4d1dffd2c5f485a4bc1d\"}";

}

