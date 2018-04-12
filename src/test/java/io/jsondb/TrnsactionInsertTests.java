/**
 *  Copyright (c) 2012-2018 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package io.jsondb;


import java.io.File;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONAssert;


/**
 * Junit Tests for the insert() apis within a transaction
 */
public class TrnsactionInsertTests
{

  private String dbFilesLocation = "src/test/resources/dbfiles/insertTests";
  private File dbFilesFolder = new File(dbFilesLocation);
  private File instancesJson = new File(dbFilesFolder, "instances.json");

  private JsonDBTemplate jsonDBTemplate = null;
  private TransactionCollectionRef transactionCollectionRef;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception
  {
    dbFilesFolder.mkdir();
    
    jsonDBTemplate = new JsonDBTemplate(dbFilesLocation);
    transactionCollectionRef = new TransactionCollectionRef(jsonDBTemplate.getCollectionsRef());
  }

  @After
  public void tearDown() throws Exception
  {
    Util.delete(dbFilesFolder);
  }

  /**
   * insert a new object ensure not written to disk
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testInsert_NewObject() throws JSONException, org.json.JSONException
  {
    jsonDBTemplate.createCollection("instances");
    JSONObject json = new JSONObject(JSON1);
    JSONObject json1 = jsonDBTemplate.insert(json, "instances", transactionCollectionRef);
    Assert.assertNotNull(json1.getString(Util.ID_FIELD_KEY));
    Assert.assertEquals(36, json1.getString(Util.ID_FIELD_KEY).length());
    JSONObject json2 = jsonDBTemplate.findById(json1.getString(Util.ID_FIELD_KEY), "instances", transactionCollectionRef);
    JSONAssert.assertEquals("" + json1, "" + json2, true);
    // verify collection in transactionCollectionRef
    Assert.assertTrue(transactionCollectionRef.contains("instances"));
    // verify not saved to disk
    jsonDBTemplate.reloadCollection("instances");
    json2 = jsonDBTemplate.findById(json1.getString(Util.ID_FIELD_KEY), "instances", jsonDBTemplate.getCollectionsRef());
    Assert.assertNull(json2);
  }

  private static final String JSON1 =
      "{\"hostname\":\"ec2-54-191-01\",\"privateKey\":\"Zf9vl5K6WV6BA3eL7JbnrfPMjfJxc9Rkoo0zlROQlgTslmcp9iFzos+MP93GZqop\",\"publicKey\":\"d3aa045f71bf4d1dffd2c5f485a4bc1d\"}";

}

