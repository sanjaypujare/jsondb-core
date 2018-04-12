/**
 *  Copyright (c) 2012-2018 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package io.jsondb;


import java.io.File;
import java.util.List;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skyscreamer.jsonassert.JSONAssert;

import com.google.common.io.Files;


/**
 * Junit Tests for the delete() apis
 */
public class SearchTests {

  private String dbFilesLocation = "src/test/resources/dbfiles/searchTests";
  private File dbFilesFolder = new File(dbFilesLocation);
  private File complexSiteJson = new File(dbFilesFolder, "complexsite.json");

  private JsonDBTemplate jsonDBTemplate = null;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    dbFilesFolder.mkdir();    
    jsonDBTemplate = new JsonDBTemplate(dbFilesLocation);
    Files.copy(new File("src/test/resources/dbfiles/complexsite.json"), complexSiteJson);
    jsonDBTemplate.reLoadDB();
  }

  @After
  public void tearDown() throws Exception {
    Util.delete(dbFilesFolder);
  }

  /**
   * Search yields a single record
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testSearchOneObject() throws JSONException, org.json.JSONException
  {
    String jxQuery = String.format("/.[location='%s']", "eclb-54-01");
    List<JSONObject> jsons = jsonDBTemplate.search(jxQuery, "complexsite", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(jsons);
    Assert.assertEquals(1, jsons.size());
    JSONAssert.assertEquals(JSON1, "" + jsons.get(0), true);
  }

  private static final String JSON1 =
      "{\"_id\":\"001\",\"location\":\"eclb-54-01\",\"address\":{\"number\":23,\"street\":\"Junction Ave\",\"city\":\"San Jose\"}}";
  
  /**
   * Search yields a single record
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testSearchNestedQuery() throws JSONException, org.json.JSONException
  {
    String jxQuery = String.format("/.[location='%s']/address[street='%s']", "eclb-54-05", "First St");
    List<JSONObject> jsons = jsonDBTemplate.search(jxQuery, "complexsite", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(jsons);
    Assert.assertEquals(1, jsons.size());
    JSONAssert.assertEquals(JSON2, "" + jsons.get(0), true);
  }
  
  private static final String JSON2 =
      "{\"number\":43,\"street\":\"First St\",\"city\":\"Cupertino\"}";
  
  /**
   * Search yields a single record
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testSearchNestedQueryGetParent() throws JSONException, org.json.JSONException
  {
    String jxQuery = String.format("/.[location='%s']/address[street='%s']", "eclb-54-05", "First St");
    List<JSONObject> jsons = jsonDBTemplate.search(jxQuery, "complexsite", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(jsons);
    Assert.assertEquals(1, jsons.size());
    JSONAssert.assertEquals(JSON2, "" + jsons.get(0), true);
  }
  
  /**
   * Use /.. to get the parent node. JXPath has a bug which causes NPE
   *
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Ignore
  public void testSearchNestedQueryParent() throws JSONException, org.json.JSONException
  {
    String jxQuery = String.format("/.[location='%s']/address[street='%s']/..", "eclb-54-05", "First St");
    List<JSONObject> jsons = jsonDBTemplate.search(jxQuery, "complexsite", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(jsons);
    Assert.assertEquals(1, jsons.size());
    JSONAssert.assertEquals(JSON2, "" + jsons.get(0), true);
  }
  
  /**
   * Search yields a single record
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testSearchQueryArray() throws JSONException, org.json.JSONException
  {
    String jxQuery = String.format("/.[Locations[2]='%s']", "eclb-64-10");
    List<JSONObject> jsons = jsonDBTemplate.search(jxQuery, "complexsite", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(jsons);
    Assert.assertEquals(1, jsons.size());
    JSONAssert.assertEquals(JSON3, "" + jsons.get(0), true);
  }
  
  private static final String JSON3 =
      "{\"_id\":\"010\",\"Locations\":[\"eclb-54-10\",\"eclb-64-10\"],\"address\":{\"number\":67,\"street\":\"Terrace Blvd\",\"city\":\"Fremont\"}}";

  /**
   * Search yields a single record
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testSearchQueryArray2() throws JSONException, org.json.JSONException
  {
    String jxQuery = String.format("/.[addresses[2]/number='%s']", "129");
    List<JSONObject> jsons = jsonDBTemplate.search(jxQuery, "complexsite", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(jsons);
    Assert.assertEquals(1, jsons.size());
    JSONAssert.assertEquals(JSON4, "" + jsons.get(0), true);
  }
  
  private static final String JSON4 =
  "{\"_id\":\"002\",\"loc\":\"eclb-54-02\",\"addresses\":[{\"number\":24,\"street\":\"Fruitvale Ave\",\"city\":\"Santa Clara\"},{\"number\":129,\"street\":\"Dale Rd\",\"city\":\"Authin\",\"state\":\"TX\"}]}";

  /**
   * Search yields multiple records
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testSearchQueryMultiple() throws JSONException, org.json.JSONException
  {
    String jxQuery = String.format("/.[address/city='%s']", "San Jose");
    List<JSONObject> jsons = jsonDBTemplate.search(jxQuery, "complexsite", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(jsons);
    Assert.assertEquals(2, jsons.size());
    JSONAssert.assertEquals(JSON5, "" + jsons, true);
  }
  
  private static final String JSON5 =
  "[{\"_id\":\"001\",\"location\":\"eclb-54-01\",\"address\":{\"number\":23,\"street\":\"Junction Ave\",\"city\":\"San Jose\"}}, {\"_id\":\"004\",\"placement\":\"eclb-54-04\",\"address\":{\"number\":35,\"street\":\"Zanker St\",\"city\":\"San Jose\"}}]";
 
  /**
   * Search yields a record with AND expr
   * @throws JSONException 
   * @throws org.json.JSONException 
   */
  @Test
  public void testSearchQueryAND() throws JSONException, org.json.JSONException
  {
    String jxQuery = String.format("/.[address/city='%s' and placement='%s']", "San Jose", "eclb-54-04");
    List<JSONObject> jsons = jsonDBTemplate.search(jxQuery, "complexsite", jsonDBTemplate.getCollectionsRef());
    Assert.assertNotNull(jsons);
    Assert.assertEquals(1, jsons.size());
    JSONAssert.assertEquals(JSON6, "" + jsons.get(0), true);
  }
  
  private static final String JSON6 =
  "{\"_id\":\"004\",\"placement\":\"eclb-54-04\",\"address\":{\"number\":35,\"street\":\"Zanker St\",\"city\":\"San Jose\"}}";
 
  
}

