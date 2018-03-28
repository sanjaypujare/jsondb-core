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
package io.jsondb.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.security.GeneralSecurityException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.io.Files;

import io.jsondb.InvalidJsonDbApiUsageException;
import io.jsondb.JsonDBTemplate;
import io.jsondb.Util;
import io.jsondb.crypto.DefaultAESCBCCipher;
import io.jsondb.crypto.ICipher;
import io.jsondb.tests.model.ComplexSite;
import io.jsondb.tests.model.Instance;

/**
 * Complex i.e nested queries
 *
 */
public class FindComplexQueryTests {

  private String dbFilesLocation = "src/test/resources/dbfiles/complexQueryTests";
  private File dbFilesFolder = new File(dbFilesLocation);
  private File complexSitesJson = new File(dbFilesFolder, "complexsite.json");

  private JsonDBTemplate jsonDBTemplate = null;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    dbFilesFolder.mkdir();
    Files.copy(new File("src/test/resources/dbfiles/complexsite.json"), complexSitesJson);
    jsonDBTemplate = new JsonDBTemplate(dbFilesLocation, "io.jsondb.tests.model", null);
  }

  @After
  public void tearDown() throws Exception {
    Util.delete(dbFilesFolder);
  }

  /**
   * test to find all documents for a collection type
   */
  @Test
  public void testFind_AllDocumentsForType() {
    String jxQuery = "."; //XPATH for all elements in a collection
    List<ComplexSite> complexSites = jsonDBTemplate.find(jxQuery, ComplexSite.class);
    assertEquals(11, complexSites.size());
  }

  /**
   * test to find one document by id
   */
  @Test
  public void testFind_OneDocumentById() {
    String jxQuery = String.format("/.[id='%s']", "005");
    List<ComplexSite> complexSites = jsonDBTemplate.find(jxQuery, ComplexSite.class);
    assertEquals(1, complexSites.size());
    ComplexSite site = complexSites.get(0);
    assertEquals("eclb-54-05", site.getLocation());
    assertNotNull(site.getAddress());
    assertEquals("First St", site.getAddress().getStreet());
  }

  /**
   * test to find one document by location
   */
  @Test
  public void testFind_OneDocumentByLocation() {
    String jxQuery = String.format("/.[location='%s']", "eclb-54-08");
    List<ComplexSite> complexSites = jsonDBTemplate.find(jxQuery, ComplexSite.class);
    assertEquals(1, complexSites.size());
    ComplexSite site = complexSites.get(0);
    assertEquals("008", site.getId());
    assertNotNull(site.getAddress());
    assertEquals("Sunnyvale", site.getAddress().getCity());
  }

  /**
   * test to find one ComplexSite by street
   */
  @Test
  public void testFind_OneComplexSiteByStreet() {
    String jxQuery = String.format("/./address[street='%s']/..", "Zanker St");
    List<ComplexSite> complexSites = jsonDBTemplate.find(jxQuery, ComplexSite.class);
    assertEquals(1, complexSites.size());
    ComplexSite site = complexSites.get(0);
    assertEquals("004", site.getId());
    assertNotNull(site.getAddress());
    assertEquals(35, site.getAddress().getNumber());
  }
 
  /**
   * test to find multiple ComplexSites by city
   */
  @Test
  public void testFind_OneComplexSiteByCity() {
    String jxQuery = String.format("/./address[city='%s']/..", "San Jose");
    List<ComplexSite> complexSites = jsonDBTemplate.find(jxQuery, ComplexSite.class);
    assertEquals(2, complexSites.size());
    ComplexSite site = complexSites.get(0);
    assertEquals("001", site.getId());
    assertNotNull(site.getAddress());
    assertEquals("Junction Ave", site.getAddress().getStreet());
    site = complexSites.get(1);
    assertEquals("004", site.getId());
    assertNotNull(site.getAddress());
    assertEquals("Zanker St", site.getAddress().getStreet());
  }

  /**
   * test to find one ComplexSite by Location and City
   */
  @Test
  public void testFind_OneComplexSiteByLocationAndCity() {
    String jxQuery = String.format("/.[location='%s']/address[city='%s']/..", "eclb-54-04", "San Jose");
    List<ComplexSite> complexSites = jsonDBTemplate.find(jxQuery, ComplexSite.class);
    assertEquals(1, complexSites.size());
    ComplexSite site = complexSites.get(0);
    assertEquals("004", site.getId());
    assertNotNull(site.getAddress());
    assertEquals("Zanker St", site.getAddress().getStreet());
  }

  /**
   * test to find one document by street
   */
  @Test
  public void testFind_OneAddressByStreet() {
    String jxQuery = String.format("/./address[street='%s']", "Zanker St");
    List<ComplexSite.Address> complexSitesAddrs = jsonDBTemplate.find(jxQuery, "complexsite");
    assertEquals(1, complexSitesAddrs.size());
    ComplexSite.Address addr = complexSitesAddrs.get(0);
    assertNotNull(addr);
    assertEquals("San Jose", addr.getCity());
  }
  
  /**
   * test to find a non-existent document
   */
  @Test
  public void testFind_DocumentThatDoesNotExist() {
    String jxQuery = String.format("/./address[street='%s']/..", "Badker St");
    List<ComplexSite> complexSites = jsonDBTemplate.find(jxQuery, ComplexSite.class);
    assertEquals(0, complexSites.size());
  }

  /**
   * test to find a non-existent document
   */
  @Test
  public void testFind_WithMap() {
    ComplexSite cSite = new ComplexSite();
    cSite.setId("200");
    cSite.setLocation("location200");
    ComplexSite.Address home = new ComplexSite.Address();
    home.setNumber(200);
    home.setStreet("Second St");
    home.setCity("HCity");
    cSite.getAllAddresses().put("home", home);
    ComplexSite.Address work = new ComplexSite.Address();
    work.setNumber(201);
    work.setStreet("Third St");
    work.setCity("Work City");
    cSite.getAllAddresses().put("work", work);
    jsonDBTemplate.insert(cSite);
    String jxQuery = String.format("/./allAddresses[@name='home'][city='HCity']");
    List<ComplexSite.Address> complexSitesAddrs = jsonDBTemplate.find(jxQuery, "complexsite");
    assertEquals(1, complexSitesAddrs.size());
    jxQuery = String.format("/./allAddresses[@name='home'][city='Work City']");
    complexSitesAddrs = jsonDBTemplate.find(jxQuery, "complexsite");
    assertEquals(0, complexSitesAddrs.size());
    jxQuery = String.format("/./allAddresses[@name='work'][city='Work City']");
    complexSitesAddrs = jsonDBTemplate.find(jxQuery, "complexsite");
    assertEquals(1, complexSitesAddrs.size());
  }

}

