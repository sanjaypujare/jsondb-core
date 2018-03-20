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
package io.jsondb.tests.model;

import io.jsondb.annotation.Document;
import io.jsondb.annotation.Id;

/**
 * A test Pojo representing a AWS Site.
 * Used to test JsonDB - createCollection() and dropCollection() operations
 * @version 1.0 06-Oct-2016
 */
@Document(collection = "complexsite", schemaVersion= "1.0")
public class ComplexSite {
  public static class Address {
    private int number;
    private String street;
    private String city;

    public int getNumber()
    {
      return number;
    }
    public void setNumber(int number)
    {
      this.number = number;
    }
    public String getStreet()
    {
      return street;
    }
    public void setStreet(String street)
    {
      this.street = street;
    }
    public String getCity()
    {
      return city;
    }
    public void setCity(String city)
    {
      this.city = city;
    }
  }
  
  
  @Id
  private String id;
  private String location;
  private Address address;
  public String getId() {
    return id;
  }
  public void setId(String id) {
    this.id = id;
  }
  public String getLocation() {
    return location;
  }
  public void setLocation(String location) {
    this.location = location;
  }
  public Address getAddress()
  {
    return address;
  }
  public void setAddress(Address address)
  {
    this.address = address;
  }
}
