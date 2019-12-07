/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * XML Deserializer.
 */
package org.apache.hadoop.record;
import java.io.InputStream;
import java.io.IOException;
import java.util.ArrayList;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.parsers.SAXParser;

private class XMLParser extends DefaultHandler {
    private boolean charsValid = false;
        
    private ArrayList<Value> valList;
        
    private XMLParser(ArrayList<Value> vlist) {
      valList = vlist;
    }
        
    public void startDocument() throws SAXException {}
        
    public void endDocument() throws SAXException {}
        
    public void startElement(String ns,
                             String sname,
                             String qname,
                             Attributes attrs) throws SAXException {
      charsValid = false;
      if ("boolean".equals(qname) ||
          "i4".equals(qname) ||
          "int".equals(qname) ||
          "string".equals(qname) ||
          "double".equals(qname) ||
          "ex:i1".equals(qname) ||
          "ex:i8".equals(qname) ||
          "ex:float".equals(qname)) {
        charsValid = true;
        valList.add(new Value(qname));
      } else if ("struct".equals(qname) ||
                 "array".equals(qname)) {
        valList.add(new Value(qname));
      }
    }
        
    public void endElement(String ns,
                           String sname,
                           String qname) throws SAXException {
      charsValid = false;
      if ("struct".equals(qname) ||
          "array".equals(qname)) {
        valList.add(new Value("/"+qname));
      }
    }
        
    public void characters(char buf[], int offset, int len)
      throws SAXException {
      if (charsValid) {
        Value v = valList.get(valList.size()-1);
        v.addChars(buf, offset, len);
      }
    }
        
  }