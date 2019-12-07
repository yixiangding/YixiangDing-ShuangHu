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
 * Abstract Base class for all types supported by Hadoop Record I/O.
 */
package org.apache.hadoop.record.compiler;
import java.util.Map;

abstract class CppType {
    private String name;
    
    CppType(String cppname) {
      name = cppname;
    }
    
    void genDecl(CodeBuffer cb, String fname) {
      cb.append(name+" "+fname+";\n");
    }
    
    void genStaticTypeInfo(CodeBuffer cb, String fname) {
      cb.append("p->addField(new ::std::string(\"" + 
          fname + "\"), " + getTypeIDObjectString() + ");\n");
    }
    
    void genGetSet(CodeBuffer cb, String fname) {
      cb.append("virtual "+name+" get"+toCamelCase(fname)+"() const {\n");
      cb.append("return "+fname+";\n");
      cb.append("}\n");
      cb.append("virtual void set"+toCamelCase(fname)+"("+name+" m_) {\n");
      cb.append(fname+"=m_;\n");
      cb.append("}\n");
    }
    
    abstract String getTypeIDObjectString();

    void genSetRTIFilter(CodeBuffer cb) {
      // do nothing by default
      return;
    }

    String getType() {
      return name;
    }
  }