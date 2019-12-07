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
 */
package org.apache.hadoop.record.compiler;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

class CppRecord extends CppCompType {
    
    private String fullName;
    private String name;
    private String module;
    private ArrayList<JField<CppType>> fields = 
      new ArrayList<JField<CppType>>();
    
    CppRecord(String name, ArrayList<JField<JType>> flist) {
      super(name.replaceAll("\\.","::"));
      this.fullName = name.replaceAll("\\.", "::");
      int idx = name.lastIndexOf('.');
      this.name = name.substring(idx+1);
      this.module = name.substring(0, idx).replaceAll("\\.", "::");
      for (Iterator<JField<JType>> iter = flist.iterator(); iter.hasNext();) {
        JField<JType> f = iter.next();
        fields.add(new JField<CppType>(f.getName(), f.getType().getCppType()));
      }
    }
    
    String getTypeIDObjectString() {
      return "new ::hadoop::StructTypeID(" + 
      fullName + "::getTypeInfo().getFieldTypeInfos())";
    }

    String genDecl(String fname) {
      return "  "+name+" "+fname+";\n";
    }
    
    void genSetRTIFilter(CodeBuffer cb) {
      // we set the RTI filter here
      cb.append(fullName + "::setTypeFilter(rti.getNestedStructTypeInfo(\""+
          name + "\"));\n");
    }

    void genSetupRTIFields(CodeBuffer cb) {
      cb.append("void " + fullName + "::setupRtiFields() {\n");
      cb.append("if (NULL == p" + Consts.RTI_FILTER + ") return;\n");
      cb.append("if (NULL != p" + Consts.RTI_FILTER_FIELDS + ") return;\n");
      cb.append("p" + Consts.RTI_FILTER_FIELDS + " = new int[p" + 
          Consts.RTI_FILTER + "->getFieldTypeInfos().size()];\n");
      cb.append("for (unsigned int " + Consts.RIO_PREFIX + "i=0; " + 
          Consts.RIO_PREFIX + "i<p" + Consts.RTI_FILTER + 
          "->getFieldTypeInfos().size(); " + Consts.RIO_PREFIX + "i++) {\n");
      cb.append("p" + Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + 
          "i] = 0;\n");
      cb.append("}\n");
      cb.append("for (unsigned int " + Consts.RIO_PREFIX + "i=0; " + 
          Consts.RIO_PREFIX + "i<p" + Consts.RTI_FILTER + 
          "->getFieldTypeInfos().size(); " + Consts.RIO_PREFIX + "i++) {\n");
      cb.append("for (unsigned int " + Consts.RIO_PREFIX + "j=0; " + 
          Consts.RIO_PREFIX + "j<p" + Consts.RTI_VAR + 
          "->getFieldTypeInfos().size(); " + Consts.RIO_PREFIX + "j++) {\n");
      cb.append("if (*(p" + Consts.RTI_FILTER + "->getFieldTypeInfos()[" + 
          Consts.RIO_PREFIX + "i]) == *(p" + Consts.RTI_VAR + 
          "->getFieldTypeInfos()[" + Consts.RIO_PREFIX + "j])) {\n");
      cb.append("p" + Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + 
          "i] = " + Consts.RIO_PREFIX + "j+1;\n");
      cb.append("break;\n");
      cb.append("}\n");
      cb.append("}\n");
      cb.append("}\n");
      cb.append("}\n");
    }
    
    void genCode(FileWriter hh, FileWriter cc, ArrayList<String> options)
      throws IOException {
      CodeBuffer hb = new CodeBuffer();
      
      String[] ns = module.split("::");
      for (int i = 0; i < ns.length; i++) {
        hb.append("namespace "+ns[i]+" {\n");
      }
      
      hb.append("class "+name+" : public ::hadoop::Record {\n");
      hb.append("private:\n");
      
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        type.genDecl(hb, name);
      }
      
      // type info vars
      hb.append("static ::hadoop::RecordTypeInfo* p" + Consts.RTI_VAR + ";\n");
      hb.append("static ::hadoop::RecordTypeInfo* p" + Consts.RTI_FILTER + ";\n");
      hb.append("static int* p" + Consts.RTI_FILTER_FIELDS + ";\n");
      hb.append("static ::hadoop::RecordTypeInfo* setupTypeInfo();\n");
      hb.append("static void setupRtiFields();\n");
      hb.append("virtual void deserializeWithoutFilter(::hadoop::IArchive& " + 
          Consts.RECORD_INPUT + ", const char* " + Consts.TAG + ");\n");
      hb.append("public:\n");
      hb.append("static const ::hadoop::RecordTypeInfo& getTypeInfo() " +
          "{return *p" + Consts.RTI_VAR + ";}\n");
      hb.append("static void setTypeFilter(const ::hadoop::RecordTypeInfo& rti);\n");
      hb.append("static void setTypeFilter(const ::hadoop::RecordTypeInfo* prti);\n");
      hb.append("virtual void serialize(::hadoop::OArchive& " + 
          Consts.RECORD_OUTPUT + ", const char* " + Consts.TAG + ") const;\n");
      hb.append("virtual void deserialize(::hadoop::IArchive& " + 
          Consts.RECORD_INPUT + ", const char* " + Consts.TAG + ");\n");
      hb.append("virtual const ::std::string& type() const;\n");
      hb.append("virtual const ::std::string& signature() const;\n");
      hb.append("virtual bool operator<(const "+name+"& peer_) const;\n");
      hb.append("virtual bool operator==(const "+name+"& peer_) const;\n");
      hb.append("virtual ~"+name+"() {};\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        type.genGetSet(hb, name);
      }
      hb.append("}; // end record "+name+"\n");
      for (int i=ns.length-1; i>=0; i--) {
        hb.append("} // end namespace "+ns[i]+"\n");
      }
      
      hh.write(hb.toString());
      
      CodeBuffer cb = new CodeBuffer();

      // initialize type info vars
      cb.append("::hadoop::RecordTypeInfo* " + fullName + "::p" + 
          Consts.RTI_VAR + " = " + fullName + "::setupTypeInfo();\n");
      cb.append("::hadoop::RecordTypeInfo* " + fullName + "::p" + 
          Consts.RTI_FILTER + " = NULL;\n");
      cb.append("int* " + fullName + "::p" + 
          Consts.RTI_FILTER_FIELDS + " = NULL;\n\n");

      // setupTypeInfo()
      cb.append("::hadoop::RecordTypeInfo* "+fullName+"::setupTypeInfo() {\n");
      cb.append("::hadoop::RecordTypeInfo* p = new ::hadoop::RecordTypeInfo(\"" + 
          name + "\");\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        type.genStaticTypeInfo(cb, name);
      }
      cb.append("return p;\n");
      cb.append("}\n");

      // setTypeFilter()
      cb.append("void "+fullName+"::setTypeFilter(const " +
          "::hadoop::RecordTypeInfo& rti) {\n");
      cb.append("if (NULL != p" + Consts.RTI_FILTER + ") {\n");
      cb.append("delete p" + Consts.RTI_FILTER + ";\n");
      cb.append("}\n");
      cb.append("p" + Consts.RTI_FILTER + " = new ::hadoop::RecordTypeInfo(rti);\n");
      cb.append("if (NULL != p" + Consts.RTI_FILTER_FIELDS + ") {\n");
      cb.append("delete p" + Consts.RTI_FILTER_FIELDS + ";\n");
      cb.append("}\n");
      cb.append("p" + Consts.RTI_FILTER_FIELDS + " = NULL;\n");
      // set RTIFilter for nested structs. We may end up with multiple lines that 
      // do the same thing, if the same struct is nested in more than one field, 
      // but that's OK. 
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        CppType type = jf.getType();
        type.genSetRTIFilter(cb);
      }
      cb.append("}\n");
      
      // setTypeFilter()
      cb.append("void "+fullName+"::setTypeFilter(const " +
          "::hadoop::RecordTypeInfo* prti) {\n");
      cb.append("if (NULL != prti) {\n");
      cb.append("setTypeFilter(*prti);\n");
      cb.append("}\n");
      cb.append("}\n");

      // setupRtiFields()
      genSetupRTIFields(cb);

      // serialize()
      cb.append("void "+fullName+"::serialize(::hadoop::OArchive& " + 
          Consts.RECORD_OUTPUT + ", const char* " + Consts.TAG + ") const {\n");
      cb.append(Consts.RECORD_OUTPUT + ".startRecord(*this," + 
          Consts.TAG + ");\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        if (type instanceof JBuffer.CppBuffer) {
          cb.append(Consts.RECORD_OUTPUT + ".serialize("+name+","+name+
              ".length(),\""+name+"\");\n");
        } else {
          cb.append(Consts.RECORD_OUTPUT + ".serialize("+name+",\""+
              name+"\");\n");
        }
      }
      cb.append(Consts.RECORD_OUTPUT + ".endRecord(*this," + Consts.TAG + ");\n");
      cb.append("return;\n");
      cb.append("}\n");
      
      // deserializeWithoutFilter()
      cb.append("void "+fullName+"::deserializeWithoutFilter(::hadoop::IArchive& " +
          Consts.RECORD_INPUT + ", const char* " + Consts.TAG + ") {\n");
      cb.append(Consts.RECORD_INPUT + ".startRecord(*this," + 
          Consts.TAG + ");\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        if (type instanceof JBuffer.CppBuffer) {
          cb.append("{\nsize_t len=0; " + Consts.RECORD_INPUT + ".deserialize("+
              name+",len,\""+name+"\");\n}\n");
        } else {
          cb.append(Consts.RECORD_INPUT + ".deserialize("+name+",\""+
              name+"\");\n");
        }
      }
      cb.append(Consts.RECORD_INPUT + ".endRecord(*this," + Consts.TAG + ");\n");
      cb.append("return;\n");
      cb.append("}\n");
      
      // deserialize()
      cb.append("void "+fullName+"::deserialize(::hadoop::IArchive& " +
          Consts.RECORD_INPUT + ", const char* " + Consts.TAG + ") {\n");
      cb.append("if (NULL == p" + Consts.RTI_FILTER + ") {\n");
      cb.append("deserializeWithoutFilter(" + Consts.RECORD_INPUT + ", " + 
          Consts.TAG + ");\n");
      cb.append("return;\n");
      cb.append("}\n");
      cb.append("// if we're here, we need to read based on version info\n");
      cb.append(Consts.RECORD_INPUT + ".startRecord(*this," + 
          Consts.TAG + ");\n");
      cb.append("setupRtiFields();\n");
      cb.append("for (unsigned int " + Consts.RIO_PREFIX + "i=0; " + 
          Consts.RIO_PREFIX + "i<p" + Consts.RTI_FILTER + 
          "->getFieldTypeInfos().size(); " + Consts.RIO_PREFIX + "i++) {\n");
      int ct = 0;
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        CppType type = jf.getType();
        ct++;
        if (1 != ct) {
          cb.append("else ");
        }
        cb.append("if (" + ct + " == p" + Consts.RTI_FILTER_FIELDS + "[" +
            Consts.RIO_PREFIX + "i]) {\n");
        if (type instanceof JBuffer.CppBuffer) {
          cb.append("{\nsize_t len=0; " + Consts.RECORD_INPUT + ".deserialize("+
              name+",len,\""+name+"\");\n}\n");
        } else {
          cb.append(Consts.RECORD_INPUT + ".deserialize("+name+",\""+
              name+"\");\n");
        }
        cb.append("}\n");
      }
      if (0 != ct) {
        cb.append("else {\n");
        cb.append("const std::vector< ::hadoop::FieldTypeInfo* >& typeInfos = p" + 
            Consts.RTI_FILTER + "->getFieldTypeInfos();\n");
        cb.append("::hadoop::Utils::skip(" + Consts.RECORD_INPUT + 
            ", typeInfos[" + Consts.RIO_PREFIX + "i]->getFieldID()->c_str()" + 
            ", *(typeInfos[" + Consts.RIO_PREFIX + "i]->getTypeID()));\n");
        cb.append("}\n");
      }
      cb.append("}\n");
      cb.append(Consts.RECORD_INPUT + ".endRecord(*this, " + Consts.TAG+");\n");
      cb.append("}\n");

      // operator <
      cb.append("bool "+fullName+"::operator< (const "+fullName+"& peer_) const {\n");
      cb.append("return (1\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        cb.append("&& ("+name+" < peer_."+name+")\n");
      }
      cb.append(");\n");
      cb.append("}\n");
      
      cb.append("bool "+fullName+"::operator== (const "+fullName+"& peer_) const {\n");
      cb.append("return (1\n");
      for (Iterator<JField<CppType>> i = fields.iterator(); i.hasNext();) {
        JField<CppType> jf = i.next();
        String name = jf.getName();
        cb.append("&& ("+name+" == peer_."+name+")\n");
      }
      cb.append(");\n");
      cb.append("}\n");
      
      cb.append("const ::std::string&"+fullName+"::type() const {\n");
      cb.append("static const ::std::string type_(\""+name+"\");\n");
      cb.append("return type_;\n");
      cb.append("}\n");
      
      cb.append("const ::std::string&"+fullName+"::signature() const {\n");
      cb.append("static const ::std::string sig_(\""+getSignature()+"\");\n");
      cb.append("return sig_;\n");
      cb.append("}\n");
      
      cc.write(cb.toString());
    }
  }