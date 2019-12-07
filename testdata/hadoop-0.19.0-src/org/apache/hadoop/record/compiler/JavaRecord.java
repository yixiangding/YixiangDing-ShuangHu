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

class JavaRecord extends JavaCompType {
    
    private String fullName;
    private String name;
    private String module;
    private ArrayList<JField<JavaType>> fields =
      new ArrayList<JField<JavaType>>();
    
    JavaRecord(String name, ArrayList<JField<JType>> flist) {
      super(name, "Record", name, "TypeID.RIOType.STRUCT");
      this.fullName = name;
      int idx = name.lastIndexOf('.');
      this.name = name.substring(idx+1);
      this.module = name.substring(0, idx);
      for (Iterator<JField<JType>> iter = flist.iterator(); iter.hasNext();) {
        JField<JType> f = iter.next();
        fields.add(new JField<JavaType>(f.getName(), f.getType().getJavaType()));
      }
    }
    
    String getTypeIDObjectString() {
      return "new org.apache.hadoop.record.meta.StructTypeID(" + 
      fullName + ".getTypeInfo())";
    }

    void genSetRTIFilter(CodeBuffer cb, Map<String, Integer> nestedStructMap) {
      // ignore, if we'ev already set the type filter for this record
      if (!nestedStructMap.containsKey(fullName)) {
        // we set the RTI filter here
        cb.append(fullName + ".setTypeFilter(rti.getNestedStructTypeInfo(\""+
            name + "\"));\n");
        nestedStructMap.put(fullName, null);
      }
    }

    // for each typeInfo in the filter, we see if there's a similar one in the record. 
    // Since we store typeInfos in ArrayLists, thsi search is O(n squared). We do it faster
    // if we also store a map (of TypeInfo to index), but since setupRtiFields() is called
    // only once when deserializing, we're sticking with the former, as the code is easier.  
    void genSetupRtiFields(CodeBuffer cb) {
      cb.append("private static void setupRtiFields()\n{\n");
      cb.append("if (null == " + Consts.RTI_FILTER + ") return;\n");
      cb.append("// we may already have done this\n");
      cb.append("if (null != " + Consts.RTI_FILTER_FIELDS + ") return;\n");
      cb.append("int " + Consts.RIO_PREFIX + "i, " + Consts.RIO_PREFIX + "j;\n");
      cb.append(Consts.RTI_FILTER_FIELDS + " = new int [" + 
          Consts.RIO_PREFIX + "rtiFilter.getFieldTypeInfos().size()];\n");
      cb.append("for (" + Consts.RIO_PREFIX + "i=0; " + Consts.RIO_PREFIX + "i<"+
          Consts.RTI_FILTER_FIELDS + ".length; " + Consts.RIO_PREFIX + "i++) {\n");
      cb.append(Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + "i] = 0;\n");
      cb.append("}\n");
      cb.append("java.util.Iterator<org.apache.hadoop.record.meta." +
          "FieldTypeInfo> " + Consts.RIO_PREFIX + "itFilter = " + 
          Consts.RIO_PREFIX + "rtiFilter.getFieldTypeInfos().iterator();\n");
      cb.append(Consts.RIO_PREFIX + "i=0;\n");
      cb.append("while (" + Consts.RIO_PREFIX + "itFilter.hasNext()) {\n");
      cb.append("org.apache.hadoop.record.meta.FieldTypeInfo " + 
          Consts.RIO_PREFIX + "tInfoFilter = " + 
          Consts.RIO_PREFIX + "itFilter.next();\n");
      cb.append("java.util.Iterator<org.apache.hadoop.record.meta." + 
          "FieldTypeInfo> " + Consts.RIO_PREFIX + "it = " + Consts.RTI_VAR + 
          ".getFieldTypeInfos().iterator();\n");
      cb.append(Consts.RIO_PREFIX + "j=1;\n");
      cb.append("while (" + Consts.RIO_PREFIX + "it.hasNext()) {\n");
      cb.append("org.apache.hadoop.record.meta.FieldTypeInfo " + 
          Consts.RIO_PREFIX + "tInfo = " + Consts.RIO_PREFIX + "it.next();\n");
      cb.append("if (" + Consts.RIO_PREFIX + "tInfo.equals(" +  
          Consts.RIO_PREFIX + "tInfoFilter)) {\n");
      cb.append(Consts.RTI_FILTER_FIELDS + "[" + Consts.RIO_PREFIX + "i] = " +
          Consts.RIO_PREFIX + "j;\n");
      cb.append("break;\n");
      cb.append("}\n");
      cb.append(Consts.RIO_PREFIX + "j++;\n");
      cb.append("}\n");
      /*int ct = 0;
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        ct++;
        JField<JavaType> jf = i.next();
        JavaType type = jf.getType();
        String name = jf.getName();
        if (ct != 1) {
          cb.append("else ");
        }
        type.genRtiFieldCondition(cb, name, ct);
      }
      if (ct != 0) {
        cb.append("else {\n");
        cb.append("rtiFilterFields[i] = 0;\n");
        cb.append("}\n");
      }*/
      cb.append(Consts.RIO_PREFIX + "i++;\n");
      cb.append("}\n");
      cb.append("}\n");
    }

    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(fullName+" "+fname+";\n");
      }
      cb.append(fname+"= new "+fullName+"();\n");
      cb.append(fname+".deserialize(" + Consts.RECORD_INPUT + ",\""+tag+"\");\n");
    }
    
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      cb.append(fname+".serialize(" + Consts.RECORD_OUTPUT + ",\""+tag+"\");\n");
    }
    
    void genSlurpBytes(CodeBuffer cb, String b, String s, String l) {
      cb.append("{\n");
      cb.append("int r = "+fullName+
                ".Comparator.slurpRaw("+b+","+s+","+l+");\n");
      cb.append(s+"+=r; "+l+"-=r;\n");
      cb.append("}\n");
    }
    
    void genCompareBytes(CodeBuffer cb) {
      cb.append("{\n");
      cb.append("int r1 = "+fullName+
                ".Comparator.compareRaw(b1,s1,l1,b2,s2,l2);\n");
      cb.append("if (r1 <= 0) { return r1; }\n");
      cb.append("s1+=r1; s2+=r1; l1-=r1; l2-=r1;\n");
      cb.append("}\n");
    }
    
    void genCode(String destDir, ArrayList<String> options) throws IOException {
      String pkg = module;
      String pkgpath = pkg.replaceAll("\\.", "/");
      File pkgdir = new File(destDir, pkgpath);
      if (!pkgdir.exists()) {
        // create the pkg directory
        boolean ret = pkgdir.mkdirs();
        if (!ret) {
          throw new IOException("Cannnot create directory: "+pkgpath);
        }
      } else if (!pkgdir.isDirectory()) {
        // not a directory
        throw new IOException(pkgpath+" is not a directory.");
      }
      File jfile = new File(pkgdir, name+".java");
      FileWriter jj = new FileWriter(jfile);
      
      CodeBuffer cb = new CodeBuffer();
      cb.append("// File generated by hadoop record compiler. Do not edit.\n");
      cb.append("package "+module+";\n\n");
      cb.append("public class "+name+
                " extends org.apache.hadoop.record.Record {\n");
      
      // type information declarations
      cb.append("private static final " + 
          "org.apache.hadoop.record.meta.RecordTypeInfo " + 
          Consts.RTI_VAR + ";\n");
      cb.append("private static " + 
          "org.apache.hadoop.record.meta.RecordTypeInfo " + 
          Consts.RTI_FILTER + ";\n");
      cb.append("private static int[] " + Consts.RTI_FILTER_FIELDS + ";\n");
      
      // static init for type information
      cb.append("static {\n");
      cb.append(Consts.RTI_VAR + " = " +
          "new org.apache.hadoop.record.meta.RecordTypeInfo(\"" +
          name + "\");\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genStaticTypeInfo(cb, name);
      }
      cb.append("}\n\n");

      // field definitions
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genDecl(cb, name);
      }

      // default constructor
      cb.append("public "+name+"() { }\n");
      
      // constructor
      cb.append("public "+name+"(\n");
      int fIdx = 0;
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext(); fIdx++) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genConstructorParam(cb, name);
        cb.append((!i.hasNext())?"":",\n");
      }
      cb.append(") {\n");
      fIdx = 0;
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext(); fIdx++) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genConstructorSet(cb, name);
      }
      cb.append("}\n");

      // getter/setter for type info
      cb.append("public static org.apache.hadoop.record.meta.RecordTypeInfo"
              + " getTypeInfo() {\n");
      cb.append("return " + Consts.RTI_VAR + ";\n");
      cb.append("}\n");
      cb.append("public static void setTypeFilter("
          + "org.apache.hadoop.record.meta.RecordTypeInfo rti) {\n");
      cb.append("if (null == rti) return;\n");
      cb.append(Consts.RTI_FILTER + " = rti;\n");
      cb.append(Consts.RTI_FILTER_FIELDS + " = null;\n");
      // set RTIFilter for nested structs.
      // To prevent setting up the type filter for the same struct more than once, 
      // we use a hash map to keep track of what we've set. 
      Map<String, Integer> nestedStructMap = new HashMap<String, Integer>();
      for (JField<JavaType> jf : fields) {
        JavaType type = jf.getType();
        type.genSetRTIFilter(cb, nestedStructMap);
      }
      cb.append("}\n");

      // setupRtiFields()
      genSetupRtiFields(cb);

      // getters/setters for member variables
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genGetSet(cb, name);
      }
      
      // serialize()
      cb.append("public void serialize("+ 
          "final org.apache.hadoop.record.RecordOutput " + 
          Consts.RECORD_OUTPUT + ", final String " + Consts.TAG + ")\n"+
                "throws java.io.IOException {\n");
      cb.append(Consts.RECORD_OUTPUT + ".startRecord(this," + Consts.TAG + ");\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genWriteMethod(cb, name, name);
      }
      cb.append(Consts.RECORD_OUTPUT + ".endRecord(this," + Consts.TAG+");\n");
      cb.append("}\n");

      // deserializeWithoutFilter()
      cb.append("private void deserializeWithoutFilter("+
                "final org.apache.hadoop.record.RecordInput " + 
                Consts.RECORD_INPUT + ", final String " + Consts.TAG + ")\n"+
                "throws java.io.IOException {\n");
      cb.append(Consts.RECORD_INPUT + ".startRecord(" + Consts.TAG + ");\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genReadMethod(cb, name, name, false);
      }
      cb.append(Consts.RECORD_INPUT + ".endRecord(" + Consts.TAG+");\n");
      cb.append("}\n");
      
      // deserialize()
      cb.append("public void deserialize(final " +
          "org.apache.hadoop.record.RecordInput " + 
          Consts.RECORD_INPUT + ", final String " + Consts.TAG + ")\n"+
          "throws java.io.IOException {\n");
      cb.append("if (null == " + Consts.RTI_FILTER + ") {\n");
      cb.append("deserializeWithoutFilter(" + Consts.RECORD_INPUT + ", " + 
          Consts.TAG + ");\n");
      cb.append("return;\n");
      cb.append("}\n");
      cb.append("// if we're here, we need to read based on version info\n");
      cb.append(Consts.RECORD_INPUT + ".startRecord(" + Consts.TAG + ");\n");
      cb.append("setupRtiFields();\n");
      cb.append("for (int " + Consts.RIO_PREFIX + "i=0; " + Consts.RIO_PREFIX + 
          "i<" + Consts.RTI_FILTER + ".getFieldTypeInfos().size(); " + 
          Consts.RIO_PREFIX + "i++) {\n");
      int ct = 0;
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        ct++;
        if (1 != ct) {
          cb.append("else ");
        }
        cb.append("if (" + ct + " == " + Consts.RTI_FILTER_FIELDS + "[" +
            Consts.RIO_PREFIX + "i]) {\n");
        type.genReadMethod(cb, name, name, false);
        cb.append("}\n");
      }
      if (0 != ct) {
        cb.append("else {\n");
        cb.append("java.util.ArrayList<"
                + "org.apache.hadoop.record.meta.FieldTypeInfo> typeInfos = "
                + "(java.util.ArrayList<"
                + "org.apache.hadoop.record.meta.FieldTypeInfo>)"
                + "(" + Consts.RTI_FILTER + ".getFieldTypeInfos());\n");
        cb.append("org.apache.hadoop.record.meta.Utils.skip(" + 
            Consts.RECORD_INPUT + ", " + "typeInfos.get(" + Consts.RIO_PREFIX + 
            "i).getFieldID(), typeInfos.get(" + 
            Consts.RIO_PREFIX + "i).getTypeID());\n");
        cb.append("}\n");
      }
      cb.append("}\n");
      cb.append(Consts.RECORD_INPUT + ".endRecord(" + Consts.TAG+");\n");
      cb.append("}\n");

      // compareTo()
      cb.append("public int compareTo (final Object " + Consts.RIO_PREFIX + 
          "peer_) throws ClassCastException {\n");
      cb.append("if (!(" + Consts.RIO_PREFIX + "peer_ instanceof "+name+")) {\n");
      cb.append("throw new ClassCastException(\"Comparing different types of records.\");\n");
      cb.append("}\n");
      cb.append(name+" " + Consts.RIO_PREFIX + "peer = ("+name+") " + 
          Consts.RIO_PREFIX + "peer_;\n");
      cb.append("int " + Consts.RIO_PREFIX + "ret = 0;\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genCompareTo(cb, name, Consts.RIO_PREFIX + "peer."+name);
        cb.append("if (" + Consts.RIO_PREFIX + "ret != 0) return " + 
            Consts.RIO_PREFIX + "ret;\n");
      }
      cb.append("return " + Consts.RIO_PREFIX + "ret;\n");
      cb.append("}\n");
      
      // equals()
      cb.append("public boolean equals(final Object " + Consts.RIO_PREFIX + 
          "peer_) {\n");
      cb.append("if (!(" + Consts.RIO_PREFIX + "peer_ instanceof "+name+")) {\n");
      cb.append("return false;\n");
      cb.append("}\n");
      cb.append("if (" + Consts.RIO_PREFIX + "peer_ == this) {\n");
      cb.append("return true;\n");
      cb.append("}\n");
      cb.append(name+" " + Consts.RIO_PREFIX + "peer = ("+name+") " + 
          Consts.RIO_PREFIX + "peer_;\n");
      cb.append("boolean " + Consts.RIO_PREFIX + "ret = false;\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genEquals(cb, name, Consts.RIO_PREFIX + "peer."+name);
        cb.append("if (!" + Consts.RIO_PREFIX + "ret) return " + 
            Consts.RIO_PREFIX + "ret;\n");
      }
      cb.append("return " + Consts.RIO_PREFIX + "ret;\n");
      cb.append("}\n");

      // clone()
      cb.append("public Object clone() throws CloneNotSupportedException {\n");
      cb.append(name+" " + Consts.RIO_PREFIX + "other = new "+name+"();\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genClone(cb, name);
      }
      cb.append("return " + Consts.RIO_PREFIX + "other;\n");
      cb.append("}\n");
      
      cb.append("public int hashCode() {\n");
      cb.append("int " + Consts.RIO_PREFIX + "result = 17;\n");
      cb.append("int " + Consts.RIO_PREFIX + "ret;\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genHashCode(cb, name);
        cb.append(Consts.RIO_PREFIX + "result = 37*" + Consts.RIO_PREFIX + 
            "result + " + Consts.RIO_PREFIX + "ret;\n");
      }
      cb.append("return " + Consts.RIO_PREFIX + "result;\n");
      cb.append("}\n");
      
      cb.append("public static String signature() {\n");
      cb.append("return \""+getSignature()+"\";\n");
      cb.append("}\n");
      
      cb.append("public static class Comparator extends"+
                " org.apache.hadoop.record.RecordComparator {\n");
      cb.append("public Comparator() {\n");
      cb.append("super("+name+".class);\n");
      cb.append("}\n");
      
      cb.append("static public int slurpRaw(byte[] b, int s, int l) {\n");
      cb.append("try {\n");
      cb.append("int os = s;\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genSlurpBytes(cb, "b","s","l");
      }
      cb.append("return (os - s);\n");
      cb.append("} catch(java.io.IOException e) {\n");
      cb.append("throw new RuntimeException(e);\n");
      cb.append("}\n");
      cb.append("}\n");
      
      cb.append("static public int compareRaw(byte[] b1, int s1, int l1,\n");
      cb.append("                             byte[] b2, int s2, int l2) {\n");
      cb.append("try {\n");
      cb.append("int os1 = s1;\n");
      for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
        JField<JavaType> jf = i.next();
        String name = jf.getName();
        JavaType type = jf.getType();
        type.genCompareBytes(cb);
      }
      cb.append("return (os1 - s1);\n");
      cb.append("} catch(java.io.IOException e) {\n");
      cb.append("throw new RuntimeException(e);\n");
      cb.append("}\n");
      cb.append("}\n");
      cb.append("public int compare(byte[] b1, int s1, int l1,\n");
      cb.append("                   byte[] b2, int s2, int l2) {\n");
      cb.append("int ret = compareRaw(b1,s1,l1,b2,s2,l2);\n");
      cb.append("return (ret == -1)? -1 : ((ret==0)? 1 : 0);");
      cb.append("}\n");
      cb.append("}\n\n");
      cb.append("static {\n");
      cb.append("org.apache.hadoop.record.RecordComparator.define("
                +name+".class, new Comparator());\n");
      cb.append("}\n");
      cb.append("}\n");

      jj.write(cb.toString());
      jj.close();
    }
  }