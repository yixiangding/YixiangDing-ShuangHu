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
 * Collection of upgrade objects.
 *
 * Upgrade objects should be registered here before they can be used. 
 */
package org.apache.hadoop.hdfs.server.common;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.StringUtils;

static class UOSignature implements Comparable<UOSignature> {
    int version;
    HdfsConstants.NodeType type;
    String className;

    UOSignature(Upgradeable uo) {
      this.version = uo.getVersion();
      this.type = uo.getType();
      this.className = uo.getClass().getCanonicalName();
    }

    int getVersion() {
      return version;
    }

    HdfsConstants.NodeType getType() {
      return type;
    }

    String getClassName() {
      return className;
    }

    Upgradeable instantiate() throws IOException {
      try {
        return (Upgradeable)Class.forName(getClassName()).newInstance();
      } catch(ClassNotFoundException e) {
        throw new IOException(StringUtils.stringifyException(e));
      } catch(InstantiationException e) {
        throw new IOException(StringUtils.stringifyException(e));
      } catch(IllegalAccessException e) {
        throw new IOException(StringUtils.stringifyException(e));
      }
    }

    public int compareTo(UOSignature o) {
      if(this.version != o.version)
        return (version < o.version ? -1 : 1);
      int res = this.getType().toString().compareTo(o.getType().toString());
      if(res != 0)
        return res;
      return className.compareTo(o.className);
    }

    public boolean equals(Object o) {
        if (!(o instanceof UOSignature)) {
          return false;
        }
        return this.compareTo((UOSignature)o) == 0;
      }

      public int hashCode() {
        return version ^ ((type==null)?0:type.hashCode()) 
                       ^ ((className==null)?0:className.hashCode());
      }
  }