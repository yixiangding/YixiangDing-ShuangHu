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
 * This class is the home for file permissions related commands.
 * Moved to this seperate class since FsShell is getting too large.
 */
package org.apache.hadoop.fs;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FsShell.CmdHandler;
import org.apache.hadoop.fs.permission.FsPermission;

private static class ChmodHandler extends CmdHandler {

    private short userMode, groupMode, othersMode;
    private char userType = '+', groupType = '+', othersType='+';

    private void applyNormalPattern(String modeStr, Matcher matcher)
                                    throws IOException {
      boolean commaSeperated = false;

      for(int i=0; i < 1 || matcher.end() < modeStr.length(); i++) {
        if (i>0 && (!commaSeperated || !matcher.find())) {
          patternError(modeStr);
        }

        /* groups : 1 : [ugoa]*
         *          2 : [+-=]
         *          3 : [rwxX]+
         *          4 : [,\s]*
         */

        String str = matcher.group(2);
        char type = str.charAt(str.length() - 1);

        boolean user, group, others;
        user = group = others = false;

        for(char c : matcher.group(1).toCharArray()) {
          switch (c) {
          case 'u' : user = true; break;
          case 'g' : group = true; break;
          case 'o' : others = true; break;
          case 'a' : break;
          default  : throw new RuntimeException("Unexpected");          
          }
        }

        if (!(user || group || others)) { // same as specifying 'a'
          user = group = others = true;
        }

        short  mode = 0;
        for(char c : matcher.group(3).toCharArray()) {
          switch (c) {
          case 'r' : mode |= 4; break;
          case 'w' : mode |= 2; break;
          case 'x' : mode |= 1; break;
          case 'X' : mode |= 8; break;
          default  : throw new RuntimeException("Unexpected");
          }
        }

        if ( user ) {
          userMode = mode;
          userType = type;
        }

        if ( group ) {
          groupMode = mode;
          groupType = type;
        }

        if ( others ) {
          othersMode = mode;
          othersType = type;
        }

        commaSeperated = matcher.group(4).contains(",");
      }
    }

    private void applyOctalPattern(String modeStr, Matcher matcher) {
      userType = groupType = othersType = '=';
      String str = matcher.group(1);
      userMode = Short.valueOf(str.substring(0, 1));
      groupMode = Short.valueOf(str.substring(1, 2));
      othersMode = Short.valueOf(str.substring(2, 3));      
    }

    private void patternError(String mode) throws IOException {
      throw new IOException("chmod : mode '" + mode + 
                            "' does not match the expected pattern.");      
    }

    ChmodHandler(FileSystem fs, String modeStr) throws IOException {
      super("chmod", fs);
      Matcher matcher = null;

      if ((matcher = chmodNormalPattern.matcher(modeStr)).find()) {
        applyNormalPattern(modeStr, matcher);
      } else if ((matcher = chmodOctalPattern.matcher(modeStr)).matches()) {
        applyOctalPattern(modeStr, matcher);
      } else {
        patternError(modeStr);
      }
    }

    private int applyChmod(char type, int mode, int existing, boolean exeOk) {
      boolean capX = false;

      if ((mode&8) != 0) { // convert X to x;
        capX = true;
        mode &= ~8;
        mode |= 1;
      }

      switch (type) {
      case '+' : mode = mode | existing; break;
      case '-' : mode = (~mode) & existing; break;
      case '=' : break;
      default  : throw new RuntimeException("Unexpected");      
      }

      // if X is specified add 'x' only if exeOk or x was already set.
      if (capX && !exeOk && (mode&1) != 0 && (existing&1) == 0) {
        mode &= ~1; // remove x
      }

      return mode;
    }

    @Override
    public void run(FileStatus file, FileSystem srcFs) throws IOException {
      FsPermission perms = file.getPermission();
      int existing = perms.toShort();
      boolean exeOk = file.isDir() || (existing & 0111) != 0;
      int newperms = ( applyChmod(userType, userMode, 
                                  (existing>>>6)&7, exeOk) << 6 |
                       applyChmod(groupType, groupMode, 
                                  (existing>>>3)&7, exeOk) << 3 |
                       applyChmod(othersType, othersMode, existing&7, exeOk) );

      if (existing != newperms) {
        try {
          srcFs.setPermission(file.getPath(), 
                                new FsPermission((short)newperms));
        } catch (IOException e) {
          System.err.println(getName() + ": changing permissions of '" + 
                             file.getPath() + "':" + e.getMessage());
        }
      }
    }
  }