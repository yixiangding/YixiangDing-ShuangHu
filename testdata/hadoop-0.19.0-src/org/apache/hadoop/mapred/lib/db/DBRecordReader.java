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
 * A InputFormat that reads input data from an SQL table.
 * <p>
 * DBInputFormat emits LongWritables containing the record number as 
 * key and DBWritables as value. 
 * 
 * The SQL query, and input class can be using one of the two 
 * setInput methods.
 */
package org.apache.hadoop.mapred.lib.db;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

protected class DBRecordReader implements
  RecordReader<LongWritable, T> {
    private ResultSet results;

    private Statement statement;

    private Class<T> inputClass;

    private JobConf job;

    private DBInputSplit split;

    private long pos = 0;

    /**
     * @param split The InputSplit to read data for
     * @throws SQLException 
     */
    protected DBRecordReader(DBInputSplit split, Class<T> inputClass, JobConf job) throws SQLException {
      this.inputClass = inputClass;
      this.split = split;
      this.job = job;
      
      statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      //statement.setFetchSize(Integer.MIN_VALUE);
      results = statement.executeQuery(getSelectQuery());
    }

    /** Returns the query for selecting the records, 
     * subclasses can override this for custom behaviour.*/
    protected String getSelectQuery() {
      StringBuilder query = new StringBuilder();
      
      if(dbConf.getInputQuery() == null) {
        query.append("SELECT ");

        for (int i = 0; i < fieldNames.length; i++) {
          query.append(fieldNames[i]);
          if(i != fieldNames.length -1) {
            query.append(", ");
          }
        }

        query.append(" FROM ").append(tableName);
        query.append(" AS ").append(tableName); //in hsqldb this is necessary
        if (conditions != null && conditions.length() > 0)
          query.append(" WHERE (").append(conditions).append(")");
        String orderBy = dbConf.getInputOrderBy();
        if(orderBy != null && orderBy.length() > 0) {
          query.append(" ORDER BY ").append(orderBy);
        }
      }
      else {
        query.append(dbConf.getInputQuery());
      }

      try {
        query.append(" LIMIT ").append(split.getLength());
        query.append(" OFFSET ").append(split.getStart());
      }
      catch (IOException ex) {
        //ignore, will not throw
      }
      return query.toString();
    }

    /** {@inheritDoc} */
    public void close() throws IOException {
      try {
        connection.commit();
        results.close();
        statement.close();
      } catch (SQLException e) {
        throw new IOException(e.getMessage());
      }
    }

    /** {@inheritDoc} */
    public LongWritable createKey() {
      return new LongWritable();  
    }

    /** {@inheritDoc} */
    public T createValue() {
      return ReflectionUtils.newInstance(inputClass, job);
    }

    /** {@inheritDoc} */
    public long getPos() throws IOException {
      return pos;
    }

    /** {@inheritDoc} */
    public float getProgress() throws IOException {
      return pos / (float)split.getLength();
    }

    /** {@inheritDoc} */
    public boolean next(LongWritable key, T value) throws IOException {
      try {
        if (!results.next())
          return false;

        // Set the key field value as the output key value
        key.set(pos + split.getStart());

        value.readFields(results);

        pos ++;
      } catch (SQLException e) {
        throw new IOException(e.getMessage());
      }
      return true;
    }
  }