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
 * The Chain class provides all the common functionality for the
 * {@link ChainMapper} and the {@link ChainReducer} classes.
 */
package org.apache.hadoop.mapred.lib;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Stringifier;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.GenericsUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

private class ChainOutputCollector<K, V> implements OutputCollector<K, V> {
    private int nextMapperIndex;
    private Serialization<K> keySerialization;
    private Serialization<V> valueSerialization;
    private OutputCollector output;
    private Reporter reporter;

    /*
     * Constructor for Mappers
     */
    public ChainOutputCollector(int index, Serialization<K> keySerialization,
                                Serialization<V> valueSerialization,
                                OutputCollector output, Reporter reporter) {
      this.nextMapperIndex = index + 1;
      this.keySerialization = keySerialization;
      this.valueSerialization = valueSerialization;
      this.output = output;
      this.reporter = reporter;
    }

    /*
     * Constructor for Reducer
     */
    public ChainOutputCollector(Serialization<K> keySerialization,
                                Serialization<V> valueSerialization,
                                OutputCollector output, Reporter reporter) {
      this.nextMapperIndex = 0;
      this.keySerialization = keySerialization;
      this.valueSerialization = valueSerialization;
      this.output = output;
      this.reporter = reporter;
    }

    @SuppressWarnings({"unchecked"})
    public void collect(K key, V value) throws IOException {
      if (nextMapperIndex < mappers.size()) {
        // there is a next mapper in chain

        // only need to ser/deser if there is next mapper in the chain
        if (keySerialization != null) {
          key = makeCopyForPassByValue(keySerialization, key);
          value = makeCopyForPassByValue(valueSerialization, value);
        }

        // gets ser/deser and mapper of next in chain
        Serialization nextKeySerialization =
          mappersKeySerialization.get(nextMapperIndex);
        Serialization nextValueSerialization =
          mappersValueSerialization.get(nextMapperIndex);
        Mapper nextMapper = mappers.get(nextMapperIndex);

        // invokes next mapper in chain
        nextMapper.map(key, value,
                       new ChainOutputCollector(nextMapperIndex,
                                                nextKeySerialization,
                                                nextValueSerialization,
                                                output, reporter),
                       reporter);
      } else {
        // end of chain, user real output collector
        output.collect(key, value);
      }
    }

    private <E> E makeCopyForPassByValue(Serialization<E> serialization,
                                          E obj) throws IOException {
      Serializer<E> ser =
        serialization.getSerializer(GenericsUtil.getClass(obj));
      Deserializer<E> deser =
        serialization.getDeserializer(GenericsUtil.getClass(obj));

      DataOutputBuffer dof = threadLocalDataOutputBuffer.get();

      dof.reset();
      ser.open(dof);
      ser.serialize(obj);
      ser.close();
      obj = ReflectionUtils.newInstance(GenericsUtil.getClass(obj),
                                        getChainJobConf());
      ByteArrayInputStream bais =
        new ByteArrayInputStream(dof.getData(), 0, dof.getLength());
      deser.open(bais);
      deser.deserialize(obj);
      deser.close();
      return obj;
    }

  }