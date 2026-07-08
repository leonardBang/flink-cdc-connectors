/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.api.TableException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Java-serialization based state serializer for split/enumerator state. */
class ElasticsearchSourceStateSerializer<T> implements SimpleVersionedSerializer<T> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream objectOut = new ObjectOutputStream(out)) {
            objectOut.writeObject(obj);
            objectOut.flush();
            return out.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new TableException(
                    String.format(
                            "Unsupported Elasticsearch source state serializer version %s, expected %s.",
                            version, VERSION));
        }
        try (ByteArrayInputStream in = new ByteArrayInputStream(serialized);
                ObjectInputStream objectIn = new ObjectInputStream(in)) {
            return (T) objectIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize Elasticsearch source state.", e);
        }
    }
}
