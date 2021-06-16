/*
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

package com.alibaba.ververica.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import io.debezium.relational.TableId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer Serializer} for the enumerator
 * state of MySQL CDC source.
 */
public class MySQLSourceEnumStateSerializer implements SimpleVersionedSerializer<MySQLSourceEnumState> {

    private static final int VERSION = 1;
    private static final int VERSION_1_MAGIC_NUMBER = 0xDEADBEEF;

    private final SimpleVersionedSerializer<MySQLSplit> splitSerializer;

    public MySQLSourceEnumStateSerializer(SimpleVersionedSerializer<MySQLSplit> splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(MySQLSourceEnumState sourceEnumState) throws IOException {
        // optimization: the splits lazily cache their own serialized form
        if (sourceEnumState.serializedFormCache != null) {
            return sourceEnumState.serializedFormCache;
        }

        final SimpleVersionedSerializer<MySQLSplit> splitSerializer = this.splitSerializer; // stack cache
        final Collection<MySQLSplit> splits = sourceEnumState.getRemainingSplits();
        final Collection<TableId> tableIds = sourceEnumState.getAlreadyProcessedTables();

        final ArrayList<byte[]> serializedSplits = new ArrayList<>(splits.size());
        final ArrayList<byte[]> serializedTables = new ArrayList<>(tableIds.size());

        int totalLen =
            16; // four ints: magic, version of split serializer, count splits, count tables

        for (MySQLSplit split : splits) {
            final byte[] serSplit = splitSerializer.serialize(split);
            serializedSplits.add(serSplit);
            totalLen += serSplit.length + 4; // 4 bytes for the length field
        }

        for (TableId tableId : tableIds) {
            final byte[] serTableId = tableId.toString().getBytes(StandardCharsets.UTF_8);
            serializedTables.add(serTableId);
            totalLen += serTableId.length + 4; // 4 bytes for the length field
        }

        final byte[] result = new byte[totalLen];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(VERSION_1_MAGIC_NUMBER);
        byteBuffer.putInt(splitSerializer.getVersion());
        byteBuffer.putInt(serializedSplits.size());
        byteBuffer.putInt(serializedTables.size());

        for (byte[] splitBytes : serializedSplits) {
            byteBuffer.putInt(splitBytes.length);
            byteBuffer.put(splitBytes);
        }

        for (byte[] pathBytes : serializedTables) {
            byteBuffer.putInt(pathBytes.length);
            byteBuffer.put(pathBytes);
        }

        assert byteBuffer.remaining() == 0;

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        sourceEnumState.serializedFormCache = result;

        return result;
    }

    @Override
    public MySQLSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private MySQLSourceEnumState deserializeV1(byte[] serialized) throws IOException {
        final ByteBuffer bb = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        final int magic = bb.getInt();
        if (magic != VERSION_1_MAGIC_NUMBER) {
            throw new IOException(
                String.format(
                    "Invalid magic number for PendingSplitsCheckpoint. "
                        + "Expected: %X , found %X",
                    VERSION_1_MAGIC_NUMBER, magic));
        }

        final int splitSerializerVersion = bb.getInt();
        final int numSplits = bb.getInt();
        final int numTables = bb.getInt();

        final SimpleVersionedSerializer<MySQLSplit> splitSerializer = this.splitSerializer; // stack cache
        final ArrayList<MySQLSplit> splits = new ArrayList<>(numSplits);
        final ArrayList<TableId> tableIds = new ArrayList<>(numTables);

        for (int remaining = numSplits; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final MySQLSplit split = splitSerializer.deserialize(splitSerializerVersion, bytes);
            splits.add(split);
        }

        for (int remaining = numTables; remaining > 0; remaining--) {
            final byte[] bytes = new byte[bb.getInt()];
            bb.get(bytes);
            final TableId tableId = TableId.parse(new String(bytes, StandardCharsets.UTF_8));
            tableIds.add(tableId);
        }

        return MySQLSourceEnumState.reusingCollection(tableIds, splits);
    }
}
