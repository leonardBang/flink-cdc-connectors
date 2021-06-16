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

package com.alibaba.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import io.debezium.connector.mysql.legacy.BinlogReader.BinlogPosition;
import io.debezium.relational.TableId;

import java.io.IOException;
import java.util.Optional;

/**
 * A serializer for the {@link MySQLSplit}.
 */
public final class MySQLSplitSerializer implements SimpleVersionedSerializer<MySQLSplit> {

    private static final int VERSION = 1;

    public static final MySQLSplitSerializer INSTANCE = new MySQLSplitSerializer();

    private RowDataSerializer reusedBoundarySerializer;

    private MySQLSplitSerializer() {
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(MySQLSplit split) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(64);

        out.writeUTF(split.getTableId().toString());
        out.writeUTF(split.getSplitId());
        out.writeUTF(split.getSplitBoundaryType().asSerializableString());

        if (reusedBoundarySerializer == null) {
            reusedBoundarySerializer = new RowDataSerializer(split.getSplitBoundaryType());
        }
        final RowDataSerializer boundarySerializer = reusedBoundarySerializer;

        final Optional<RowData> splitBoundaryStart = split.getSplitBoundaryStart();
        final Optional<RowData> splitBoundaryEnd = split.getSplitBoundaryEnd();
        out.writeBoolean(splitBoundaryStart.isPresent());
        if (splitBoundaryStart.isPresent()) {
            boundarySerializer.serialize(splitBoundaryStart.get(), out);
        }
        out.writeBoolean(splitBoundaryEnd.isPresent());
        if (splitBoundaryEnd.isPresent()) {
            boundarySerializer.serialize(splitBoundaryEnd.get(), out);
        }
        out.writeBoolean(split.isSnapshotFinished());
        if (split.isSnapshotFinished()) {
            out.writeUTF(split.getLowWatermark().getFilename());
            out.writeLong(split.getLowWatermark().getPosition());
            out.writeUTF(split.getHighWatermark().getFilename());
            out.writeLong(split.getHighWatermark().getPosition());
        }

        if (split.getBinlogStartOffset() != null) {
            out.writeBoolean(true);
            out.writeUTF(split.getBinlogStartOffset().getFilename());
            out.writeLong(split.getBinlogStartOffset().getPosition());
        } else {
            out.writeBoolean(false);
        }

        return out.getCopyOfBuffer();
    }

    @Override
    public MySQLSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    public MySQLSplit deserializeV1(byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final TableId tableId = TableId.parse(in.readUTF());
        final String splitId = in.readUTF();
        final RowType rowType = (RowType) LogicalTypeParser.parse(in.readUTF());
        if (reusedBoundarySerializer == null) {
            reusedBoundarySerializer = new RowDataSerializer(rowType);
        }
        final RowDataSerializer boundarySerializer = reusedBoundarySerializer;
        final Optional<RowData> splitStart = in.readBoolean() ?
            Optional.of(boundarySerializer.deserialize(in)) : Optional.empty();
        final Optional<RowData> splitEnd = in.readBoolean() ?
            Optional.of(boundarySerializer.deserialize(in)) : Optional.empty();
        final Boolean snapshotFinished = in.readBoolean();
        final BinlogPosition lowWatermark = snapshotFinished ? new BinlogPosition(in.readUTF(), in.readLong()) : null;
        final BinlogPosition highWatermark = snapshotFinished ? new BinlogPosition(in.readUTF(), in.readLong()) : null;
        final BinlogPosition startOffset = in.readBoolean() ? new BinlogPosition(in.readUTF(), in.readLong()) : null;

        return new MySQLSplit(tableId, splitId, rowType, splitStart, splitEnd, snapshotFinished, lowWatermark, highWatermark, startOffset, null);

    }
}
