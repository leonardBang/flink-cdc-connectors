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

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import io.debezium.connector.mysql.legacy.BinlogReader.BinlogPosition;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * The split of table comes from a Table that splits by primary key.
 */
public class MySQLSplit implements SourceSplit {

    private final TableId tableId;
    private final String splitId;
    private final RowType splitBoundaryType;
    private final Optional<RowData> splitBoundaryStart;
    private final Optional<RowData> splitBoundaryEnd;

    private final boolean snapshotFinished;
    private BinlogPosition lowWatermark = null;
    private BinlogPosition highWatermark = null;
    private BinlogPosition binlogStartOffset = null;

    /**
     * The splits are frequently serialized into checkpoints. Caching the byte representation makes
     * repeated serialization cheap. This field is used by {@link MySQLSplitSerializer}.
     */
    @Nullable
    transient byte[] serializedFormCache;


    public MySQLSplit(TableId tableId, String splitId, RowType splitBoundaryType, Optional<RowData> splitBoundaryStart, Optional<RowData> splitBoundaryEnd,
                      @Nullable byte[] serializedFormCache) {
        this.tableId = tableId;
        this.splitId = splitId;
        this.splitBoundaryType = splitBoundaryType;
        this.splitBoundaryStart = splitBoundaryStart;
        this.splitBoundaryEnd = splitBoundaryEnd;
        Preconditions.checkState(!splitBoundaryStart.isPresent() && !splitBoundaryEnd.isPresent(),
            "The splitBoundaryStart and splitBoundaryEnd cannot both be empty");
        this.serializedFormCache = serializedFormCache;
        this.snapshotFinished = false;
    }

    public MySQLSplit(
        TableId tableId,
        String splitId,
        RowType splitBoundaryType,
        Optional<RowData> splitBoundaryStart,
        Optional<RowData> splitBoundaryEnd,
        boolean snapshotFinished,
        BinlogPosition lowWatermark,
        BinlogPosition highWatermark,
        BinlogPosition binlogStartOffset,
        @Nullable byte[] serializedFormCache) {
        this.tableId = tableId;
        this.splitId = splitId;
        this.splitBoundaryType = splitBoundaryType;
        this.splitBoundaryStart = splitBoundaryStart;
        this.splitBoundaryEnd = splitBoundaryEnd;
        this.snapshotFinished = snapshotFinished;
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
        this.binlogStartOffset = binlogStartOffset;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getSplitId() {
        return splitId;
    }

    public RowType getSplitBoundaryType() {
        return splitBoundaryType;
    }

    public Optional<RowData> getSplitBoundaryStart() {
        return splitBoundaryStart;
    }

    public Optional<RowData> getSplitBoundaryEnd() {
        return splitBoundaryEnd;
    }

    @Nullable
    public byte[] getSerializedFormCache() {
        return serializedFormCache;
    }

    public boolean isSnapshotFinished() {
        return snapshotFinished;
    }

    public BinlogPosition getLowWatermark() {
        return lowWatermark;
    }

    public BinlogPosition getHighWatermark() {
        return highWatermark;
    }

    public BinlogPosition getBinlogStartOffset() {
        return binlogStartOffset;
    }

    @Override
    public String toString() {
        return "MySQLSplit{" +
            "tableId=" + tableId +
            ", splitId='" + splitId + '\'' +
            ", splitBoundaryType=" + splitBoundaryType +
            ", splitBoundaryStart=" + splitBoundaryStart +
            ", splitBoundaryEnd=" + splitBoundaryEnd +
            ", snapshotFinished=" + snapshotFinished +
            ", lowWatermark=" + lowWatermark +
            ", highWatermark=" + highWatermark +
            ", binlogStartOffset=" + binlogStartOffset +
            '}';
    }
}
