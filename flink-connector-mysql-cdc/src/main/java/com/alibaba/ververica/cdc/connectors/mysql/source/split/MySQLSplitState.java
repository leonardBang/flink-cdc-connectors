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

import io.debezium.connector.mysql.legacy.BinlogReader.BinlogPosition;

/**
 * State of the reader, essentially a mutable version of the {@link MySQLSplit}. It has a
 * modifiable split status and split offset (i.e. BinlogPosition).
 */
public final class MySQLSplitState extends MySQLSplit {

    private boolean splitScanFinished;
    private BinlogPosition lowWatermark;
    private BinlogPosition highWatermark;
    private BinlogPosition currentOffset;

    public MySQLSplitState(MySQLSplit mySQLSplit) {
        super(
            mySQLSplit.getTableId(),
            mySQLSplit.getSplitId(),
            mySQLSplit.getSplitBoundaryType(),
            mySQLSplit.getSplitBoundaryStart(),
            mySQLSplit.getSplitBoundaryEnd(),
            mySQLSplit.getSerializedFormCache());
        this.splitScanFinished = mySQLSplit.isSnapshotFinished();
        this.lowWatermark = mySQLSplit.getLowWatermark();
        this.highWatermark = mySQLSplit.getHighWatermark();
        this.currentOffset = mySQLSplit.getBinlogStartOffset();
    }

    public void setSplitScanFinished(boolean snapshotFinished) {
        this.splitScanFinished = snapshotFinished;
    }

    public boolean isSplitScanFinished() {
        return splitScanFinished;
    }

    @Override
    public BinlogPosition getLowWatermark() {
        return lowWatermark;
    }

    public void setLowWatermark(BinlogPosition lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    @Override
    public BinlogPosition getHighWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(BinlogPosition highWatermark) {
        this.highWatermark = highWatermark;
    }

    public BinlogPosition getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(BinlogPosition currentOffset) {
        this.currentOffset = currentOffset;
    }

    /** Use the current split state to create a new MySQLSplit. */
    public MySQLSplit toMySQLSplit() {
        return new MySQLSplit(
            getTableId(),
            getSplitId(),
            getSplitBoundaryType(),
            getSplitBoundaryStart(),
            getSplitBoundaryEnd(),
            isSplitScanFinished(),
            getLowWatermark(),
            getHighWatermark(),
            getCurrentOffset(),
            null);
    }
}
