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
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import io.debezium.relational.TableId;

/**
 * The split of table comes from a Table that splits by primary key.
 */
public final class MySQLSplit implements SourceSplit, Comparable<MySQLSplit>, Serializable {

    private static final long serialVersionUID = 1L;
    private final MySQLSplitType mySQLSplitType;
    private final Long assignTimestamp;
    private final TableId tableId;
    private final String splitId;
    private final Map<String, Object> splitStart;
    private final Map<String, Object> splitEnd;

    public MySQLSplit(MySQLSplitType mySQLSplitType, TableId tableId, String splitId, @Nullable Map<String, Object> splitStart, @Nullable Map<String, Object> splitEnd) {
        this(mySQLSplitType, System.currentTimeMillis(), tableId, splitId, splitStart, splitEnd);

    }

    public MySQLSplit(MySQLSplitType mySQLSplitType, long assignTimestamp, TableId tableId, String splitId, Map<String, Object> splitStart, Map<String, Object> splitEnd) {
        this.mySQLSplitType = mySQLSplitType;
        this.assignTimestamp = assignTimestamp;
        this.tableId = tableId;
        this.splitId = splitId;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        Preconditions.checkState(splitStart == null && splitEnd == null,
            "The split start and split end cannot both be null. ");
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public TableId getTableId() {
        return tableId;
    }

    public MySQLSplitType getMySQLSplitType() {
        return mySQLSplitType;
    }

    public boolean isLastSnapshotSplit(){
        return mySQLSplitType == MySQLSplitType.SNAPSHOT && splitEnd == null;
    }

    public Map<String, Object> getSplitStart() {
        return splitStart;
    }

    public Map<String, Object> getSplitEnd() {
        return splitEnd;
    }

    @Override
    public int compareTo(MySQLSplit o) {
        return this.assignTimestamp.compareTo(o.assignTimestamp);
    }

    @Override
    public String toString() {
        return "MySQLSplit{" +
            "mySQLSplitType=" + mySQLSplitType +
            ", assignTimestamp=" + assignTimestamp +
            ", tableId=" + tableId +
            ", splitId='" + splitId + '\'' +
            ", splitStart=" + splitStart +
            ", splitEnd=" + splitEnd +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySQLSplit)) {
            return false;
        }
        MySQLSplit that = (MySQLSplit) o;
        return mySQLSplitType == that.mySQLSplitType &&
            Objects.equals(assignTimestamp, that.assignTimestamp) &&
            Objects.equals(tableId, that.tableId) &&
            Objects.equals(splitId, that.splitId) &&
            Objects.equals(splitStart, that.splitStart) &&
            Objects.equals(splitEnd, that.splitEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mySQLSplitType, assignTimestamp, tableId, splitId, splitStart, splitEnd);
    }
}
