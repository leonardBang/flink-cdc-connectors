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

import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.debezium.relational.TableId;

/**
 * The state of MySQL CDC source enumerator.
 */
public class MySQLSourceEnumState {

    private final Set<TableId> capturedTables;
    private final Set<TableId> assignedTables;
    private final HashSet<MySQLSplit> recycleSplits;
    private final Map<TableId, MySQLSplit> assignedTableMaxSplit;
    private final Map<TableId, Object[]> tableMaxPrimaryKey;
    private final TableId currentTable;

    public MySQLSourceEnumState(Set<TableId> capturedTables, Set<TableId> assignedTables, HashSet<MySQLSplit> recycleSplits, Map<TableId, MySQLSplit> assignedTableMaxSplit, Map<TableId, Object[]> tableMaxPrimaryKey, TableId currentTable) {
        this.capturedTables = capturedTables;
        this.assignedTables = assignedTables;
        this.recycleSplits = recycleSplits;
        this.assignedTableMaxSplit = assignedTableMaxSplit;
        this.tableMaxPrimaryKey = tableMaxPrimaryKey;
        this.currentTable = currentTable;
    }

    public Set<TableId> getCapturedTables() {
        return capturedTables;
    }

    public Set<TableId> getAssignedTables() {
        return assignedTables;
    }

    public HashSet<MySQLSplit> getRecycleSplits() {
        return recycleSplits;
    }

    public Map<TableId, MySQLSplit> getAssignedTableMaxSplit() {
        return assignedTableMaxSplit;
    }

    public Map<TableId, Object[]> getTableMaxPrimaryKey() {
        return tableMaxPrimaryKey;
    }

    public TableId getCurrentTable() {
        return currentTable;
    }
}
