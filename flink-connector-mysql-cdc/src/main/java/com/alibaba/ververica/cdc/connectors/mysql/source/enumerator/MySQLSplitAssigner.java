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


import com.alibaba.ververica.cdc.connectors.mysql.debezium.utils.StatementUtils;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitType;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.utils.RecordUtils.getPrimaryKeyFromRow;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.utils.RecordUtils.getSplitBoundary;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.utils.RecordUtils.rowToArray;
import static com.alibaba.ververica.cdc.connectors.mysql.debezium.utils.StatementUtils.buildMaxPrimaryKeyQuery;
import io.debezium.connector.mysql.MySqlJdbcContext;
import io.debezium.connector.mysql.MySqlSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.ColumnUtils;

/**
 * Split assigner that assign splits to readers.
 */
public class MySQLSplitAssigner {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSplitAssigner.class);
    private static final int maxSplitSize = 1024;

    private final MySqlJdbcContext jdbcContext;
    private final Set<TableId> capturedTables;
    private final Set<TableId> assignedTables;
    private final HashSet<MySQLSplit> recycleSplits;
    private final Map<TableId, MySQLSplit> assignedTableMaxSplit;
    private final Map<TableId, Object[]> tableMaxPrimaryKey;
    private final MySqlSchema databaseSchema;

    private TableId currentTableId;
    private Table currentTable;
    private JdbcConnection jdbc;

    public MySQLSplitAssigner(MySqlJdbcContext jdbcContext, Set<TableId> capturedTables, Set<TableId> assignedTables, HashSet<MySQLSplit> recycleSplits, Map<TableId, MySQLSplit> assignedTableMaxSplit, Map<TableId, Object[]> tableMaxPrimaryKey, TableId currentTableId, MySqlSchema databaseSchema) {
        this.jdbcContext = jdbcContext;
        this.capturedTables = capturedTables;
        this.assignedTables = assignedTables;
        this.recycleSplits = recycleSplits;
        this.assignedTableMaxSplit = assignedTableMaxSplit;
        this.tableMaxPrimaryKey = tableMaxPrimaryKey;
        this.databaseSchema = databaseSchema;
        this.currentTableId = currentTableId;
    }

    public void open() {
        this.jdbcContext.start();
        this.jdbc = jdbcContext.jdbc();
        if (currentTableId == null) {
            currentTableId = capturedTables.iterator().next();
        }
        this.currentTable = databaseSchema.tableFor(currentTableId);
    }

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    public Optional<MySQLSplit> getNext(@Nullable String hostname) {
        if (recycleSplits.iterator().hasNext()) {
            return Optional.of(recycleSplits.iterator().next());
        } else {
            MySQLSplit assignedMaxSplit = assignedTableMaxSplit.get(currentTableId);
            MySQLSplit nextSplit = getNextSplit(assignedMaxSplit);
            if (nextSplit != null) {
                assignedTableMaxSplit.put(currentTableId, nextSplit);
                return Optional.of(nextSplit);
            } else {
                // next table
                logger.info(String.format("The captured table %s have been assigned success.", currentTable));
                assignedTables.add(currentTableId);
                if (assignedTables.size() < capturedTables.size()) {
                    Set<TableId> remainingTables = capturedTables.stream().filter(assignedTables::contains).collect(Collectors.toSet());
                    currentTableId = remainingTables.iterator().next();
                    currentTable = databaseSchema.tableFor(currentTableId);
                    nextSplit = getNextSplit(null);
                    assignedTableMaxSplit.put(currentTableId, nextSplit);
                    return Optional.of(nextSplit);
                } else {
                    logger.info(String.format("All captured tables %s have been assigned success.", capturedTables));
                    close();
                    return Optional.empty();
                }
            }
        }
    }

    private MySQLSplit getNextSplit(MySQLSplit assignedMaxSplit) {
        List<Column> primaryKeyCols = currentTable.primaryKeyColumns();
        final boolean isFirstSplit = assignedMaxSplit == null;
        Object[] lastSplitEnd = null;
        if (isFirstSplit) {
            try {
                Object[] primaryKey = jdbc.queryAndMap(buildMaxPrimaryKeyQuery(currentTable), rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    return getPrimaryKeyFromRow(rowToArray(currentTable, databaseSchema, rs, ColumnUtils.toArray(rs, currentTable)), primaryKeyCols);
                });
                tableMaxPrimaryKey.put(currentTableId, primaryKey);
            } catch (SQLException e) {
                logger.error(String.format("Read max primary key from table %s failed.", currentTableId), e);
            }
        } else {
            lastSplitEnd = new ArrayList(assignedMaxSplit.getSplitEnd().values()).toArray();
        }
        Object[] maxiPrimaryKey = tableMaxPrimaryKey.get(currentTableId);
        Preconditions.checkState(maxiPrimaryKey != null);

        Object[] splitEnd = null;
        String splitBoundaryQuery = StatementUtils.buildSplitBoundaryQuery(currentTable, isFirstSplit, maxSplitSize);
        try (PreparedStatement statement = StatementUtils.readTableSplitStatement(jdbc, splitBoundaryQuery, isFirstSplit, maxiPrimaryKey, lastSplitEnd, 1);
             ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                return null;
            }
            splitEnd = getPrimaryKeyFromRow(rowToArray(currentTable, databaseSchema, rs, ColumnUtils.toArray(rs, currentTable)), primaryKeyCols);
        } catch (Exception e) {
            throw new IllegalStateException("Read split end of table  " + currentTableId + " failed", e);
        }
        return new MySQLSplit(MySQLSplitType.SNAPSHOT, currentTableId, UUID.randomUUID().toString(), getSplitBoundary(lastSplitEnd, primaryKeyCols),  getSplitBoundary(splitEnd, primaryKeyCols));
    }

    public void close() {
        if (jdbcContext != null) {
            jdbcContext.close();
        }
    }

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added, or when new splits got discovered.
     */
    public void addSplits(Collection<MySQLSplit> splits) {
        recycleSplits.addAll(splits);
    }


    /** Gets the remaining splits that this assigner has pending. */
    public Collection<MySQLSplit> remainingSplits() {
        return recycleSplits;
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

    public TableId getCurrentTableId() {
        return currentTableId;
    }
}
