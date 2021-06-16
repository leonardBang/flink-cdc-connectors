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

package com.alibaba.ververica.cdc.connectors.mysql.source.assigner;


import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.cdc.connectors.mysql.converter.MySQLJdbcConverter;
import com.alibaba.ververica.cdc.connectors.mysql.source.MySQLSourceOptions;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.legacy.Filters;
import io.debezium.connector.mysql.legacy.MySqlJdbcContext;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.alibaba.ververica.cdc.connectors.mysql.utils.RecordUtils.getSplitBoundary;
import static com.alibaba.ververica.cdc.connectors.mysql.utils.RecordUtils.rowToArray;
import static com.alibaba.ververica.cdc.connectors.mysql.utils.StatementUtils.buildMaxPrimaryKeyQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.utils.StatementUtils.buildSplitBoundaryQuery;
import static com.alibaba.ververica.cdc.connectors.mysql.utils.StatementUtils.quote;
import static com.alibaba.ververica.cdc.connectors.mysql.utils.StatementUtils.readTableSplitStatement;


/**
 * Split assigner that assign splits to readers.
 */
public class MySQLSplitAssigner {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSplitAssigner.class);

    private final Collection<TableId> alreadyProcessedTables;
    private final Collection<MySQLSplit> remainingSplits;
    private final RowType pkRowType;
    private final Properties properties;
    private final int splitSize;

    private Collection<TableId> capturedTables;
    private MySQLJdbcConverter splitBoundaryConverter;
    private TableId currentTableId;
    private int currentTableSplitSeq;
    private Object[] currentTableMaxPrimaryKey;
    private Filters tableFilters;
    private JdbcConnection jdbc;


    public MySQLSplitAssigner(Properties properties, RowType pkRowType, Collection<TableId> alreadyProcessedTables, Collection<MySQLSplit> remainingSplits) {
        this.properties = properties;
        this.pkRowType = pkRowType;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.splitSize = Integer.valueOf(properties.getProperty(MySQLSourceOptions.SCAN_SPLIT_SIZE.key()));
    }

    public void open() {
        initJdbcConnection();
        this.tableFilters = getTableFilters();
        this.capturedTables = getCapturedTables();
        this.currentTableSplitSeq = 0;
    }

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    public Optional<MySQLSplit> getNext(@Nullable String hostname) {

        if (remainingSplits.size() > 0) {
            // return remaining splits firstly
            MySQLSplit split = remainingSplits.iterator().next();
            remainingSplits.remove(split);
            if (remainingSplits.size() == 0) {
                this.alreadyProcessedTables.add(currentTableId);
            }
            return Optional.of(split);
        } else {
            // it's turn for new table
            TableId nextTable = getNextTable();
            if (nextTable != null) {
                currentTableId = nextTable;
                currentTableSplitSeq = 0;
                getAllSplitForCurrentTable();
                return getNext(hostname);
            } else {
                logger.info(String.format("All captured tables %s have been assigned success.", capturedTables));
                close();
                return Optional.empty();
            }
        }
    }

    private TableId getNextTable() {
        for (TableId tableId : capturedTables) {
            if (!alreadyProcessedTables.contains(tableId)) {
                return tableId;
            }
        }
        return null;
    }

    public MySQLJdbcConverter getSplitBoundaryConverter() {
        if (this.splitBoundaryConverter == null) {
            this.splitBoundaryConverter = new MySQLJdbcConverter(this.pkRowType);
        }
        return this.splitBoundaryConverter;
    }

    private void getAllSplitForCurrentTable() {
        MySQLSplit firstSplit = getNextSplit(null);
        if (firstSplit != null) {
            MySQLSplit currentSplit = firstSplit;
            remainingSplits.add(firstSplit);

            MySQLSplit nextSplit;
            while ((nextSplit = getNextSplit(currentSplit)) != null && !splitBoundaryConverter.toJdbcFields(nextSplit.getSplitBoundaryStart().get()).equals(currentTableMaxPrimaryKey)) {
                remainingSplits.add(nextSplit);
                currentSplit = nextSplit;
            }
            logger.info(String.format("The captured table %s have been assigned success.", currentTableId));
        }
    }

    private MySQLSplit getNextSplit(MySQLSplit prevSplit) {
        boolean isFirstSplit = prevSplit == null;
        // first split
        Object[] prevSplitEnd;
        if (isFirstSplit) {
            try {
                Object[] primaryKey = jdbc.queryAndMap(buildMaxPrimaryKeyQuery(currentTableId, pkRowType), rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    return rowToArray(rs, pkRowType.getFieldCount());
                });
                currentTableMaxPrimaryKey = primaryKey;
            } catch (SQLException e) {
                logger.error(String.format("Read max primary key from table %s failed.", currentTableId), e);
            }
            prevSplitEnd = null;
        } else {
            prevSplitEnd = splitBoundaryConverter.toJdbcFields(prevSplit.getSplitBoundaryEnd().get());
        }

        String splitBoundaryQuery = buildSplitBoundaryQuery(currentTableId, pkRowType, isFirstSplit, splitSize);
        try (PreparedStatement statement = readTableSplitStatement(jdbc, splitBoundaryQuery, isFirstSplit, currentTableMaxPrimaryKey, prevSplitEnd, 1);
             ResultSet rs = statement.executeQuery()) {
            if (!rs.next()) {
                return null;
            }
            Object[] splitEnd = rowToArray(rs, pkRowType.getFieldCount());
            return new MySQLSplit(currentTableId, createSplitId(), pkRowType, getSplitBoundary(prevSplitEnd, getSplitBoundaryConverter()), getSplitBoundary(splitEnd, getSplitBoundaryConverter()), null);
        } catch (Exception e) {
            throw new IllegalStateException("Read split end of table  " + currentTableId + " failed", e);
        }
    }

    private String createSplitId() {
        return currentTableId + "-" + currentTableSplitSeq;
    }

    public void close() {
        if (jdbc != null) {
            try {
                jdbc.close();
            } catch (SQLException e) {
                logger.error("Close jdbc connection error", e);
            }
        }
    }

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added, or when new splits got discovered.
     */
    public void addSplits(Collection<MySQLSplit> splits) {
        remainingSplits.addAll(splits);
    }


    public Collection<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    /**
     * Gets the remaining splits that this assigner has pending.
     */
    public Collection<MySQLSplit> remainingSplits() {
        return remainingSplits;
    }


    private Collection<TableId> getCapturedTables() {
        final List<TableId> capturedTableIds = new ArrayList<>();
        try {
            logger.info("Read list of available databases。");
            final List<String> databaseNames = new ArrayList<>();

            jdbc.query("SHOW DATABASES", rs -> {
                while (rs.next()) {
                    databaseNames.add(rs.getString(1));
                }
            });
            logger.info("The list of available databases is: {}", databaseNames);

            logger.info("Read list of available tables in each database");

            for (String dbName : databaseNames) {
                jdbc.query("SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'", rs -> {
                    while (rs.next()) {
                        TableId id = new TableId(dbName, null, rs.getString(1));
                        if (tableFilters.tableFilter().test(id)) {
                            capturedTableIds.add(id);
                            logger.info("Add table '{}' to capture", id);
                        } else {
                            logger.info("Table '{}' is filtered out of capturing", id);
                        }
                    }
                });
            }
        } catch (Exception e) {
            logger.error("Obtain available tables fail.", e);
            this.close();
        }
        return capturedTableIds;
    }

    private Filters getTableFilters() {
        final Map<String, Object> props = new HashMap<>();
        if (properties.contains(MySQLSourceOptions.DATABASE_NAME.key())) {
            props.put("database.whitelist", properties.getProperty(MySQLSourceOptions.DATABASE_NAME.key()));
        }
        if (properties.contains(MySQLSourceOptions.TABLE_NAME.key())) {
            props.put("table.whitelist", properties.getProperty(MySQLSourceOptions.TABLE_NAME.key()));
        }
        final Configuration filterConf = Configuration.from(props);

        return new Filters.Builder(filterConf).build();
    }

    private void initJdbcConnection() {
        final MySqlJdbcContext jdbcContext =
            new MySqlJdbcContext(new MySqlConnectorConfig(Configuration.from(properties)));
        jdbcContext.start();
        this.jdbc = jdbcContext.jdbc();
    }
}
