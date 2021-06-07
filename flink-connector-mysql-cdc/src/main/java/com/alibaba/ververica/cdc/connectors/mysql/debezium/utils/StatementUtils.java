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

package com.alibaba.ververica.cdc.connectors.mysql.debezium.utils;

import org.apache.flink.table.data.TimestampData;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Utils to prepare SQL statement.
 */
public class StatementUtils {

    public static String buildSplitBoundaryQuery(Table table, boolean isFirstSplit, int maxSplitSize) {
        return buildSplitQuery(table, isFirstSplit, 1, true);
    }

    public static String buildSplitScanQuery(Table table, boolean isFirstSplit, int maxSplitSize) {
        return buildSplitQuery(table, isFirstSplit, maxSplitSize, false);
    }

    private static String buildSplitQuery(Table table, boolean isFirstSplit, int limitSize, boolean onlyScanBoundary) {
        String condition = null;
        if (!isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            // Window boundaries
            addPrimaryKeyColumnsToCondition(table, sql, " >= ?");
            sql.append(" AND NOT (");
            addPrimaryKeyColumnsToCondition(table, sql, " = ?");
            sql.append(")");
            // Table boundaries
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(table, sql, " <= ?");
            condition = sql.toString();
        }
        final String orderBy = table.primaryKeyColumns().stream()
            .map(Column::name)
            .collect(Collectors.joining(", "));
        if(onlyScanBoundary) {
            return buildSelectWithRowLimits(table.id(),
                limitSize,
                getPrimaryKeyColumnsProjection(table),
                Optional.ofNullable(condition),
                orderBy);
        } else {
            return buildSelectWithRowLimits(table.id(),
                limitSize,
                "*",
                Optional.ofNullable(condition),
                orderBy);
        }
    }

    public static PreparedStatement readTableSplitStatement(JdbcConnection jdbc, String sql, boolean isFirstSplit, Object[] maxiPrimaryKey, Object[] lastSplitEnd, int fetchSize) throws SQLException {
        final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
        if (!isFirstSplit) {
            for (int i = 0; i < lastSplitEnd.length; i++) {
                statement.setObject(i + 1, lastSplitEnd[i]);
                statement.setObject(i + 1 + lastSplitEnd.length, lastSplitEnd[i]);
                statement.setObject(i + 1 + 2 * lastSplitEnd.length, maxiPrimaryKey[i]);
            }
        }
        return statement;
    }

    public static String buildMaxPrimaryKeyQuery(Table table) {
        final String orderBy = table.primaryKeyColumns().stream()
            .map(Column::name)
            .collect(Collectors.joining(" DESC, ")) + " DESC";
        return buildSelectWithRowLimits(table.id(), 1, "*", Optional.empty(), orderBy.toString());
    }

    private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize) throws SQLException {
        final PreparedStatement statement = jdbc.connection().prepareStatement(sql);
        statement.setFetchSize(fetchSize);
        return statement;
    }


    private static void addPrimaryKeyColumnsToCondition(Table table, StringBuilder sql, String predicate) {
        for (Iterator<Column> i = table.primaryKeyColumns().iterator(); i.hasNext();) {
            final Column key = i.next();
            sql.append(key.name()).append(predicate);
            if (i.hasNext()) {
                sql.append(" AND ");
            }
        }
    }

    private static String getPrimaryKeyColumnsProjection(Table table) {
        StringBuilder sql = new StringBuilder();
        for (Iterator<Column> i = table.primaryKeyColumns().iterator(); i.hasNext();) {
            final Column key = i.next();
            sql.append("MAX(" + key.name() + ")");
            if (i.hasNext()) {
                sql.append(" , ");
            }
        }
        return sql.toString();
    }

    private static String buildSelectWithRowLimits(TableId tableId, int limit, String projection, Optional<String> condition, String orderBy) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql
            .append(projection)
            .append(" FROM ");
        sql.append(quotedTableIdString(tableId));
        if (condition.isPresent()) {
            sql
                .append(" WHERE ")
                .append(condition.get());
        }
        sql
            .append(" ORDER BY ")
            .append(orderBy)
            .append(" LIMIT ")
            .append(limit);
        return sql.toString();
    }

    private static String quotedTableIdString(TableId tableId) {
        return tableId.toDoubleQuotedString();
    }
}
