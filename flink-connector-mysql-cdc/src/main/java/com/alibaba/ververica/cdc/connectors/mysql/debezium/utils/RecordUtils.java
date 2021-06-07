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

import org.apache.flink.util.Preconditions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.connector.mysql.MySqlSchema;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.ColumnUtils;

/**
 * Utility class to deal record.
 */
public class RecordUtils {

    public static Object[] getPrimaryKeyFromRow(Object[] row, List<Column> primaryKeyCols) {
        if (row == null) {
            return null;
        }
        final Object[] primaryKey = new Object[primaryKeyCols.size()];
        for (int i = 0; i < primaryKeyCols.size(); i++) {
            primaryKey[i] = row[primaryKeyCols.get(i).position() - 1];
        }
        return primaryKey;
    }


    /**
     * Converts a {@link ResultSet} row to an array of Objects
     */
    public static Object[] rowToArray(Table table, MySqlSchema databaseSchema, ResultSet rs,
                                      ColumnUtils.ColumnArray columnArray)
        throws SQLException {
        final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
        for (int i = 0; i < columnArray.getColumns().length; i++) {
            row[columnArray.getColumns()[i].position() - 1] = rs.getObject( i + 1);
        }
        return row;
    }

    /**
     * Gets the split boundary which constructs by primary key.
     */
    public static Map<String, Object> getSplitBoundary(Object[] primaryKey, List<Column> primaryKeyCols) {
        Map<String, Object> boundary = new HashMap<>();
        for (int i = 0; i < primaryKeyCols.size(); i++) {
            Object val = null;
            if (primaryKey != null) {
                val = primaryKey[i];
            }
            boundary.put(primaryKeyCols.get(i).name(), val);
        }
        return boundary;
    }
}
