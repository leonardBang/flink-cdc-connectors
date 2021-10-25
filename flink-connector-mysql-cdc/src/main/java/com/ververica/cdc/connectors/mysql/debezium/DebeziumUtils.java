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

package com.ververica.cdc.connectors.mysql.debezium;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.source.connection.MySqlConnectionFactory;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext.toDebeziumConfig;
import static com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.listTables;

/** Utilities related to Debezium. */
public class DebeziumUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumUtils.class);

    /**
     * Creates {@link RelationalTableFilters} from configuration. The {@link RelationalTableFilters}
     * can be used to filter tables according to "table.whitelist" and "database.whitelist" options.
     */
    public static RelationalTableFilters createTableFilters(Configuration configuration) {
        io.debezium.config.Configuration debeziumConfig = toDebeziumConfig(configuration);
        final MySqlConnectorConfig mySqlConnectorConfig = new MySqlConnectorConfig(debeziumConfig);
        return mySqlConnectorConfig.getTableFilters();
    }

    /** Creates and opens a new {@link JdbcConnection} backing connection pool. */
    public static JdbcConnection createJdbcConnection(Configuration configuration) {
        JdbcConnection jdbc =
                new JdbcConnection(toDebeziumConfig(configuration), new MySqlConnectionFactory());
        try {
            jdbc.connect();
        } catch (Exception e) {
            LOG.error("Failed to open MySQL connection", e);
            throw new FlinkRuntimeException("Failed to open MySQL connection", e);
        }

        LOG.info("Open MySQL connection {} success", jdbc);
        return jdbc;
    }

    /** Creates and opens a new {@link MySqlConnection}. */
    public static MySqlConnection openMySqlConnection(Configuration configuration) {
        MySqlConnection jdbc =
                new MySqlConnection(
                        new MySqlConnection.MySqlConnectionConfiguration(
                                toDebeziumConfig(configuration)));
        try {
            jdbc.connect();
        } catch (Exception e) {
            LOG.error("Failed to open MySQL connection", e);
            closeMySqlConnection(jdbc);
            throw new FlinkRuntimeException("Failed to open MySQL connection", e);
        }

        LOG.info("Open MySQL connection {} success", jdbc);
        return jdbc;
    }

    /** Closes the given {@link MySqlConnection}. */
    public static void closeMySqlConnection(MySqlConnection jdbc) {
        String connectionObjName = "";
        try {
            if (jdbc != null) {
                connectionObjName = jdbc.toString();
                jdbc.close();
            }
        } catch (SQLException e) {
            LOG.error("Failed to close MySQL connection", e);
            throw new FlinkRuntimeException("Failed to close MySQL connection", e);
        }
        LOG.info("Close MySQL connection {} success", connectionObjName);
    }

    /** Fetch current binlog offsets in MySql Server. */
    public static BinlogOffset currentBinlogOffset(MySqlConnection jdbc) {
        final String showMasterStmt = "SHOW MASTER STATUS";
        try {
            return jdbc.queryAndMap(
                    showMasterStmt,
                    rs -> {
                        if (rs.next()) {
                            final String binlogFilename = rs.getString(1);
                            final long binlogPosition = rs.getLong(2);
                            final String gtidSet =
                                    rs.getMetaData().getColumnCount() > 4 ? rs.getString(5) : null;
                            return new BinlogOffset(
                                    binlogFilename, binlogPosition, 0L, 0, 0, gtidSet, null);
                        } else {
                            throw new FlinkRuntimeException(
                                    "Cannot read the binlog filename and position via '"
                                            + showMasterStmt
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    "Cannot read the binlog filename and position via '"
                            + showMasterStmt
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }

    public static List<TableId> discoverCapturedTables(
            JdbcConnection jdbc, Configuration configuration) {

        final List<TableId> capturedTableIds;
        try {
            capturedTableIds = listTables(jdbc, createTableFilters(configuration));
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to discover captured tables", e);
        }
        if (capturedTableIds.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can't find any matched tables, please check your configured database-name: %s and table-name: %s",
                            configuration.getString("database.name", null),
                            configuration.getString("table.name", null)));
        }
        return capturedTableIds;
    }

    public static boolean isTableIdCaseSensitive(JdbcConnection connection) {
        return !"0"
                .equals(
                        readMySqlSystemVariables(connection)
                                .get(MySqlSystemVariables.LOWER_CASE_TABLE_NAMES));
    }

    public static Map<String, String> readMySqlSystemVariables(JdbcConnection connection) {
        // Read the system variables from the MySQL instance and get the current database name ...
        return querySystemVariables(connection, "SHOW VARIABLES");
    }

    private static Map<String, String> querySystemVariables(
            JdbcConnection connection, String statement) {
        final Map<String, String> variables = new HashMap<>();
        try {
            connection.query(
                    statement,
                    rs -> {
                        while (rs.next()) {
                            String varName = rs.getString(1);
                            String value = rs.getString(2);
                            if (varName != null && value != null) {
                                variables.put(varName, value);
                            }
                        }
                    });
        } catch (SQLException e) {
            throw new DebeziumException("Error reading MySQL variables: " + e.getMessage(), e);
        }

        return variables;
    }
}
