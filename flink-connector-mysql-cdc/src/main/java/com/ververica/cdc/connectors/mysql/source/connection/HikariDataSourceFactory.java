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

package com.ververica.cdc.connectors.mysql.source.connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;

/** A connection pool Factory. */
public class HikariDataSourceFactory {

    public HikariDataSource createDataSource(JdbcConfiguration jdbcConfiguration) {
        HikariConfig config = new HikariConfig();

        String port = jdbcConfiguration.getString("database.port");
        String hostName = jdbcConfiguration.getString("database.hostname");

        config.setJdbcUrl("jdbc:mysql://" + hostName + ":" + port);
        config.setUsername(jdbcConfiguration.getString("database.user"));
        config.setPassword(jdbcConfiguration.getString("database.password"));
        config.setMaximumPoolSize(jdbcConfiguration.getInteger("connection.pool.size"));
        config.setConnectionTimeout(
                Long.parseLong(jdbcConfiguration.getString("connect.timeout.ms")));
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }
}
