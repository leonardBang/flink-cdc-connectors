/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.source;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.table.api.ValidationException;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.HOSTS;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.ID_COLUMN;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.INDICES;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.PIT_KEEP_ALIVE;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.POLL_INTERVAL;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.REQUEST_TIMEOUT;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.SCAN_INCREMENTAL_FIELD;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.SCAN_PAGE_SIZE;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.SCAN_TIEBREAKER_FIELD;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.SNAPSHOT_ENABLED;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.SOFT_DELETE_FIELD;
import static org.apache.flink.cdc.connectors.elasticsearch.source.ElasticsearchDataSourceOptions.USERNAME;

/** Factory for creating {@link ElasticsearchDataSource}. */
public class ElasticsearchDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "elasticsearch";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validate();
        Configuration config = context.getFactoryConfiguration();

        int pageSize = config.get(SCAN_PAGE_SIZE);
        validatePositive(SCAN_PAGE_SIZE.key(), pageSize);
        validatePositiveDuration(PIT_KEEP_ALIVE.key(), config.get(PIT_KEEP_ALIVE));
        validatePositiveDuration(POLL_INTERVAL.key(), config.get(POLL_INTERVAL));
        validatePositiveDuration(REQUEST_TIMEOUT.key(), config.get(REQUEST_TIMEOUT));

        ElasticsearchSourceConfig sourceConfig =
                new ElasticsearchSourceConfig(
                        splitAndTrim(config.get(HOSTS)),
                        splitAndTrim(config.get(INDICES)),
                        config.get(USERNAME),
                        config.get(PASSWORD),
                        config.get(SCAN_INCREMENTAL_FIELD),
                        config.get(SCAN_TIEBREAKER_FIELD),
                        config.get(ID_COLUMN),
                        config.get(SOFT_DELETE_FIELD),
                        pageSize,
                        config.get(PIT_KEEP_ALIVE),
                        config.get(POLL_INTERVAL),
                        config.get(REQUEST_TIMEOUT),
                        config.get(SNAPSHOT_ENABLED));
        return new ElasticsearchDataSource(sourceConfig);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOSTS);
        requiredOptions.add(INDICES);
        requiredOptions.add(SCAN_INCREMENTAL_FIELD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SCAN_TIEBREAKER_FIELD);
        optionalOptions.add(ID_COLUMN);
        optionalOptions.add(SOFT_DELETE_FIELD);
        optionalOptions.add(SCAN_PAGE_SIZE);
        optionalOptions.add(PIT_KEEP_ALIVE);
        optionalOptions.add(POLL_INTERVAL);
        optionalOptions.add(REQUEST_TIMEOUT);
        optionalOptions.add(SNAPSHOT_ENABLED);
        return optionalOptions;
    }

    private static List<String> splitAndTrim(String value) {
        List<String> values =
                Arrays.stream(value.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
        if (values.isEmpty()) {
            throw new ValidationException("Option value must contain at least one entry.");
        }
        return values;
    }

    private static void validatePositive(String option, int value) {
        if (value <= 0) {
            throw new ValidationException(
                    String.format("Option %s must be positive, but was %s.", option, value));
        }
    }

    private static void validatePositiveDuration(String option, Duration value) {
        if (value.isZero() || value.isNegative()) {
            throw new ValidationException(
                    String.format("Option %s must be positive, but was %s.", option, value));
        }
    }
}
