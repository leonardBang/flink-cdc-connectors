/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.source;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ElasticsearchDataSourceFactory}. */
class ElasticsearchDataSourceFactoryTest {

    @Test
    void testCreateDataSource() {
        ElasticsearchDataSourceFactory factory = new ElasticsearchDataSourceFactory();

        DataSource dataSource = factory.createDataSource(context(validOptions()));

        assertThat(dataSource).isInstanceOf(ElasticsearchDataSource.class);
        assertThat(dataSource.getEventSourceProvider()).isNotNull();
        assertThat(dataSource.getMetadataAccessor()).isNotNull();
    }

    @Test
    void testLackRequiredOption() {
        ElasticsearchDataSourceFactory factory = new ElasticsearchDataSourceFactory();

        assertThatThrownBy(
                        () ->
                                factory.createDataSource(
                                        context(
                                                ImmutableMap.of(
                                                        "hosts",
                                                        "http://localhost:9200",
                                                        "indices",
                                                        "users"))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.incremental.field");
    }

    @Test
    void testValidatePositivePageSize() {
        ElasticsearchDataSourceFactory factory = new ElasticsearchDataSourceFactory();

        assertThatThrownBy(
                        () ->
                                factory.createDataSource(
                                        context(
                                                ImmutableMap.<String, String>builder()
                                                        .putAll(validOptions())
                                                        .put("scan.page.size", "0")
                                                        .build())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.page.size must be positive");
    }

    @Test
    void testUnsupportedOption() {
        ElasticsearchDataSourceFactory factory = new ElasticsearchDataSourceFactory();

        assertThatThrownBy(
                        () ->
                                factory.createDataSource(
                                        context(
                                                ImmutableMap.<String, String>builder()
                                                        .putAll(validOptions())
                                                        .put("unknown", "value")
                                                        .build())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported options found for 'elasticsearch'")
                .hasMessageContaining("unknown");
    }

    private static FactoryHelper.DefaultContext context(ImmutableMap<String, String> options) {
        return new FactoryHelper.DefaultContext(
                Configuration.fromMap(options),
                new Configuration(),
                ElasticsearchDataSourceFactoryTest.class.getClassLoader());
    }

    private static ImmutableMap<String, String> validOptions() {
        return ImmutableMap.of(
                "hosts", "http://localhost:9200",
                "indices", "users,orders",
                "scan.incremental.field", "updated_at",
                "scan.tiebreaker.field", "id",
                "id.column", "id");
    }
}
