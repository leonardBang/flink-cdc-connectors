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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.elasticsearch.sink.utils.ElasticsearchContainer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for the query-based Elasticsearch pipeline source. */
@Testcontainers
class ElasticsearchDataSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchDataSourceITCase.class);

    private static final String ELASTICSEARCH_VERSION = "8.12.1";
    private static final String INDEX = "es_source_it";
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(5);

    @Container
    private static final ElasticsearchContainer ELASTICSEARCH_CONTAINER =
            createElasticsearchContainer();

    private RestClient client;

    @BeforeEach
    void setUp() throws Exception {
        client = RestClient.builder(org.apache.http.HttpHost.create(baseUrl())).build();
        createIndex();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            try {
                client.performRequest(new Request("DELETE", "/" + INDEX));
            } finally {
                client.close();
            }
        }
    }

    @Test
    void testReadSnapshotAndIncrementalDocuments() throws Exception {
        indexDocument("1", "Alice", 1L, false);
        indexDocument("2", "Bob", 2L, false);
        refreshIndex();

        ElasticsearchSourceConfig sourceConfig = createSourceConfig();
        assertThat(sourceConfig.getPollInterval()).isEqualTo(POLL_INTERVAL);

        ElasticsearchPipelineSource source = new ElasticsearchPipelineSource(sourceConfig);
        TestingSourceReaderContext context = new TestingSourceReaderContext();
        CollectingReaderOutput output = new CollectingReaderOutput();

        try (SourceReader<Event, ElasticsearchSourceSplit> reader = source.createReader(context)) {
            reader.start();
            assertThat(context.isSplitRequested()).isTrue();
            reader.addSplits(
                    Collections.singletonList(
                            new ElasticsearchSourceSplit(
                                    sourceConfig.getIndices(), sourceConfig.isSnapshotEnabled())));
            reader.notifyNoMoreSplits();

            waitUntilDataChangeCount(reader, output, 2, Duration.ofSeconds(30));
            assertRecord(output.events(), "1", "Alice", 1L, OperationType.REPLACE);
            assertRecord(output.events(), "2", "Bob", 2L, OperationType.REPLACE);

            indexDocument("3", "Carol", 3L, false);
            refreshIndex();

            waitUntilDataChangeCount(reader, output, 3, Duration.ofSeconds(30));
            assertRecord(output.events(), "3", "Carol", 3L, OperationType.REPLACE);
        }
    }

    private static ElasticsearchContainer createElasticsearchContainer() {
        ElasticsearchContainer esContainer = new ElasticsearchContainer(ELASTICSEARCH_VERSION);
        esContainer.withLogConsumer(new Slf4jLogConsumer(LOG));
        return esContainer;
    }

    private static String baseUrl() {
        return String.format(
                "http://%s:%s",
                ELASTICSEARCH_CONTAINER.getHost(), ELASTICSEARCH_CONTAINER.getFirstMappedPort());
    }

    private ElasticsearchSourceConfig createSourceConfig() {
        return new ElasticsearchSourceConfig(
                Collections.singletonList(baseUrl()),
                Collections.singletonList(INDEX),
                null,
                null,
                "updated_at",
                "id",
                "id",
                "__deleted",
                1,
                Duration.ofMinutes(1),
                POLL_INTERVAL,
                Duration.ofSeconds(30),
                true);
    }

    private void createIndex() throws Exception {
        Request request = new Request("PUT", "/" + INDEX);
        request.setJsonEntity(
                "{"
                        + "\"mappings\":{"
                        + "\"properties\":{"
                        + "\"id\":{\"type\":\"keyword\"},"
                        + "\"name\":{\"type\":\"keyword\"},"
                        + "\"updated_at\":{\"type\":\"long\"},"
                        + "\"__deleted\":{\"type\":\"boolean\"}"
                        + "}"
                        + "}"
                        + "}");
        client.performRequest(request);
    }

    private void indexDocument(String id, String name, long updatedAt, boolean deleted)
            throws Exception {
        Request request = new Request("PUT", "/" + INDEX + "/_doc/" + id);
        request.setJsonEntity(
                String.format(
                        "{\"id\":\"%s\",\"name\":\"%s\",\"updated_at\":%s,\"__deleted\":%s}",
                        id, name, updatedAt, deleted));
        Response ignored = client.performRequest(request);
        assertThat(ignored.getStatusLine().getStatusCode()).isBetween(200, 299);
    }

    private void refreshIndex() throws Exception {
        client.performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
    }

    private void waitUntilDataChangeCount(
            SourceReader<Event, ElasticsearchSourceSplit> reader,
            CollectingReaderOutput output,
            int expectedCount,
            Duration timeout)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (!reader.isAvailable().isDone()) {
                reader.isAvailable()
                        .get(
                                Math.max(1, deadline - System.currentTimeMillis()),
                                java.util.concurrent.TimeUnit.MILLISECONDS);
            }
            reader.pollNext(output);
            if (dataChangeEvents(output.events()).size() >= expectedCount) {
                return;
            }
        }
        assertThat(dataChangeEvents(output.events()))
                .as("Collected events: %s", output.events())
                .hasSizeGreaterThanOrEqualTo(expectedCount);
    }

    private static List<DataChangeEvent> dataChangeEvents(List<Event> events) {
        List<DataChangeEvent> changes = new ArrayList<>();
        for (Event event : events) {
            if (event instanceof DataChangeEvent) {
                changes.add((DataChangeEvent) event);
            }
        }
        return changes;
    }

    private static void assertRecord(
            List<Event> events, String id, String name, long updatedAt, OperationType op) {
        Schema schema = schema(events);
        int idColumn = columnIndex(schema, "id");
        int nameColumn = columnIndex(schema, "name");
        int updatedAtColumn = columnIndex(schema, "updated_at");

        Optional<DataChangeEvent> matchingEvent =
                dataChangeEvents(events).stream()
                        .filter(event -> event.after().getString(idColumn).toString().equals(id))
                        .reduce((first, second) -> second);

        assertThat(matchingEvent).isPresent();
        DataChangeEvent event = matchingEvent.get();
        assertThat(event.tableId()).isEqualTo(TableId.tableId(INDEX));
        assertThat(event.op()).isEqualTo(op);
        assertThat(event.after().getString(idColumn).toString()).isEqualTo(id);
        assertThat(event.after().getString(nameColumn).toString()).isEqualTo(name);
        assertThat(event.after().getLong(updatedAtColumn)).isEqualTo(updatedAt);
    }

    private static Schema schema(List<Event> events) {
        return events.stream()
                .filter(event -> event instanceof CreateTableEvent)
                .map(event -> ((CreateTableEvent) event).getSchema())
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing CreateTableEvent."));
    }

    private static int columnIndex(Schema schema, String columnName) {
        int index = schema.getColumnNames().indexOf(columnName);
        assertThat(index)
                .as("Column %s exists in schema %s", columnName, schema)
                .isGreaterThanOrEqualTo(0);
        return index;
    }

    private static class TestingSourceReaderContext implements SourceReaderContext {

        private final AtomicBoolean splitRequested = new AtomicBoolean(false);

        boolean isSplitRequested() {
            return splitRequested.get();
        }

        @Override
        public SourceReaderMetricGroup metricGroup() {
            return null;
        }

        @Override
        public Configuration getConfiguration() {
            return new Configuration();
        }

        @Override
        public String getLocalHostName() {
            return "localhost";
        }

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public void sendSplitRequest() {
            splitRequested.set(true);
        }

        @Override
        public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return new UserCodeClassLoader() {
                @Override
                public ClassLoader asClassLoader() {
                    return ElasticsearchDataSourceITCase.class.getClassLoader();
                }

                @Override
                public void registerReleaseHookIfAbsent(String releaseHookName, Runnable hook) {}
            };
        }
    }

    private static class CollectingReaderOutput implements ReaderOutput<Event> {

        private final List<Event> events = new CopyOnWriteArrayList<>();

        List<Event> events() {
            return events;
        }

        @Override
        public void collect(Event event) {
            events.add(event);
        }

        @Override
        public void collect(Event event, long timestamp) {
            events.add(event);
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public SourceOutput<Event> createOutputForSplit(String splitId) {
            return this;
        }

        @Override
        public void releaseOutputForSplit(String splitId) {}
    }
}
