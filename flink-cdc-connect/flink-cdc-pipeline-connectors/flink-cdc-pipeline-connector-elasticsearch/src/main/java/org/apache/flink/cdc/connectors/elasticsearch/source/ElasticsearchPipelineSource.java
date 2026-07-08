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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.api.TableException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** A query-based Elasticsearch source using PIT + search_after. */
class ElasticsearchPipelineSource
        implements Source<Event, ElasticsearchSourceSplit, List<ElasticsearchSourceSplit>> {

    private static final long serialVersionUID = 1L;

    private final ElasticsearchSourceConfig config;

    ElasticsearchPipelineSource(ElasticsearchSourceConfig config) {
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<ElasticsearchSourceSplit, List<ElasticsearchSourceSplit>>
            createEnumerator(SplitEnumeratorContext<ElasticsearchSourceSplit> enumContext) {
        return new ElasticsearchEnumerator(
                enumContext,
                new ArrayList<>(
                        Collections.singletonList(
                                new ElasticsearchSourceSplit(
                                        config.getIndices(), config.isSnapshotEnabled()))));
    }

    @Override
    public SplitEnumerator<ElasticsearchSourceSplit, List<ElasticsearchSourceSplit>>
            restoreEnumerator(
                    SplitEnumeratorContext<ElasticsearchSourceSplit> enumContext,
                    List<ElasticsearchSourceSplit> checkpoint) {
        return new ElasticsearchEnumerator(enumContext, new ArrayList<>(checkpoint));
    }

    @Override
    public SimpleVersionedSerializer<ElasticsearchSourceSplit> getSplitSerializer() {
        return new ElasticsearchSourceStateSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<List<ElasticsearchSourceSplit>>
            getEnumeratorCheckpointSerializer() {
        return new ElasticsearchSourceStateSerializer<>();
    }

    @Override
    public SourceReader<Event, ElasticsearchSourceSplit> createReader(
            SourceReaderContext readerContext) {
        return new ElasticsearchReader(readerContext, config);
    }

    private static class ElasticsearchEnumerator
            implements SplitEnumerator<ElasticsearchSourceSplit, List<ElasticsearchSourceSplit>> {

        private final SplitEnumeratorContext<ElasticsearchSourceSplit> context;
        private final List<ElasticsearchSourceSplit> unassignedSplits;

        ElasticsearchEnumerator(
                SplitEnumeratorContext<ElasticsearchSourceSplit> context,
                List<ElasticsearchSourceSplit> unassignedSplits) {
            this.context = context;
            this.unassignedSplits = unassignedSplits;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            assignSplit(subtaskId);
        }

        @Override
        public void addSplitsBack(List<ElasticsearchSourceSplit> splits, int subtaskId) {
            unassignedSplits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {
            assignSplit(subtaskId);
        }

        @Override
        public List<ElasticsearchSourceSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(unassignedSplits);
        }

        @Override
        public void close() {}

        private void assignSplit(int subtaskId) {
            if (!unassignedSplits.isEmpty()) {
                ElasticsearchSourceSplit split = unassignedSplits.remove(0);
                Map<Integer, List<ElasticsearchSourceSplit>> assignment = new HashMap<>();
                assignment.put(subtaskId, Collections.singletonList(split));
                context.assignSplits(new SplitsAssignment<>(assignment));
            }
            context.signalNoMoreSplits(subtaskId);
        }
    }

    private static class ElasticsearchReader
            implements SourceReader<Event, ElasticsearchSourceSplit> {

        private static final ObjectMapper OBJECT_MAPPER = ElasticsearchRestClient.objectMapper();

        private final SourceReaderContext context;
        private final ElasticsearchSourceConfig config;
        private final Queue<Event> pendingEvents = new ArrayDeque<>();
        private final Map<String, Schema> schemaCache = new HashMap<>();

        private ElasticsearchRestClient client;
        private ElasticsearchSourceSplit split;
        private boolean noMoreSplits;
        private CompletableFuture<Void> availability = new CompletableFuture<>();

        ElasticsearchReader(SourceReaderContext context, ElasticsearchSourceConfig config) {
            this.context = context;
            this.config = config;
        }

        @Override
        public void start() {
            client = new ElasticsearchRestClient(config);
            context.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Event> output) throws Exception {
            if (pendingEvents.isEmpty()) {
                fillPendingEvents();
            }
            if (pendingEvents.isEmpty() && split == null && !noMoreSplits) {
                availability = new CompletableFuture<>();
            }
            Event event = pendingEvents.poll();
            if (event != null) {
                output.collect(event);
                return pendingEvents.isEmpty()
                        ? InputStatus.NOTHING_AVAILABLE
                        : InputStatus.MORE_AVAILABLE;
            }
            if (split == null && noMoreSplits) {
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<ElasticsearchSourceSplit> snapshotState(long checkpointId) {
            if (split == null) {
                return Collections.emptyList();
            }
            return Collections.singletonList(split);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availability;
        }

        @Override
        public void addSplits(List<ElasticsearchSourceSplit> splits) {
            if (splits.size() != 1) {
                throw new TableException(
                        "Elasticsearch source expects exactly one split, but got " + splits.size());
            }
            this.split = splits.get(0);
            availability.complete(null);
        }

        @Override
        public void notifyNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void close() throws Exception {
            if (split != null) {
                for (ElasticsearchSourceSplit.IndexState state : split.getIndexStates().values()) {
                    closePitQuietly(state);
                }
            }
            if (client != null) {
                client.close();
            }
        }

        private void fillPendingEvents() throws Exception {
            if (split == null) {
                return;
            }
            if (!emitMissingSchema()) {
                if (!split.isSnapshotFinished()) {
                    readSnapshotPage();
                } else {
                    readIncrementalPage();
                }
            }
        }

        private boolean emitMissingSchema() throws IOException {
            for (String index : config.getIndices()) {
                ElasticsearchSourceSplit.IndexState state = split.state(index);
                if (!state.schemaEmitted) {
                    Schema schema = schema(index);
                    pendingEvents.add(new CreateTableEvent(TableId.tableId(index), schema));
                    state.schemaEmitted = true;
                    return true;
                }
            }
            return false;
        }

        private void readSnapshotPage() throws Exception {
            for (String index : config.getIndices()) {
                ElasticsearchSourceSplit.IndexState state = split.state(index);
                if (!state.snapshotFinished) {
                    List<JsonNode> hits = search(index, state, true);
                    if (hits.isEmpty()) {
                        state.snapshotFinished = true;
                        state.snapshotSearchAfterJson = null;
                        closePitQuietly(state);
                    } else {
                        appendDataEvents(index, state, hits, true);
                    }
                    return;
                }
            }
        }

        private void readIncrementalPage() throws Exception {
            int size = config.getIndices().size();
            for (int attempt = 0; attempt < size; attempt++) {
                int indexPos = Math.floorMod(split.getIndexPosition(), size);
                split.setIndexPosition(indexPos + 1);
                String index = config.getIndices().get(indexPos);
                ElasticsearchSourceSplit.IndexState state = split.state(index);
                List<JsonNode> hits = search(index, state, false);
                closePitQuietly(state);
                if (!hits.isEmpty()) {
                    appendDataEvents(index, state, hits, false);
                    return;
                }
            }
            scheduleNextPoll();
        }

        private void scheduleNextPoll() {
            CompletableFuture<Void> nextAvailability = new CompletableFuture<>();
            availability = nextAvailability;
            CompletableFuture.delayedExecutor(
                            config.getPollInterval().toMillis(), TimeUnit.MILLISECONDS)
                    .execute(() -> nextAvailability.complete(null));
        }

        private List<JsonNode> search(
                String index, ElasticsearchSourceSplit.IndexState state, boolean snapshot)
                throws IOException {
            ensurePit(index, state);
            ObjectNode body = OBJECT_MAPPER.createObjectNode();
            body.put("size", config.getPageSize());
            body.set("pit", pitNode(state.pitId));
            body.set("sort", sortNode());
            body.set("query", snapshot ? matchAllQuery() : incrementalQuery(state));
            String searchAfterJson = snapshot ? state.snapshotSearchAfterJson : null;
            if (searchAfterJson != null) {
                body.set("search_after", OBJECT_MAPPER.readTree(searchAfterJson));
            }
            JsonNode response = client.post("/_search", body);
            if (response.has("pit_id")) {
                state.pitId = response.path("pit_id").asText();
            }
            List<JsonNode> hits = new ArrayList<>();
            for (JsonNode hit : response.path("hits").path("hits")) {
                hits.add(hit);
            }
            return hits;
        }

        private void appendDataEvents(
                String index,
                ElasticsearchSourceSplit.IndexState state,
                List<JsonNode> hits,
                boolean snapshot)
                throws IOException {
            ElasticsearchRecordConverter converter =
                    new ElasticsearchRecordConverter(config, schema(index));
            for (JsonNode hit : hits) {
                if (snapshot) {
                    JsonNode sort = hit.path("sort");
                    if (sort.isArray()) {
                        state.snapshotSearchAfterJson = sort.toString();
                    }
                }
                updateIncrementalCursor(state, hit);
                if (converter.isSoftDelete(hit)) {
                    pendingEvents.add(
                            DataChangeEvent.deleteEvent(
                                    TableId.tableId(index), converter.convert(hit)));
                } else {
                    pendingEvents.add(
                            DataChangeEvent.replaceEvent(
                                    TableId.tableId(index), converter.convert(hit)));
                }
            }
        }

        private void updateIncrementalCursor(
                ElasticsearchSourceSplit.IndexState state, JsonNode hit) {
            JsonNode source = hit.path("_source");
            JsonNode incrementalValue =
                    ElasticsearchRecordConverter.readSourceField(
                            source, config.getIncrementalField());
            if (!incrementalValue.isMissingNode() && !incrementalValue.isNull()) {
                state.lastIncrementalValueJson = incrementalValue.toString();
            }
            if (config.hasTiebreakerField()) {
                JsonNode tieBreakerValue =
                        ElasticsearchRecordConverter.readSourceField(
                                source, config.getTiebreakerField());
                if (!tieBreakerValue.isMissingNode() && !tieBreakerValue.isNull()) {
                    state.lastTieBreakerValueJson = tieBreakerValue.toString();
                }
            }
        }

        private ObjectNode pitNode(String pitId) {
            ObjectNode pit = OBJECT_MAPPER.createObjectNode();
            pit.put("id", pitId);
            pit.put("keep_alive", config.getPitKeepAliveString());
            return pit;
        }

        private ArrayNode sortNode() {
            ArrayNode sort = OBJECT_MAPPER.createArrayNode();
            sort.add(fieldSort(config.getIncrementalField()));
            if (config.hasTiebreakerField()) {
                sort.add(fieldSort(config.getTiebreakerField()));
            } else {
                ObjectNode shardDoc = OBJECT_MAPPER.createObjectNode();
                shardDoc.set("_shard_doc", OBJECT_MAPPER.createObjectNode().put("order", "asc"));
                sort.add(shardDoc);
            }
            return sort;
        }

        private ObjectNode fieldSort(String field) {
            ObjectNode sort = OBJECT_MAPPER.createObjectNode();
            ObjectNode order = OBJECT_MAPPER.createObjectNode();
            order.put("order", "asc");
            order.put("missing", "_last");
            sort.set(field, order);
            return sort;
        }

        private ObjectNode matchAllQuery() {
            ObjectNode query = OBJECT_MAPPER.createObjectNode();
            query.set("match_all", OBJECT_MAPPER.createObjectNode());
            return query;
        }

        private ObjectNode incrementalQuery(ElasticsearchSourceSplit.IndexState state)
                throws IOException {
            if (state.lastIncrementalValueJson == null) {
                return matchAllQuery();
            }
            JsonNode lastIncremental = OBJECT_MAPPER.readTree(state.lastIncrementalValueJson);
            ObjectNode query = OBJECT_MAPPER.createObjectNode();
            ObjectNode bool = OBJECT_MAPPER.createObjectNode();
            ArrayNode should = OBJECT_MAPPER.createArrayNode();
            should.add(rangeQuery(config.getIncrementalField(), "gt", lastIncremental));
            if (config.hasTiebreakerField() && state.lastTieBreakerValueJson != null) {
                JsonNode lastTieBreaker = OBJECT_MAPPER.readTree(state.lastTieBreakerValueJson);
                ObjectNode sameIncrementalAndLaterTie = OBJECT_MAPPER.createObjectNode();
                ObjectNode innerBool = OBJECT_MAPPER.createObjectNode();
                ArrayNode must = OBJECT_MAPPER.createArrayNode();
                must.add(termQuery(config.getIncrementalField(), lastIncremental));
                must.add(rangeQuery(config.getTiebreakerField(), "gt", lastTieBreaker));
                innerBool.set("must", must);
                sameIncrementalAndLaterTie.set("bool", innerBool);
                should.add(sameIncrementalAndLaterTie);
            }
            bool.set("should", should);
            bool.put("minimum_should_match", 1);
            query.set("bool", bool);
            return query;
        }

        private ObjectNode rangeQuery(String field, String operator, JsonNode value) {
            ObjectNode query = OBJECT_MAPPER.createObjectNode();
            ObjectNode range = OBJECT_MAPPER.createObjectNode();
            ObjectNode condition = OBJECT_MAPPER.createObjectNode();
            condition.set(operator, value);
            range.set(field, condition);
            query.set("range", range);
            return query;
        }

        private ObjectNode termQuery(String field, JsonNode value) {
            ObjectNode query = OBJECT_MAPPER.createObjectNode();
            ObjectNode term = OBJECT_MAPPER.createObjectNode();
            term.set(field, value);
            query.set("term", term);
            return query;
        }

        private void ensurePit(String index, ElasticsearchSourceSplit.IndexState state)
                throws IOException {
            if (state.pitId == null) {
                state.pitId = client.openPit(index).path("id").asText();
            }
        }

        private Schema schema(String index) throws IOException {
            Schema schema = schemaCache.get(index);
            if (schema == null) {
                JsonNode mapping = client.get("/" + index + "/_mapping");
                schema = ElasticsearchSchemaUtils.buildSchema(index, mapping, config);
                schemaCache.put(index, schema);
            }
            return schema;
        }

        private void closePitQuietly(ElasticsearchSourceSplit.IndexState state) {
            try {
                client.closePit(state.pitId);
            } catch (Exception ignored) {
                // Closing PIT is best-effort; expired PITs are harmless.
            } finally {
                state.pitId = null;
            }
        }
    }
}
