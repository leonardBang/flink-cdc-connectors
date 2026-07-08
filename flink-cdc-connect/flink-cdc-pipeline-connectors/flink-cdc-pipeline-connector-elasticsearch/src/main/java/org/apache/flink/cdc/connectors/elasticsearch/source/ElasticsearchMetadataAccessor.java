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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.table.api.TableException;

import com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/** Metadata accessor backed by Elasticsearch index mappings. */
class ElasticsearchMetadataAccessor implements MetadataAccessor {

    private final ElasticsearchSourceConfig config;

    ElasticsearchMetadataAccessor(ElasticsearchSourceConfig config) {
        this.config = config;
    }

    @Override
    public List<String> listNamespaces() {
        throw new UnsupportedOperationException("Elasticsearch does not support namespaces.");
    }

    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        throw new UnsupportedOperationException("Elasticsearch does not support schemas.");
    }

    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
        return config.getIndices().stream().map(TableId::tableId).collect(Collectors.toList());
    }

    @Override
    public Schema getTableSchema(TableId tableId) {
        String index = tableId.getTableName();
        try (ElasticsearchRestClient client = new ElasticsearchRestClient(config)) {
            JsonNode mapping = client.get("/" + index + "/_mapping");
            return ElasticsearchSchemaUtils.buildSchema(index, mapping, config);
        } catch (IOException e) {
            throw new TableException("Failed to read Elasticsearch mapping for index " + index, e);
        }
    }
}
