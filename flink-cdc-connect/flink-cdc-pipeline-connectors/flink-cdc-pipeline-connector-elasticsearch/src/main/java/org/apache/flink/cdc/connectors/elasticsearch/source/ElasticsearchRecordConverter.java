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

import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypeRoot;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

/** Converts Elasticsearch search hits into Flink CDC {@link RecordData}. */
class ElasticsearchRecordConverter {

    private final ElasticsearchSourceConfig config;
    private final Schema schema;

    ElasticsearchRecordConverter(ElasticsearchSourceConfig config, Schema schema) {
        this.config = config;
        this.schema = schema;
    }

    RecordData convert(JsonNode hit) {
        JsonNode source = hit.path("_source");
        List<Column> columns = schema.getColumns();
        GenericRecordData row = new GenericRecordData(columns.size());
        for (int pos = 0; pos < columns.size(); pos++) {
            Column column = columns.get(pos);
            JsonNode value = readSourceField(source, column.getName());
            if (column.getName().equals(config.getIdColumn()) && isNull(value)) {
                value = hit.path("_id");
            }
            row.setField(pos, convertValue(value, column.getType().getTypeRoot()));
        }
        return row;
    }

    boolean isSoftDelete(JsonNode hit) {
        if (!config.hasSoftDeleteField()) {
            return false;
        }
        JsonNode value = readSourceField(hit.path("_source"), config.getSoftDeleteField());
        return value.isBoolean() ? value.asBoolean() : Boolean.parseBoolean(value.asText("false"));
    }

    static JsonNode readSourceField(JsonNode source, String fieldPath) {
        JsonNode current = source;
        for (String part : fieldPath.split("\\.")) {
            current = current.path(part);
        }
        return current;
    }

    private static boolean isNull(JsonNode value) {
        return value == null || value.isMissingNode() || value.isNull();
    }

    private static Object convertValue(JsonNode value, DataTypeRoot typeRoot) {
        if (isNull(value)) {
            return null;
        }
        if (value.isContainerNode()) {
            return BinaryStringData.fromString(value.toString());
        }
        switch (typeRoot) {
            case BOOLEAN:
                return value.asBoolean();
            case TINYINT:
                return (byte) value.asInt();
            case SMALLINT:
                return (short) value.asInt();
            case INTEGER:
                return value.asInt();
            case BIGINT:
                return value.asLong();
            case FLOAT:
                return (float) value.asDouble();
            case DOUBLE:
                return value.asDouble();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return parseTimestamp(value);
            case BINARY:
            case VARBINARY:
                return value.asText().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            case CHAR:
            case VARCHAR:
            default:
                return BinaryStringData.fromString(value.asText());
        }
    }

    private static TimestampData parseTimestamp(JsonNode value) {
        if (value.isNumber()) {
            return TimestampData.fromMillis(value.asLong());
        }
        String text = value.asText();
        try {
            return TimestampData.fromMillis(Instant.parse(text).toEpochMilli());
        } catch (Exception ignored) {
            LocalDateTime localDateTime = LocalDateTime.parse(text);
            return TimestampData.fromMillis(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
        }
    }
}
