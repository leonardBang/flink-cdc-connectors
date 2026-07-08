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

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.Map;

/** Utilities for converting Elasticsearch mappings into pipeline schemas. */
class ElasticsearchSchemaUtils {

    private ElasticsearchSchemaUtils() {}

    static Schema buildSchema(String index, JsonNode mapping, ElasticsearchSourceConfig config) {
        Schema.Builder builder = Schema.newBuilder();
        builder.physicalColumn(config.getIdColumn(), DataTypes.STRING().notNull());
        JsonNode properties = mapping.path(index).path("mappings").path("properties");
        if (properties.isMissingNode()) {
            // Some aliases return the concrete index name as the root key. Fall back to the first.
            Iterator<Map.Entry<String, JsonNode>> roots = mapping.fields();
            if (roots.hasNext()) {
                properties = roots.next().getValue().path("mappings").path("properties");
            }
        }
        appendProperties(builder, "", properties, config.getIdColumn());
        builder.primaryKey(config.getIdColumn());
        return builder.build();
    }

    private static void appendProperties(
            Schema.Builder builder, String prefix, JsonNode properties, String idColumn) {
        if (properties == null || !properties.isObject()) {
            return;
        }
        Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = prefix.isEmpty() ? field.getKey() : prefix + "." + field.getKey();
            if (fieldName.equals(idColumn)) {
                continue;
            }
            JsonNode mapping = field.getValue();
            String type = mapping.path("type").asText("");
            if (type.isEmpty() && mapping.has("properties")) {
                builder.physicalColumn(fieldName, DataTypes.STRING());
            } else if ("object".equals(type) || "nested".equals(type)) {
                builder.physicalColumn(fieldName, DataTypes.STRING());
            } else {
                builder.physicalColumn(fieldName, toDataType(type));
            }
        }
    }

    private static DataType toDataType(String type) {
        switch (type) {
            case "boolean":
                return DataTypes.BOOLEAN();
            case "byte":
                return DataTypes.TINYINT();
            case "short":
                return DataTypes.SMALLINT();
            case "integer":
                return DataTypes.INT();
            case "long":
            case "unsigned_long":
                return DataTypes.BIGINT();
            case "float":
            case "half_float":
            case "scaled_float":
                return DataTypes.FLOAT();
            case "double":
                return DataTypes.DOUBLE();
            case "date":
            case "date_nanos":
                return DataTypes.TIMESTAMP(3);
            case "binary":
                return DataTypes.BYTES();
            case "keyword":
            case "constant_keyword":
            case "wildcard":
            case "text":
            case "match_only_text":
            default:
                return DataTypes.STRING();
        }
    }
}
