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

import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;

/** A pipeline {@link DataSource} that reads Elasticsearch by snapshot and incremental queries. */
public class ElasticsearchDataSource implements DataSource {

    private final ElasticsearchSourceConfig config;

    public ElasticsearchDataSource(ElasticsearchSourceConfig config) {
        this.config = config;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        return FlinkSourceProvider.of(new ElasticsearchPipelineSource(config));
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new ElasticsearchMetadataAccessor(config);
    }

    @Override
    public boolean isParallelMetadataSource() {
        return true;
    }
}
