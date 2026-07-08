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

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The single source split that owns per-index query cursor state. */
class ElasticsearchSourceSplit implements SourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    static final String SPLIT_ID = "elasticsearch-query-split";

    private final Map<String, IndexState> indexStates = new HashMap<>();
    private int indexPosition;

    ElasticsearchSourceSplit(List<String> indices, boolean snapshotEnabled) {
        for (String index : indices) {
            IndexState state = new IndexState();
            state.snapshotFinished = !snapshotEnabled;
            indexStates.put(index, state);
        }
    }

    Map<String, IndexState> getIndexStates() {
        return indexStates;
    }

    IndexState state(String index) {
        return indexStates.get(index);
    }

    int getIndexPosition() {
        return indexPosition;
    }

    void setIndexPosition(int indexPosition) {
        this.indexPosition = indexPosition;
    }

    boolean isSnapshotFinished() {
        return indexStates.values().stream().allMatch(s -> s.snapshotFinished);
    }

    @Override
    public String splitId() {
        return SPLIT_ID;
    }

    static class IndexState implements Serializable {
        private static final long serialVersionUID = 1L;

        boolean schemaEmitted;
        boolean snapshotFinished;
        String snapshotSearchAfterJson;
        String lastIncrementalValueJson;
        String lastTieBreakerValueJson;
        transient String pitId;
    }
}
