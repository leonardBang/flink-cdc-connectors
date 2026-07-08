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

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Serializable configuration for the Elasticsearch pipeline source. */
public class ElasticsearchSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> hosts;
    private final List<String> indices;
    private final String username;
    private final String password;
    private final String incrementalField;
    private final String tiebreakerField;
    private final String idColumn;
    private final String softDeleteField;
    private final int pageSize;
    private final Duration pitKeepAlive;
    private final Duration pollInterval;
    private final Duration requestTimeout;
    private final boolean snapshotEnabled;

    public ElasticsearchSourceConfig(
            List<String> hosts,
            List<String> indices,
            String username,
            String password,
            String incrementalField,
            String tiebreakerField,
            String idColumn,
            String softDeleteField,
            int pageSize,
            Duration pitKeepAlive,
            Duration pollInterval,
            Duration requestTimeout,
            boolean snapshotEnabled) {
        this.hosts = Collections.unmodifiableList(Objects.requireNonNull(hosts));
        this.indices = Collections.unmodifiableList(Objects.requireNonNull(indices));
        this.username = username;
        this.password = password;
        this.incrementalField = Objects.requireNonNull(incrementalField);
        this.tiebreakerField = tiebreakerField == null ? "" : tiebreakerField;
        this.idColumn = Objects.requireNonNull(idColumn);
        this.softDeleteField = softDeleteField == null ? "" : softDeleteField;
        this.pageSize = pageSize;
        this.pitKeepAlive = Objects.requireNonNull(pitKeepAlive);
        this.pollInterval = Objects.requireNonNull(pollInterval);
        this.requestTimeout = Objects.requireNonNull(requestTimeout);
        this.snapshotEnabled = snapshotEnabled;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public List<String> getIndices() {
        return indices;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getIncrementalField() {
        return incrementalField;
    }

    public String getTiebreakerField() {
        return tiebreakerField;
    }

    public boolean hasTiebreakerField() {
        return !tiebreakerField.isEmpty();
    }

    public String getIdColumn() {
        return idColumn;
    }

    public String getSoftDeleteField() {
        return softDeleteField;
    }

    public boolean hasSoftDeleteField() {
        return !softDeleteField.isEmpty();
    }

    public int getPageSize() {
        return pageSize;
    }

    public Duration getPitKeepAlive() {
        return pitKeepAlive;
    }

    public String getPitKeepAliveString() {
        return pitKeepAlive.toMillis() + "ms";
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public boolean isSnapshotEnabled() {
        return snapshotEnabled;
    }
}
