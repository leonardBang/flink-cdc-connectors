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
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import java.time.Duration;

/** Options for the Elasticsearch pipeline source. */
public class ElasticsearchDataSourceOptions {

    /** The comma-separated list of Elasticsearch hosts to connect to. */
    public static final ConfigOption<String> HOSTS =
            ConfigOptions.key("hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The comma-separated list of Elasticsearch hosts to connect to.");

    /** The comma-separated list of Elasticsearch indices to read. */
    public static final ConfigOption<String> INDICES =
            ConfigOptions.key("indices")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The comma-separated list of Elasticsearch indices to read.");

    /** The username for Elasticsearch authentication. */
    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username for Elasticsearch authentication.");

    /** The password for Elasticsearch authentication. */
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password for Elasticsearch authentication.");

    /** The document source field used as incremental cursor, for example updated_at. */
    public static final ConfigOption<String> SCAN_INCREMENTAL_FIELD =
            ConfigOptions.key("scan.incremental.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The document source field used as the incremental cursor. The field must be updated for every insert/update/soft-delete and must have doc_values enabled for sorting.");

    /** Optional source field used as tie breaker for deterministic checkpoint restore. */
    public static final ConfigOption<String> SCAN_TIEBREAKER_FIELD =
            ConfigOptions.key("scan.tiebreaker.field")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Optional source field used as a stable tie breaker with scan.incremental.field. Configure a unique doc_values field such as an id copy field for checkpoint-safe pagination.");

    /** The column name that stores the Elasticsearch document id in emitted rows. */
    public static final ConfigOption<String> ID_COLUMN =
            ConfigOptions.key("id.column")
                    .stringType()
                    .defaultValue("_id")
                    .withDescription(
                            "The column name that stores the Elasticsearch document id in emitted rows. It is used as the pipeline primary key by default.");

    /** Optional source field that marks a document as soft deleted. */
    public static final ConfigOption<String> SOFT_DELETE_FIELD =
            ConfigOptions.key("soft-delete.field")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Optional boolean source field. When true, the source emits a DELETE event instead of an upsert event.");

    /** Number of documents to fetch per Elasticsearch search request. */
    public static final ConfigOption<Integer> SCAN_PAGE_SIZE =
            ConfigOptions.key("scan.page.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Number of documents to fetch per Elasticsearch search request.");

    /** The PIT keep-alive passed to Elasticsearch. */
    public static final ConfigOption<Duration> PIT_KEEP_ALIVE =
            ConfigOptions.key("scan.pit.keep-alive")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(1))
                    .withDescription("The Elasticsearch point-in-time keep-alive duration.");

    /** Interval between incremental polling rounds when no new document is found. */
    public static final ConfigOption<Duration> POLL_INTERVAL =
            ConfigOptions.key("scan.poll.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription(
                            "Interval between incremental polling rounds when no new document is found.");

    /** HTTP request timeout. */
    public static final ConfigOption<Duration> REQUEST_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("Elasticsearch HTTP connection and socket timeout.");

    /** Whether to emit an initial snapshot before polling incremental changes. */
    public static final ConfigOption<Boolean> SNAPSHOT_ENABLED =
            ConfigOptions.key("scan.snapshot.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to emit an initial snapshot before incremental polling.");

    private ElasticsearchDataSourceOptions() {}
}
