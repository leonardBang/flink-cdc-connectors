/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.source;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.SharedBinlogReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSourceEnumStateSerializer;
import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSourceEnumerator;
import com.alibaba.ververica.cdc.connectors.mysql.source.assigner.MySQLSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySQLSnapshotRecordEmitter;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySQLSourceReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSourceEnumState;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitSerializer;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.mysql.legacy.BinlogReader.BinlogPosition;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * The MySQL Source based on FLIP-27 which can read snapshot and continue to consume binlog.
 * @param <T> The record type.
 */
public class MySQLSource<T> implements Source<T, MySQLSplit, MySQLSourceEnumState> {

    private final RowType pkRowType;
    private final DebeziumDeserializationSchema<T> debeziumDeserializationSchema;
    private final Configuration config;

    public MySQLSource(RowType pkRowType, DebeziumDeserializationSchema<T> debeziumDeserializationSchema, Configuration config) {
        this.pkRowType = pkRowType;
        this.debeziumDeserializationSchema = debeziumDeserializationSchema;
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, MySQLSplit> createReader(SourceReaderContext readerContext) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, BinlogPosition>>> elementsQueue =
            new FutureCompletingBlockingQueue<>();
        Supplier<MySQLSplitReader<T>> splitReaderSupplier =
            () ->
                new MySQLSplitReader(
                    config, debeziumDeserializationSchema, readerContext.getIndexOfSubtask());
        SharedBinlogReader<T> binlogReader = new SharedBinlogReader<>(null, null, null, readerContext.getIndexOfSubtask());

        return new MySQLSourceReader(
            elementsQueue,
            new MySQLSnapshotRecordEmitter<>(binlogReader),
            splitReaderSupplier,
            config,
            readerContext,
            binlogReader);
    }

    @Override
    public SplitEnumerator<MySQLSplit, MySQLSourceEnumState> createEnumerator(SplitEnumeratorContext<MySQLSplit> enumContext) throws Exception {
        final Properties properties = new Properties();
        config.addAllToProperties(properties);
        final MySQLSplitAssigner splitAssigner = new MySQLSplitAssigner(
            properties,
            this.pkRowType,
            Collections.emptyList(),
            Collections.emptyList());
        return new MySQLSourceEnumerator(enumContext, splitAssigner);
    }

    @Override
    public SplitEnumerator<MySQLSplit, MySQLSourceEnumState> restoreEnumerator(SplitEnumeratorContext<MySQLSplit> enumContext, MySQLSourceEnumState checkpoint) throws Exception {
        final Properties properties = new Properties();
        config.addAllToProperties(properties);
        final MySQLSplitAssigner splitAssigner = new MySQLSplitAssigner(
            properties,
            this.pkRowType,
            checkpoint.getAlreadyProcessedTables(),
            checkpoint.getRemainingSplits());
        return new MySQLSourceEnumerator(enumContext, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<MySQLSplit> getSplitSerializer() {
        return MySQLSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<MySQLSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new MySQLSourceEnumStateSerializer(getSplitSerializer());
    }
}
