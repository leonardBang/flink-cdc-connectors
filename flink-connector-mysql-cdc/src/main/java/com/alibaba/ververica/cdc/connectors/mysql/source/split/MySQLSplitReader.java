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

package com.alibaba.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.SnapshotSplitRead;
import com.alibaba.ververica.cdc.connectors.mysql.source.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.mysql.legacy.BinlogReader.BinlogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/** The {@link SplitReader} implementation for the {@link MySQLSource} */
public class MySQLSplitReader<T> implements SplitReader<Tuple2<T, BinlogPosition>, MySQLSplit> {

    private final static Logger LOG = LoggerFactory.getLogger(MySQLSplitReader.class);

    private final Queue<MySQLSplit> splits;
    private final DebeziumDeserializationSchema<T> deserializationSchema;
    private final Configuration config;
    private final int subtaskId;

    private SnapshotSplitRead snapshotSplitReader;
    private List<MySQLSplit> snapshotSplits;

    public MySQLSplitReader(Configuration config, DebeziumDeserializationSchema<T> deserializationSchema, int subtaskId) {
        this.config = config;
        this.deserializationSchema = deserializationSchema;
        this.subtaskId = subtaskId;
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<Tuple2<T, BinlogPosition>> fetch() throws IOException {
        return null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MySQLSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                String.format(
                    "The SplitChange type of %s is not supported.",
                    splitsChanges.getClass()));
        }

        LOG.debug("Handling split change {}", splitsChanges);
        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {

    }
}
