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

import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSourceEnumStateSerializer;
import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySQLSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSourceEnumState;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.HashMap;
import java.util.HashSet;

/**
 * The MySQL Source based on FLIP-27 which can read snapshot and continue to consume binlog.
 * @param <T> The record type.
 */
public class MySQLSource<T> implements Source<T, MySQLSplit, MySQLSourceEnumState> {

    public MySQLSource() {
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, MySQLSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new MySQLSplitReader<>(null, null, null, null, null);
    }

    @Override
    public SplitEnumerator<MySQLSplit, MySQLSourceEnumState> createEnumerator(SplitEnumeratorContext<MySQLSplit> enumContext) throws Exception {
        MySQLSplitAssigner splitAssigner = new MySQLSplitAssigner(
            null,
            null,
            new HashSet<>(),
            new HashSet<>(),
            new HashMap<>(),
            new HashMap<>(),
            null,
            null);
        return new com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSourceEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<MySQLSplit, MySQLSourceEnumState> restoreEnumerator(SplitEnumeratorContext<MySQLSplit> enumContext, MySQLSourceEnumState checkpoint) throws Exception {
        MySQLSplitAssigner splitAssigner = new MySQLSplitAssigner(
            null,
            checkpoint.getCapturedTables(),
            checkpoint.getAssignedTables(),
            checkpoint.getRecycleSplits(),
            checkpoint.getAssignedTableMaxSplit(),
            checkpoint.getTableMaxPrimaryKey(),
            checkpoint.getCurrentTable(),
            null);

        return new com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSourceEnumerator(enumContext, splitAssigner);
    }

    @Override
    public SimpleVersionedSerializer<MySQLSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<MySQLSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new MySQLSourceEnumStateSerializer();
    }
}
