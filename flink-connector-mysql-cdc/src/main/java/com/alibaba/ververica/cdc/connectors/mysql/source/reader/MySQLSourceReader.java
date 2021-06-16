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

package com.alibaba.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.SharedBinlogReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitState;
import io.debezium.connector.mysql.legacy.BinlogReader.BinlogPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The source reader for MySQL source splits.
 */
public class MySQLSourceReader<T> extends SingleThreadMultiplexSourceReaderBase<Tuple2<T, BinlogPosition>, T, MySQLSplit, MySQLSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLSourceReader.class);

    private final SharedBinlogReader<T> binlogReader;

    public MySQLSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, BinlogPosition>>> elementQueue,
            RecordEmitter<Tuple2<T, BinlogPosition>, T, MySQLSplitState> recordEmitter,
            Supplier<MySQLSplitReader<T>> splitReaderSupplier,
            Configuration config,
            SourceReaderContext context,
            SharedBinlogReader<T> binlogReader) {
        super(
            elementQueue,
            new SingleThreadFetcherManager(elementQueue, splitReaderSupplier::get),
            recordEmitter,
            config,
            context);
        this.binlogReader = binlogReader;
    }

    @Override
    public void start() {
        binlogReader.initialize();
        super.start();
    }

    @Override
    protected MySQLSplitState initializedState(MySQLSplit split) {

        return new MySQLSplitState(split);
    }

    @Override
    public void notifyNoMoreSplits() {
        super.notifyNoMoreSplits();
    }

    @Override
    public List<MySQLSplit> snapshotState(long checkpointId) {
        // save binlog offset for snapshot-finished splits
        binlogReader.pause();

        List<MySQLSplit> splits = super.snapshotState(checkpointId);
        List<MySQLSplit> splitsWithOffset = splits.stream().map(MySQLSplitState::new).map(
            mySQLSplitState -> {
            if (mySQLSplitState.isSplitScanFinished()) {
                mySQLSplitState.setCurrentOffset(binlogReader.getCurrentOffset());
            }
            return mySQLSplitState.toMySQLSplit();
            }).collect(Collectors.toList());

        binlogReader.resume();
        return splitsWithOffset;
    }


    @Override
    protected void onSplitFinished(Map<String, MySQLSplitState> finishedSplitIds) {
        LOG.info("The snapshot read for split(s) {} has finished.", finishedSplitIds);

        super.addSplits(finishedSplitIds.values()
            .stream()
            .map(MySQLSplitState::toMySQLSplit)
            .collect(Collectors.toList()));
        LOG.info("Add back finished split(s) {} to reader as binlog filter.", finishedSplitIds);

        context.sendSplitRequest();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        super.handleSourceEvents(sourceEvent);
    }

    @Override
    protected MySQLSplit toSplitType(String splitId, MySQLSplitState splitState) {
        return splitState;
    }
}
