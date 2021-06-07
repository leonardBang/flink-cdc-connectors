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

import com.alibaba.ververica.cdc.connectors.mysql.debezium.SharedBinlogReader;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.SnapshotSplitReader;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.InputStatus;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MySQLSplitReader<E, T> extends SourceReaderBase<E, T, MySQLSplit, MySQLSplit> {

    private SharedBinlogReader sharedBinlogReader;
    private SnapshotSplitReader snapshotSplitReader;
    private List<MySQLSplit> snapshotSplits;


    public MySQLSplitReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue, SplitFetcherManager<E, MySQLSplit> splitFetcherManager, RecordEmitter<E, T, MySQLSplit> recordEmitter, Configuration config, SourceReaderContext context) {
        super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
        snapshotSplits = new ArrayList<>();

        sharedBinlogReader = new SharedBinlogReader(null, null,  null);
        snapshotSplitReader = new SnapshotSplitReader(null, null, null, sharedBinlogReader, null);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        //snapshot reading

        while(!snapshotSplits.isEmpty()) {
            snapshotSplitReader.setCurrentTableSplit(snapshotSplits.get(0));

            sharedBinlogReader.start();

            List<SourceRecord>  sourceRecords;
            while ((sourceRecords = snapshotSplitReader.poll()) != null) {
                System.out.println(sourceRecords);
            }
            sharedBinlogReader.stop();
        }
        sharedBinlogReader.poll();

        return super.pollNext(output);
    }

    private void finishCurrentSplit() {

    }

    @Override
    public void notifyNoMoreSplits() {
        super.notifyNoMoreSplits();
        // TODO binlog read

    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return super.isAvailable();
    }

    @Override
    public List<MySQLSplit> snapshotState(long checkpointId) {
        finishCurrentSplit();
        return super.snapshotState(checkpointId);
    }

    @Override
    public void addSplits(List<MySQLSplit> splits) {
        snapshotSplits.addAll(splits);
    }

    @Override
    protected void onSplitFinished(Map<String, MySQLSplit> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected MySQLSplit initializedState(MySQLSplit split) {
        return null;
    }

    @Override
    protected MySQLSplit toSplitType(String splitId, MySQLSplit splitState) {
        return null;
    }
}
