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

package com.alibaba.ververica.cdc.connectors.mysql.debezium;


import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;

import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashSet;
import java.util.List;

import io.debezium.connector.mysql.BinlogReader;
import io.debezium.connector.mysql.HaltingPredicate;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.function.Callable;

/**
 * A Debezium binlog reader that reads binlog and deal overlapping data with {@link com.alibaba.ververica.cdc.connectors.mysql.debezium.SnapshotSplitReader}.
 */
public class SharedBinlogReader extends BaseReader implements Callable {

    private BinlogReader.BinlogPosition currentBinlogPosition;
    private HashSet<MySQLSplit> fetchedSplits;

    public SharedBinlogReader(String name, MySqlTaskContext context, HaltingPredicate acceptAndContinue) {
        super(name, context, acceptAndContinue);
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doCleanup() {

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return super.poll();
    }

    @Override
    protected void pollComplete(List<SourceRecord> batch) {
        super.pollComplete(batch);
    }

    @Override
    protected void enqueueRecord(SourceRecord record) throws InterruptedException {
        super.enqueueRecord(record);
    }

    @Override
    public void call() {

    }
}
