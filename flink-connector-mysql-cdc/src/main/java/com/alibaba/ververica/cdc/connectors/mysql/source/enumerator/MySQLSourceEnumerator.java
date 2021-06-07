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

package com.alibaba.ververica.cdc.connectors.mysql.source.enumerator;

import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

/**
 * A MySQL CDC source enumerator that enumerates receive the split request and assign the split to source readers.
 */
public class MySQLSourceEnumerator implements SplitEnumerator<MySQLSplit, MySQLSourceEnumState> {

    private static final Logger logger = LoggerFactory.getLogger(MySQLSourceEnumerator.class);
    private final SplitEnumeratorContext<MySQLSplit> context;
    private final MySQLSplitAssigner splitAssigner;

    public MySQLSourceEnumerator(SplitEnumeratorContext<MySQLSplit> context, MySQLSplitAssigner splitAssigner) {
        this.context = context;
        this.splitAssigner = splitAssigner;
    }

    @Override
    public void start() {
        this.splitAssigner.open();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        Optional<MySQLSplit> split = splitAssigner.getNext(requesterHostname);
        if (split.isPresent()) {
            context.assignSplit(split.get(), subtaskId);
            logger.info(String.format("Assign split %s for task %s in host %s", split.get(), subtaskId, requesterHostname));
        } else {
            context.signalNoMoreSplits(subtaskId);
            logger.info(String.format("No available split for task %s in host %s.", subtaskId, requesterHostname));
        }
    }

    @Override
    public void addSplitsBack(List<MySQLSplit> splits, int subtaskId) {
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public MySQLSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new MySQLSourceEnumState(splitAssigner.getCapturedTables(), splitAssigner.getAssignedTables(), splitAssigner.getRecycleSplits(), splitAssigner.getAssignedTableMaxSplit(), splitAssigner.getTableMaxPrimaryKey(), splitAssigner.getCurrentTableId());
    }

    @Override
    public void close() throws IOException {
        this.splitAssigner.close();
    }
}
