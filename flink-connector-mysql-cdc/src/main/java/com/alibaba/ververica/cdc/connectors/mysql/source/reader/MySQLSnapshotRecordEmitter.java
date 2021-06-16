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

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.SnapshotReadCallBack;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.WatermarkInterval;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplitState;
import io.debezium.connector.mysql.legacy.BinlogReader.BinlogPosition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link RecordEmitter} implementation for {@link MySQLSourceReader}.
 *
 * <p> The {@link RecordEmitter} buffers the snapshot records of split and
 * call the binlog reader to emit records rather than emit the records directly.
 */
public final class MySQLSnapshotRecordEmitter<T> implements RecordEmitter<Tuple2<T, BinlogPosition>, T, MySQLSplitState> {

    private final SnapshotReadCallBack<T> snapshotReadCallBack;
    private final Map<String, ArrayList<T>> splitBuffer;

    public MySQLSnapshotRecordEmitter(SnapshotReadCallBack<T> snapshotReadCallBack) {
        this.snapshotReadCallBack = snapshotReadCallBack;
        this.splitBuffer = new ConcurrentHashMap<>();
    }

    @Override
    public void emitRecord(Tuple2<T, BinlogPosition> element, SourceOutput<T> output, MySQLSplitState splitState) throws Exception {
        final ArrayList<T> splitData = splitBuffer.getOrDefault(splitState.splitId(), new ArrayList<>());
        // the first snapshot record
        if (!splitBuffer.containsKey(splitState.splitId())) {
            splitData.add(element.f0);
            splitState.setLowWatermark(element.f1);
            splitBuffer.put(splitState.splitId(), splitData);
        }
        // the last snapshot record
        else if (isLastSnapshotRecord(element)) {
            splitState.setHighWatermark(element.f1);

            splitState.setCurrentOffset(element.f1);

            splitState.setSplitScanFinished(true);
            // send the normalized snapshot data in binglog reader
            onSplitSnapshotFinish(splitState);
            // release the buffer
            splitBuffer.remove(splitState.splitId());
        } else {
            splitData.add(element.f0);
            splitBuffer.put(splitState.splitId(), splitData);
        }
    }

    private boolean isLastSnapshotRecord(Tuple2<T, BinlogPosition> element) {
        return element.f0 == null;
    }

    private void onSplitSnapshotFinish(MySQLSplitState mySQLSplitState) {

        snapshotReadCallBack.splitSnapshotFinished(
            new SnapshotReadCallBack.CallBackContext() {

                @Override
                public MySQLSplit getSplit() {
                    return mySQLSplitState.toMySQLSplit();
                }

                @Override
                public Collection getSplitData() {
                    return splitBuffer.getOrDefault(mySQLSplitState.splitId(), new ArrayList<>());
                }

                @Override
                public WatermarkInterval getWaterMarkInterval() {
                    return new WatermarkInterval(mySQLSplitState.getLowWatermark(), mySQLSplitState.getHighWatermark());
                }

            });
    }
}
