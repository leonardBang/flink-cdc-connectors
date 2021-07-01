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

package com.alibaba.ververica.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.offset.BinlogPosition;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.MySQLBinlogSplitReadTask;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySQLSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getBinlogPosition;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getPrimaryKey;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getTableId;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;

/**
 * A Debezium binlog reader implementation that also support reads binlog and filter overlapping
 * snapshot data that {@link SnapshotSplitReader} read.
 */
public class BinlogSplitReader implements DebeziumReader<SourceRecord, MySQLSplit> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogSplitReader.class);
    private final StatefulTaskContext statefulTaskContext;
    private final ExecutorService executor;
    private final String logicalName;

    private volatile boolean currentTaskRunning;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private MySQLBinlogSplitReadTask binlogSplitReadTask;
    private MySQLSplit currentTableSplit;
    // tableId -> List[splitKeyStart, splitKeyEnd, splitHighWatermark]
    private Map<TableId, List<Tuple3<Object[], Object[], BinlogPosition>>> finishedSplitsInfo;
    private MySqlDatabaseSchema databaseSchema;

    public BinlogSplitReader(StatefulTaskContext statefulTaskContext, int subTaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subTaskId).build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.logicalName =
                statefulTaskContext
                        .getSchemaNameAdjuster()
                        .adjust(statefulTaskContext.getConnectorConfig().getLogicalName());
        this.currentTaskRunning = false;
    }

    public void submitSplit(MySQLSplit mySQLSplit) {
        this.currentTableSplit = mySQLSplit;
        configureFilter();
        statefulTaskContext.configure(currentTableSplit);
        this.queue = statefulTaskContext.getQueue();
        this.databaseSchema = statefulTaskContext.getDatabaseSchema();
        final MySqlOffsetContext mySqlOffsetContext = statefulTaskContext.getOffsetContext();
        mySqlOffsetContext.setBinlogStartPoint(
                currentTableSplit.getOffset().getFilename(),
                currentTableSplit.getOffset().getPosition());
        this.binlogSplitReadTask =
                new MySQLBinlogSplitReadTask(
                        statefulTaskContext.getConnectorConfig(),
                        mySqlOffsetContext,
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getErrorHandler(),
                        StatefulTaskContext.getClock(),
                        statefulTaskContext.getTaskContext(),
                        (MySqlStreamingChangeEventSourceMetrics)
                                statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                        statefulTaskContext
                                .getTopicSelector()
                                .topicNameFor(currentTableSplit.getTableId()),
                        currentTableSplit);

        executor.submit(
                () -> {
                    try {
                        currentTaskRunning = true;
                        binlogSplitReadTask.execute(new BinlogSplitChangeEventSourceContextImpl());
                    } catch (Exception e) {
                        currentTaskRunning = false;
                        LOGGER.error("execute task error", e);
                        e.printStackTrace();
                    }
                });
    }

    private class BinlogSplitChangeEventSourceContextImpl
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return currentTaskRunning;
        }
    }

    @Override
    public boolean isIdle() {
        return currentTableSplit == null || !currentTaskRunning;
    }

    @Nullable
    @Override
    public Iterator<SourceRecord> pollSplitRecords() throws InterruptedException {
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (currentTaskRunning) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
        }
        return sourceRecords.iterator();
    }

    /**
     * Returns the record should emit or not.
     *
     * <p>The watermark signal algorithm is the binlog split reader only sends the binlog event that
     * belongs to its finished snapshot splits. For each snapshot split, the binlog event is valid
     * since the offset is after its high watermark.
     *
     * <pre> E.g: the data input is :
     *    snapshot-split-0 info : [0,    1024) highWatermark0
     *    snapshot-split-1 info : [1024, 2048) highWatermark1
     *  the data output is:
     *  only the binlog event belong to [0,    1024) and offset is after highWatermark0 should send,
     *  only the binlog event belong to [1024, 2048) and offset is after highWatermark1 should send.
     * </pre>
     */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (isDataChangeRecord(sourceRecord)) {
            TableId tableId = getTableId(sourceRecord);
            Object[] key = getPrimaryKey(sourceRecord, databaseSchema.schemaFor(tableId));
            BinlogPosition position = getBinlogPosition(sourceRecord);
            for (Tuple3<Object[], Object[], BinlogPosition> splitInfo :
                    finishedSplitsInfo.get(tableId)) {
                if (RecordUtils.splitKeyRangeContains(key, splitInfo.f0, splitInfo.f1)
                        && position.isAtOrBefore(splitInfo.f2)) {
                    return true;
                }
            }
            // not in the monitored splits scope, do not emit
            return false;
        }
        // always send the schema change event and signal event
        // we need record them to state of Flink
        return true;
    }

    private void configureFilter() {
        List<Tuple5<TableId, String, Object[], Object[], BinlogPosition>> finishedSplitsInfo =
                currentTableSplit.getFinishedSplitsInfo();
        Map<TableId, List<Tuple3<Object[], Object[], BinlogPosition>>> splitsInfoMap =
                new HashMap<>();

        for (Tuple5<TableId, String, Object[], Object[], BinlogPosition> finishedSplitInfo :
                finishedSplitsInfo) {
            TableId tableId = finishedSplitInfo.f0;
            List<Tuple3<Object[], Object[], BinlogPosition>> list =
                    splitsInfoMap.getOrDefault(tableId, new ArrayList<>());
            list.add(Tuple3.of(finishedSplitInfo.f2, finishedSplitInfo.f3, finishedSplitInfo.f4));
            splitsInfoMap.put(tableId, list);
        }
        this.finishedSplitsInfo = splitsInfoMap;
    }
}
