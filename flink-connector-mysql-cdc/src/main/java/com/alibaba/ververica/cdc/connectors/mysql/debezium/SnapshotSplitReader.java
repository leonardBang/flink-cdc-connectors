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

import org.apache.flink.util.Preconditions;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.debezium.connector.mysql.BinlogReader.BinlogPosition;
import io.debezium.connector.mysql.HaltingPredicate;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.schema.DatabaseSchema;

/**
 * A snapshot reader that reads data from Table in split level, the split is assigned by primary key range.
 */
public class SnapshotSplitReader extends BaseReader implements SnapshotReaderCallBack {

    protected static final Logger logger = LoggerFactory.getLogger(SnapshotSplitReader.class);
    private final SharedBinlogReader sharedBinlogReader;
    private JdbcConnection mysql;
    private MySQLSplit currentTableSplit;
    private HashMap<String, WatermarkInterval> splitsWatermark;
    private HashSet<MySQLSplit> fetchedSplits;
    /**
     * Buffer to save the scanned data from one split,
     * will be used to with data read between low watermark and high watermark by binlog reader.
     */
    private Map<Struct, SourceRecord> windowBuffer = new LinkedHashMap<>();
    private DatabaseSchema databaseSchema;

    public SnapshotSplitReader(String name, MySqlTaskContext context, HaltingPredicate acceptAndContinue, SharedBinlogReader sharedBinlogReader, MySQLSplit currentTableSplit) {
        super(name, context, acceptAndContinue);
        this.sharedBinlogReader = sharedBinlogReader;
        this.currentTableSplit = currentTableSplit;
        this.mysql = context.getConnectionContext().jdbc();
    }

    public MySQLSplit getCurrentTableSplit() {
        return currentTableSplit;
    }

    public void setCurrentTableSplit(MySQLSplit currentTableSplit) {
        this.currentTableSplit = currentTableSplit;
    }

    @Override
    protected void doStart() {
        Preconditions.checkNotNull(currentTableSplit);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return super.poll();
    }

    @Override
    protected void pollComplete(List<SourceRecord> batch) {
        //TODO
        batch =  null;
    }

    public void uponSplitFinished() throws Exception {
        sharedBinlogReader.call();
    }

    @Override
    protected void doStop() {
        try {

        } catch (Exception e) {
            logger.error("Reading split error", e);
        }
//        uponSplitFinished();

        currentTableSplit = null;
    }

    @Override
    protected void doCleanup() {
        if (mysql != null) {
            try {
                mysql.close();
            } catch (Exception e) {
                logger.error("Close mysql connection error.", e);
            }
            mysql = null;
        }
        fetchedSplits = null;

    }

    private void markLowWatermark(MySQLSplit currentTableSplit, BinlogPosition binlogPosition){
        WatermarkInterval watermarkInterval = new WatermarkInterval(binlogPosition, null);
        splitsWatermark.put(currentTableSplit.splitId(), watermarkInterval);
    }

    private void markHighWatermark(MySQLSplit currentTableSplit, BinlogPosition binlogPosition){
        WatermarkInterval watermarkInterval = splitsWatermark.get(currentTableSplit.splitId());
        Preconditions.checkState(watermarkInterval != null, "");
        watermarkInterval.setHighWatermark(binlogPosition);
        splitsWatermark.put(currentTableSplit.splitId(), watermarkInterval);
    }

    private BinlogPosition getCurrentBinlogPosition() {
        AtomicReference<BinlogPosition> currentBinlogPosition = null;
        try {
            mysql.setAutoCommit(false);
            String showMasterStmt = "SHOW MASTER STATUS";
            mysql.query(showMasterStmt, rs -> {
                if (rs.next()) {
                    String binlogFilename = rs.getString(1);
                    long binlogPosition = rs.getLong(2);
                    currentBinlogPosition.set(new BinlogPosition(binlogFilename, binlogPosition));
                    logger.info("Read binlog '{}' at position '{}'", binlogFilename, binlogPosition);
                }
                else {
                    throw new IllegalStateException("Cannot read the binlog filename and position via '" + showMasterStmt
                        + "'. Make sure your server is correctly configured");
                }
            });
            mysql.commit();
        } catch (Exception e) {
            logger.error("Read current binlog position error.", e);
        }
        return currentBinlogPosition.get();
    }

    public List<SourceRecord> merge(List<SourceRecord> binlogRecords) throws Exception {

        List<SourceRecord> normalizedBinlogRecords = new ArrayList<>();

        if (!binlogRecords.isEmpty()) {
            for (SourceRecord sourceRecord: binlogRecords) {
                Struct key = (Struct) sourceRecord.key();
                Struct value = (Struct) sourceRecord.value();
                Operation operation = Operation.forCode(value.getString(Envelope.FieldName.OPERATION));
                switch (operation) {
                    case UPDATE:
                        Envelope envelope = null;
                        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                        Struct updateAfter = value.getStruct(Envelope.FieldName.AFTER);
                        Instant ts = Instant.ofEpochMilli((Long) value.get(Envelope.FieldName.SOURCE));
                        SourceRecord record = new SourceRecord(sourceRecord.sourcePartition(),
                            sourceRecord.sourceOffset(), sourceRecord.topic(), sourceRecord.kafkaPartition(),
                            sourceRecord.keySchema(),  sourceRecord.key(), sourceRecord.valueSchema(), envelope.read(updateAfter, source, ts));
                        normalizedBinlogRecords.add(record);
                        case DELETE:
                            if (windowBuffer.containsKey(key)) {
                                windowBuffer.remove(key);
                            } else {
                                throw new IllegalStateException(String.format("The delete record %s doesn't exists in table split %s.", sourceRecord, currentTableSplit));
                            }
                    case CREATE:
                        normalizedBinlogRecords.add(sourceRecord);
                    case READ:
                        throw new IllegalStateException(String.format("Binlog record shouldn't use READ operation, the the record is %s.", sourceRecord));
                }

            }
            normalizedBinlogRecords.addAll(windowBuffer.values());
            return normalizedBinlogRecords;
        } else {
            return new ArrayList<>(windowBuffer.values());
        }
    }

    @Override
    public void onSplitFinished(ReaderCallBackContext callBackContext) {

    }
}

