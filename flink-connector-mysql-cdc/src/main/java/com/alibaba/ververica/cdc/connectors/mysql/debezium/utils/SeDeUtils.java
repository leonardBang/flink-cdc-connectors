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

package com.alibaba.ververica.cdc.connectors.mysql.debezium.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Utils class for serialization and deserialization.
 */
public class SeDeUtils {

    public static Object[] getDeserializeObjs(byte[]serializedObjs, TypeSerializer<RowData> serializer, RowType rowType) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedObjs)) {
            DataInputViewStreamWrapper input =  new DataInputViewStreamWrapper(in);
            RowData rowData =  serializer.deserialize(input);
            return getObjArrFromRowData(rowData, rowType);
        }
    }

    public static byte[] getSerializeObjs(RowData record, TypeSerializer<RowData> serializer) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(out);
            serializer.serialize(record, outputView);
            return out.toByteArray();
        }
    }

    public static Object[] getObjArrFromRowData(RowData record, RowType recordType) throws IOException {
        if (record != null) {
            Object[] objArr = new Object[recordType.getFieldCount()];
            for(int i = 0; i < recordType.getFieldCount(); i++){
                LogicalType elementType = recordType.getTypeAt(i);
                switch (elementType.getTypeRoot()) {
                    case BOOLEAN:
                        objArr[i] = record.getBoolean(i);
                    case TINYINT:
                        objArr[i] = record.getByte(i);
                    case SMALLINT:
                        objArr[i] = record.getShort(i);
                    case INTEGER:
                    case INTERVAL_YEAR_MONTH:
                        objArr[i] = record.getInt(i);
                    case BIGINT:
                    case INTERVAL_DAY_TIME:
                        objArr[i] = record.getLong(i);
                    case FLOAT:
                        objArr[i] = record.getFloat(i);
                    case DOUBLE:
                        objArr[i] = record.getDouble(i);
                    case CHAR:
                    case VARCHAR:
                        objArr[i] = record.getString(i).toString();
                    case BINARY:
                    case VARBINARY:
                        objArr[i] = record.getBinary(i);
                    case DATE:
                        objArr[i] = Date.valueOf(LocalDate.ofEpochDay(record.getInt(i)));
                    case TIME_WITHOUT_TIME_ZONE:
                        objArr[i] = LocalTime.ofNanoOfDay(record.getInt(i) * 1_000_000L);
                    case TIMESTAMP_WITH_TIME_ZONE:
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        final int timestampPrecision = ((TimestampType) elementType).getPrecision();
                        objArr[i] = record.getTimestamp(i, timestampPrecision).toTimestamp();
                    case DECIMAL:
                        final int decimalPrecision = ((DecimalType) elementType).getPrecision();
                        final int decimalScale = ((DecimalType) elementType).getScale();
                        objArr[i] = record.getDecimal(i, decimalPrecision, decimalScale);
                    case ARRAY:
                    case MAP:
                    case MULTISET:
                    case ROW:
                    case RAW:
                    default:
                        throw new UnsupportedOperationException("Unsupported type:" + elementType);
                }
            }
        }
        return null;
    }

}
