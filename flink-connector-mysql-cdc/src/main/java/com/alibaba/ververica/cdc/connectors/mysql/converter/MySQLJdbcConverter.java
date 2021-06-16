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

package com.alibaba.ververica.cdc.connectors.mysql.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * A MySQL {@link JdbcConverter} that converts between JDBC objects and Flink SQL
 * internal data structure {@link RowData}.
 */
public class MySQLJdbcConverter implements JdbcConverter {

    private final RowType rowType;
    private final DeserializationConverter[] deserializationConverters;
    private final SerializationConverter[] serializationConverters;
    private final LogicalType[] fieldTypes;

    public MySQLJdbcConverter(RowType rowType) {
        Preconditions.checkNotNull(rowType);
        this.rowType = rowType;
        this.fieldTypes =
            rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
        this.deserializationConverters = new DeserializationConverter[rowType.getFieldCount()];
        this.serializationConverters = new SerializationConverter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            deserializationConverters[i] = createNullableDeserializationConverter(rowType.getTypeAt(i));
            serializationConverters[i] = createNullableSerializationConverter(fieldTypes[i]);
        }
    }

    @Override
    public Object[] toJdbcFields(RowData rowData) {
        Object[] jdbcObjects = new Object[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            jdbcObjects[i] = serializationConverters[i].serialize(rowData, i);
        }
        return jdbcObjects;
    }

    @Override
    public RowData toRowData(Object[] jdbcFields) {
        GenericRowData genericRowData = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            genericRowData.setField(i, deserializationConverters[i].deserialize(jdbcFields[i]));
        }
        return genericRowData;
    }


    /**
     * Create a nullable runtime {@link DeserializationConverter} from given {@link
     * LogicalType}.
     */
    private DeserializationConverter createNullableDeserializationConverter(LogicalType type) {
        return wrapIntoNullableDeserializationConverter(createDeserializationConverter(type));
    }

    private DeserializationConverter wrapIntoNullableDeserializationConverter(
        DeserializationConverter jdbcDeserializationConverter) {
        return val -> {
            if (val == null) {
                return null;
            } else {
                return jdbcDeserializationConverter.deserialize(val);
            }
        };
    }

    /** Create a nullable JDBC {@link DeserializationConverter} from given sql type. */
    private DeserializationConverter createDeserializationConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return val -> null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return val -> val;
            case TINYINT:
                return val -> ((Integer) val).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return val -> val instanceof Integer ? ((Integer) val).shortValue() : val;
            case INTEGER:
                return val -> val;
            case BIGINT:
                return val -> val;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return val ->
                    val instanceof BigInteger
                        ? DecimalData.fromBigDecimal(
                        new BigDecimal((BigInteger) val, 0), precision, scale)
                        : DecimalData.fromBigDecimal((BigDecimal) val, precision, scale);
            case DATE:
                return val -> (int) (((Date) val).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return val -> (int) (((Time) val).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return val ->
                    val instanceof LocalDateTime
                        ? TimestampData.fromLocalDateTime((LocalDateTime) val)
                        : TimestampData.fromTimestamp((Timestamp) val);
            case CHAR:
            case VARCHAR:
                return val -> StringData.fromString((String) val);
            case BINARY:
            case VARBINARY:
                return val -> (byte[]) val;
            case ARRAY:
            case ROW:
            case MAP:
            case MULTISET:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /** Create a nullable JDBC f{@link SerializationConverter} from given sql type. */
    protected SerializationConverter createNullableSerializationConverter(LogicalType type) {
        return wrapIntoNullableSerializationConverter(createSerializationConverter(type));
    }

    protected SerializationConverter wrapIntoNullableSerializationConverter(SerializationConverter serializationConverter) {
        return (val, index) -> {
            if (val == null || val.isNullAt(index)) {
               return null;
            } else {
                return serializationConverter.serialize(val, index);
            }
        };
    }

    private SerializationConverter createSerializationConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index) -> val.getBoolean(index);
            case TINYINT:
                return (val, index) -> val.getByte(index);
            case SMALLINT:
                return (val, index) -> val.getShort(index);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (val, index) -> val.getInt(index);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (val, index) -> val.getLong(index);
            case FLOAT:
                return (val, index) -> val.getFloat(index);
            case DOUBLE:
                return (val, index) -> val.getDouble(index);
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                return (val, index) -> val.getString(index).toString();
            case BINARY:
            case VARBINARY:
                return (val, index) -> val.getBinary(index);
            case DATE:
                return (val, index) -> Date.valueOf(LocalDate.ofEpochDay(val.getInt(index)));
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index) -> Time.valueOf(
                    LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                return (val, index) -> val.getTimestamp(index, timestampPrecision).toTimestamp();
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return (val, index) -> val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal();
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case RAW:
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    /** Runtime converter to convert JDBC object to {@link RowData} type object. */
    @FunctionalInterface
    private interface DeserializationConverter extends Serializable {

        Object deserialize(Object jdbcField);
    }

    /**
     * Runtime converter to convert {@link RowData} field to JDBC object.
     */
    @FunctionalInterface
    private interface SerializationConverter extends Serializable {

        Object serialize(RowData rowData, int index);
    }
}
