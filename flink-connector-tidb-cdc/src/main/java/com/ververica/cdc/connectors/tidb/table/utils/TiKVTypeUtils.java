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

package com.ververica.cdc.connectors.tidb.table.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.commons.lang3.BooleanUtils;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.BytesType;
import org.tikv.common.types.StringType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static java.lang.String.format;

/**
 * Utils to deal the type mapping between TiKV and Flink SQL, this is inspired by TiFlink project.
 */
public class TiKVTypeUtils {

    private TiKVTypeUtils() {}

    public static boolean isIndexKey(final byte[] key) {
        return key[9] == '_' && key[10] == 'i';
    }

    public static boolean isRecordKey(final byte[] key) {
        return key[9] == '_' && key[10] == 'r';
    }

    /** The default Data Type mapping from TiKV to Flink SQL. */
    public static DataType getNullableFlinkType(org.tikv.common.types.DataType dataType) {
        boolean unsigned = dataType.isUnsigned();
        int length = (int) dataType.getLength();
        switch (dataType.getType()) {
            case TypeBit:
                if (length == 1) {
                    return DataTypes.BOOLEAN();
                }
                return DataTypes.BINARY(length);
            case TypeTiny:
                if (length == 1) {
                    return DataTypes.BOOLEAN();
                }
                return unsigned ? DataTypes.SMALLINT() : DataTypes.TINYINT();
            case TypeYear:
            case TypeShort:
                return unsigned ? DataTypes.INT() : DataTypes.SMALLINT();
            case TypeInt24:
                return DataTypes.INT();
            case TypeLong:
                return unsigned ? DataTypes.BIGINT() : DataTypes.INT();
            case TypeLonglong:
                return unsigned ? DataTypes.DECIMAL(length, 0) : DataTypes.BIGINT();
            case TypeFloat:
                return DataTypes.FLOAT();
            case TypeDouble:
                return DataTypes.DOUBLE();
            case TypeNull:
                return DataTypes.NULL();
            case TypeDatetime:
                return DataTypes.TIMESTAMP();
            case TypeTimestamp:
                return DataTypes.TIMESTAMP_LTZ();
            case TypeDate:
            case TypeNewDate:
                return DataTypes.DATE();
            case TypeDuration:
                return DataTypes.TIME();
            case TypeTinyBlob:
            case TypeMediumBlob:
            case TypeBlob:
            case TypeVarString:
            case TypeString:
            case TypeVarchar:
            case TypeLongBlob:
                if (length > Integer.MAX_VALUE || length == -1) {
                    length = Integer.MAX_VALUE;
                }
                if (dataType instanceof StringType) {
                    return DataTypes.VARCHAR(length);
                } else if (dataType instanceof BytesType) {
                    return DataTypes.BINARY(length); // 2147483647
                }
                return DataTypes.VARBINARY(length);
            case TypeJSON:
            case TypeEnum:
                return DataTypes.STRING();
            case TypeSet:
                return DataTypes.ARRAY(DataTypes.STRING());
            case TypeDecimal:
                return DataTypes.DECIMAL(length, dataType.getDecimal());
            case TypeNewDecimal:
                if (length > 38) {
                    return DataTypes.STRING();
                }
                return DataTypes.DECIMAL(length, dataType.getDecimal());
            case TypeGeometry:
            default:
                throw new IllegalArgumentException(
                        format("can not get flink datatype by tikv type: %s", dataType));
        }
    }

    public static DataType getFlinkType(final org.tikv.common.types.DataType dataType) {
        final DataType typ = getNullableFlinkType(dataType);
        return dataType.isNotNull() ? typ.notNull() : typ;
    }

    /**
     * Transform TiKV java object to Flink java object by given Flink Datatype.
     *
     * @param object TiKV java object
     * @param dataType Flink datatype
     * @param serverTimeZone
     */
    public static Optional<Object> getObjectWithDataType(
            Object object, DataType dataType, DateTimeFormatter formatter, String serverTimeZone) {
        if (object == null) {
            return Optional.empty();
        }
        Class<?> conversionClass = dataType.getConversionClass();
        if (dataType.getConversionClass() == object.getClass()) {
            return Optional.of(object);
        }
        switch (conversionClass.getSimpleName()) {
            case "String":
                if (object instanceof byte[]) {
                    object = new String((byte[]) object);
                } else {
                    object = object.toString();
                }
                break;
            case "String[]":
                String[] strArray = ((String) object).split(",");
                StringData[] stringDataArray = new StringData[strArray.length];
                for (int i = 0; i < strArray.length; i++) {
                    stringDataArray[i] = StringData.fromString(strArray[i]);
                }
                object = new GenericArrayData(stringDataArray);
                break;
            case "Boolean":
                if (object instanceof byte[]) {
                    object = BooleanUtils.toBoolean(((byte[]) object)[0]);
                } else if (object instanceof Long) {
                    object = BooleanUtils.toBoolean(Integer.parseInt(object.toString()));
                }
                break;
            case "Byte":
                object = Byte.valueOf(((Long) object).toString());
                break;
            case "Short":
                object = Short.valueOf(((Long) object).toString());
                break;
            case "BigDecimal":
                object = BigDecimal.valueOf((long) object);
                break;
            case "Integer":
                object =
                        (int)
                                (long)
                                        getObjectWithDataType(
                                                        object,
                                                        DataTypes.BIGINT(),
                                                        formatter,
                                                        serverTimeZone)
                                                .get();
                break;
            case "byte[]":
                object = ((String) object).getBytes();
                break;
            case "Long":
                if (object instanceof LocalDate) {
                    object = ((LocalDate) object).toEpochDay();
                } else if (object instanceof LocalDateTime) {
                    object = Timestamp.valueOf(((LocalDateTime) object)).getTime();
                } else if (object instanceof LocalTime) {
                    object = ((LocalTime) object).toNanoOfDay();
                }
                break;
            case "LocalDate":
                if (object instanceof Date) {
                    object = ((Date) object).toLocalDate();
                } else if (object instanceof String) {
                    object = LocalDate.parse((String) object);
                } else if (object instanceof Long || object instanceof Integer) {
                    object = LocalDate.ofEpochDay(Long.parseLong(object.toString()));
                }
                break;
            case "LocalDateTime":
                if (object instanceof Timestamp) {
                    object = ((Timestamp) object).toLocalDateTime();
                } else if (object instanceof String) {
                    String timeString = (String) object;
                    object =
                            formatter == null
                                    ? LocalDateTime.parse(timeString)
                                    : LocalDateTime.parse(timeString, formatter);
                } else if (object instanceof Long) {
                    object = new Timestamp(((Long) object) / 1000).toLocalDateTime();
                }
                break;
            case "Instant":
                if (object instanceof Timestamp) {
                    object = ((Timestamp) object).toInstant();
                }
                break;
            case "LocalTime":
                if (object instanceof Long || object instanceof Integer) {
                    object = LocalTime.ofNanoOfDay(Long.parseLong(object.toString()));
                }
                break;
            default:
                object = null;
        }
        return Optional.of(object);
    }

    public static Optional<Object> getObjectWithDataType(
            final Object object, final DataType dataType, String serverTimeZone) {
        return getObjectWithDataType(object, dataType, null, serverTimeZone);
    }

    public static Object[] getObjectsWithDataTypes(
            final Object[] objects, final TiTableInfo tableInfo, String serverTimeZone) {
        for (int i = 0; i < objects.length; i++) {
            if (objects[i] == null) {
                continue;
            }
            org.tikv.common.types.DataType tidbType = tableInfo.getColumn(i).getType();
            DataType flinkType = getFlinkType(tidbType);
            objects[i] =
                    toRowDataType(
                            getObjectWithDataType(objects[i], flinkType, serverTimeZone).get(),
                            flinkType);
            if (tidbType.isUnsigned()) {
                objects[i] = dealUnsignedColumnValue(tidbType, objects[i]);
            }
        }
        return objects;
    }

    /** Deal with unsigned column's value. */
    public static Object dealUnsignedColumnValue(
            org.tikv.common.types.DataType dataType, Object object) {
        // For more information about numeric columns with unsigned, please refer link
        // https://docs.pingcap.com/tidb/stable/data-type-numeric#integer-types
        switch (dataType.getType()) {
            case TypeTiny:
                return (short) Byte.toUnsignedInt(((Short) object).byteValue());
            case TypeShort:
                return Short.toUnsignedInt(((Integer) object).shortValue());
            case TypeInt24:
                return ((int) object) & 0xffffff;
            case TypeLong:
                return Integer.toUnsignedLong(((Long) object).intValue());
            case TypeLonglong:
                return DecimalData.fromBigDecimal(
                        new BigDecimal(
                                Long.toUnsignedString(((DecimalData) object).toUnscaledLong())),
                        ((DecimalData) object).precision(),
                        ((DecimalData) object).scale());
            default:
                return object;
        }
    }

    /** Transform Row type to GenericRowData type. */
    public static Object toRowDataType(Object object, DataType dataType) {
        Object result = object;
        if (object == null) {
            return null;
        }
        switch (object.getClass().getSimpleName()) {
            case "String":
                result = StringData.fromString(object.toString());
                break;
            case "BigDecimal":
                BigDecimal bigDecimal = (BigDecimal) object;
                int precision = ((DecimalType) dataType.getLogicalType()).getPrecision();
                int scale = ((DecimalType) dataType.getLogicalType()).getScale();
                result = DecimalData.fromBigDecimal(bigDecimal, precision, scale);
                break;
            case "LocalDate":
                LocalDate localDate = (LocalDate) object;
                result = (int) localDate.toEpochDay();
                break;
            case "LocalDateTime":
                result = TimestampData.fromLocalDateTime((LocalDateTime) object);
                break;
            case "LocalTime":
                LocalTime localTime = (LocalTime) object;
                result = (int) (localTime.toNanoOfDay() / (1000 * 1000));
                break;
            case "Instant":
                result = TimestampData.fromInstant((Instant) object);
                break;
            default:
                // pass code style
                break;
        }
        return result;
    }
}
