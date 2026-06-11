/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.sqlserver.utils;

import org.apache.flink.cdc.common.types.DataTypes;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlServerTypeUtils} type mapping. */
class SqlServerTypeUtilsTest {

    @Test
    void testMoneyMapsToDecimal19Scale4() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.OTHER, "money", 0, null)))
                .isEqualTo(DataTypes.DECIMAL(19, 4));
    }

    @Test
    void testSmallMoneyMapsToDecimal10Scale4() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.OTHER, "smallmoney", 0, null)))
                .isEqualTo(DataTypes.DECIMAL(10, 4));
    }

    @Test
    void testCharWithoutLengthMapsToString() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.CHAR, "char", 0, null)))
                .isEqualTo(DataTypes.STRING());
    }

    @Test
    void testCharWithLengthMapsToChar() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.CHAR, "char", 10, null)))
                .isEqualTo(DataTypes.CHAR(10));
    }

    @Test
    void testNCharWithLengthMapsToChar() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.NCHAR, "nchar", 5, null)))
                .isEqualTo(DataTypes.CHAR(5));
    }

    @Test
    void testDatetime2MapsToTimestamp() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.OTHER, "datetime2", 0, null)))
                .isEqualTo(DataTypes.TIMESTAMP(7));
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.OTHER, "datetime2", 0, 3)))
                .isEqualTo(DataTypes.TIMESTAMP(3));
    }

    @Test
    void testDatetimeMapsToTimestamp3() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.OTHER, "datetime", 0, null)))
                .isEqualTo(DataTypes.TIMESTAMP(3));
    }

    @Test
    void testSmallDatetimeMapsToTimestamp0() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.OTHER, "smalldatetime", 0, null)))
                .isEqualTo(DataTypes.TIMESTAMP(0));
    }

    @Test
    void testDatetimeOffsetMapsToTimestampLtz() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.OTHER, "datetimeoffset", 0, null)))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ(7));
    }

    @Test
    void testUniqueIdentifierMapsToString() {
        assertThat(
                        SqlServerTypeUtils.fromDbzColumn(
                                column(Types.OTHER, "uniqueidentifier", 0, null)))
                .isEqualTo(DataTypes.STRING());
    }

    @Test
    void testDecimalWithinPrecisionIsPreserved() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.DECIMAL, "decimal", 18, 2)))
                .isEqualTo(DataTypes.DECIMAL(18, 2));
    }

    @Test
    void testDecimalOverflowFallsBackToMaxPrecision() {
        assertThat(SqlServerTypeUtils.fromDbzColumn(column(Types.DECIMAL, "decimal", 40, 0)))
                .isEqualTo(DataTypes.DECIMAL(38, 0));
    }

    @Test
    void testNotNullColumnIsNotNullable() {
        Column notNull =
                Column.editor()
                        .name("c")
                        .jdbcType(Types.INTEGER)
                        .type("int")
                        .optional(false)
                        .create();
        assertThat(SqlServerTypeUtils.fromDbzColumn(notNull)).isEqualTo(DataTypes.INT().notNull());
    }

    private static Column column(int jdbcType, String typeName, int length, Integer scale) {
        ColumnEditor editor =
                Column.editor()
                        .name("c")
                        .jdbcType(jdbcType)
                        .type(typeName)
                        .length(length)
                        .optional(true);
        if (scale != null) {
            editor.scale(scale);
        }
        return editor.create();
    }
}
