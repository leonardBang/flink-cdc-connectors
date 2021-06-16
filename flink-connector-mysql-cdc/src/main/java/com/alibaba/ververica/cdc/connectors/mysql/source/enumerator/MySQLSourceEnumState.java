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

import java.util.Collection;

import io.debezium.relational.TableId;

import javax.annotation.Nullable;

/**
 * The state of MySQL CDC source enumerator.
 */
public class MySQLSourceEnumState {

    /** The splits in the checkpoint. */
    private final Collection<MySQLSplit> remainingSplits;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final Collection<TableId> alreadyProcessedTables;

    /**
     * The cached byte representation from the last serialization step. This helps to avoid paying
     * repeated serialization cost for the same checkpoint object. This field is used by {@link
     * MySQLSourceEnumStateSerializer}.
     */
    @Nullable byte[] serializedFormCache;

    public MySQLSourceEnumState(Collection<TableId> alreadyProcessedTables, Collection<MySQLSplit> remainingSplits) {
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
    }

    public Collection<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    public Collection<MySQLSplit> getRemainingSplits() {
        return remainingSplits;
    }

    static MySQLSourceEnumState reusingCollection(final Collection<TableId> alreadyProcessedTables, final Collection<MySQLSplit> splits) {
        return new MySQLSourceEnumState(alreadyProcessedTables, splits);
    }
}
