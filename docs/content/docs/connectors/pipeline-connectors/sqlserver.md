---
title: "SQL Server"
weight: 2
type: docs
aliases:
- /connectors/pipeline-connectors/sqlserver
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# SQL Server Connector

The SQL Server connector allows reading snapshot data and incremental data from SQL Server databases and provides end-to-end database synchronization capabilities.
This document describes how to set up the SQL Server connector for YAML pipelines.

## Setup SQL Server Database

A SQL Server administrator must enable change data capture on the source database and source tables that you want to capture.

**Prerequisites:**

* CDC is enabled on the SQL Server database.
* The SQL Server Agent is running.
* The connector user can access the captured tables and CDC change tables.

Enable CDC on a database and table:

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_enable_db;
GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'MyTable',
@role_name     = NULL,
@supports_net_changes = 0;
GO
```

Verify that the table is enabled for CDC:

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture;
GO
```

## Example

An example of the pipeline for reading data from SQL Server and sinking to Doris can be defined as follows:

```yaml
source:
  type: sqlserver
  name: SQL Server Source
  hostname: 127.0.0.1
  port: 1433
  username: sa
  password: Password!
  tables: inventory.dbo.\.*
  server-time-zone: UTC
  schema-change.enabled: true

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: pass

pipeline:
  name: SQL Server to Doris Pipeline
  parallelism: 4
```

## Connector Options

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width: 10%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 65%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>IP address or hostname of the SQL Server database server.</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1433</td>
      <td>Integer</td>
      <td>Integer port number of the SQL Server database server.</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Name of the SQL Server user to use when connecting to the SQL Server database server.</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Password to use when connecting to the SQL Server database server.</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table names of the SQL Server tables to monitor. Each entry must be in the <code>database.schema.table</code> form, and all captured tables must belong to the same literal database. Regular expressions are supported for schema and table names. Multiple entries are separated by commas. Escape a comma with a backslash when it is part of a regular expression. The dot (<code>.</code>) is treated as a delimiter for database, schema and table names. If a dot is needed in a regular expression to match any character, escape it with a backslash. For example: <code>db0.dbo.\.*</code>, <code>db0.dbo.user_table_[0-9]+</code>, <code>db0.dbo.(app|web)_order_\.*</code>.</td>
    </tr>
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Table names of the SQL Server tables to exclude. Each entry must be in the <code>database.schema.table</code> form and use the same literal database as <code>tables</code>. Regular expressions are supported for schema and table names.</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The session time zone in the database server. If not set, <code>ZoneId.systemDefault()</code> is used to determine the server time zone.</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to send schema change events so downstream sinks can respond to schema changes.</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>Startup mode for the SQL Server CDC consumer. Supported values are <code>initial</code>, <code>latest-offset</code>, <code>snapshot</code> and <code>timestamp</code>.</td>
    </tr>
    <tr>
      <td>scan.startup.timestamp-millis</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>Timestamp in milliseconds used when <code>scan.startup.mode</code> is <code>timestamp</code>.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.key-column</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The chunk key column for table snapshot splitting. By default, the chunk key is the first column of the primary key. This column must be a column of the primary key.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>The chunk size (number of rows) of table snapshots.</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>The maximum fetch size for each poll when reading table snapshots.</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to skip backfill in the snapshot reading phase. If backfill is skipped, changes on captured tables during the snapshot phase are consumed later in the change log reading phase instead of being merged into the snapshot.</td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to close idle readers at the end of the snapshot phase. This feature depends on FLIP-147: Support Checkpoints After Tasks Finished.</td>
    </tr>
    <tr>
      <td>scan.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to scan newly added tables when the job is restored from a savepoint or checkpoint.</td>
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>List of readable metadata from <code>SourceRecord</code> to pass downstream, separated by commas. Available metadata are <code>database_name</code>, <code>schema_name</code>, <code>table_name</code> and <code>op_ts</code>.</td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>The maximum time that the connector should wait after trying to connect to the SQL Server database server before timing out.</td>
    </tr>
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>The maximum retry times for building SQL Server database server connections.</td>
    </tr>
    <tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>The connection pool size.</td>
    </tr>
    <tr>
      <td>jdbc.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Custom JDBC URL properties. For example: <code>jdbc.properties.encrypt: true</code>.</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Pass-through Debezium properties to the Debezium Embedded Engine used to capture data changes from SQL Server.</td>
    </tr>
    </tbody>
</table>
</div>

## Available Metadata

The following metadata can be passed to downstream when configured in <code>metadata.list</code>.

<table class="colwidths-auto docutils">
  <thead>
     <tr>
       <th class="text-left" style="width: 15%">Key</th>
       <th class="text-left" style="width: 30%">DataType</th>
       <th class="text-left" style="width: 55%">Description</th>
     </tr>
  </thead>
  <tbody>
    <tr>
      <td>database_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the database that contains the row.</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the schema that contains the row.</td>
    </tr>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>Name of the table that contains the row.</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>Time when the change was made in the database. For snapshot records, the value is always 0.</td>
    </tr>
  </tbody>
</table>

## Startup Reading Position

The option <code>scan.startup.mode</code> specifies the startup mode for the SQL Server CDC consumer.

* <code>initial</code> (default): Takes a snapshot of table structure and data, then continues reading changes.
* <code>snapshot</code>: Takes a snapshot of table structure and data and stops after the snapshot phase.
* <code>latest-offset</code>: Starts from the latest change log offset.
* <code>timestamp</code>: Starts from the first available LSN whose commit time is greater than or equal to <code>scan.startup.timestamp-millis</code>.

## Limitations

### Single Database

All entries in <code>tables</code> must belong to the same literal database. Regular expressions are supported for schema and table names, but not for the database segment.

### Checkpoints During Snapshot Reading

During snapshot reading, SQL Server CDC needs to scan table chunks before switching to the change log phase. For large tables, configure checkpoint timeout and restart strategy carefully to avoid unnecessary failover caused by long-running snapshot chunks.

{{< top >}}
