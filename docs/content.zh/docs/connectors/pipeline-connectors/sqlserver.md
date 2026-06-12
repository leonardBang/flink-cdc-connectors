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

SQL Server 连接器支持从 SQL Server 数据库读取快照数据和增量数据，并提供端到端的数据库同步能力。本文介绍如何在 YAML Pipeline 中配置 SQL Server 连接器。

## 配置 SQL Server 数据库

SQL Server 管理员需要为源数据库和源表启用变更数据捕获（CDC）。

**前置条件：**

* SQL Server 数据库已启用 CDC。
* SQL Server Agent 正在运行。
* 连接器用户可以访问被捕获的源表和 CDC 变更表。

为数据库和表启用 CDC：

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

检查表是否已启用 CDC：

```sql
USE MyDB;
GO
EXEC sys.sp_cdc_help_change_data_capture;
GO
```

## 示例

从 SQL Server 读取数据同步到 Doris 的 Pipeline 可以定义如下：

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

## 连接器配置项

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
      <td>SQL Server 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1433</td>
      <td>Integer</td>
      <td>SQL Server 数据库服务器的端口号。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 SQL Server 数据库服务器时使用的用户名。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 SQL Server 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监控的 SQL Server 表名。每个条目必须使用 <code>database.schema.table</code> 格式，所有被捕获的表必须属于同一个字面量数据库。schema 和 table 名称支持正则表达式。多个条目使用逗号分隔；如果逗号是正则表达式的一部分，需要使用反斜杠转义。点号（<code>.</code>）被视为 database、schema 和 table 名称的分隔符。如果需要在正则表达式中使用点号匹配任意字符，需要使用反斜杠转义。例如：<code>db0.dbo.\.*</code>、<code>db0.dbo.user_table_[0-9]+</code>、<code>db0.dbo.(app|web)_order_\.*</code>。</td>
    </tr>
    <tr>
      <td>tables.exclude</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要排除的 SQL Server 表名。每个条目必须使用 <code>database.schema.table</code> 格式，并且使用与 <code>tables</code> 相同的字面量数据库。schema 和 table 名称支持正则表达式。</td>
    </tr>
    <tr>
      <td>server-time-zone</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>数据库服务器中的会话时区。若未设置，则使用 <code>ZoneId.systemDefault()</code> 来确定服务器时区。</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否发送 Schema 变更事件，以便下游 sink 响应表结构变更。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td>SQL Server CDC 消费者的启动模式。支持 <code>initial</code>、<code>latest-offset</code>、<code>snapshot</code> 和 <code>timestamp</code>。</td>
    </tr>
    <tr>
      <td>scan.startup.timestamp-millis</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>当 <code>scan.startup.mode</code> 为 <code>timestamp</code> 时使用的毫秒时间戳。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.key-column</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>表快照切分时使用的 chunk key 列。默认使用主键的第一列，且该列必须是主键列。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>表快照的 chunk 大小（行数）。</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>读取表快照时每次拉取的最大行数。</td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.backfill.skip</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否在快照读取阶段跳过 backfill。若跳过 backfill，快照阶段发生的变更会在后续 changelog 阶段消费，而不会合并到快照中。</td>
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照阶段结束时关闭空闲 reader。该功能依赖 FLIP-147：任务结束后支持 checkpoint。</td>
    </tr>
    <tr>
      <td>scan.newly-added-table.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>当作业从 savepoint 或 checkpoint 恢复时，是否扫描新添加的表。</td>
    </tr>
    <tr>
      <td>metadata.list</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要传递到下游的 <code>SourceRecord</code> 可读 metadata 列表，使用逗号分隔。可用 metadata 包括 <code>database_name</code>、<code>schema_name</code>、<code>table_name</code> 和 <code>op_ts</code>。</td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>连接 SQL Server 数据库服务器的最大等待时间。</td>
    </tr>
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>创建 SQL Server 数据库连接的最大重试次数。</td>
    </tr>
    <tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>连接池大小。</td>
    </tr>
    <tr>
      <td>jdbc.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>自定义 JDBC URL 属性。例如：<code>jdbc.properties.encrypt: true</code>。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>透传给 Debezium Embedded Engine 的 Debezium 属性，用于捕获 SQL Server 的数据变更。</td>
    </tr>
    </tbody>
</table>
</div>

## 可用 Metadata

配置 <code>metadata.list</code> 后，以下 metadata 可以传递到下游。

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
      <td>包含该行的数据库名称。</td>
    </tr>
    <tr>
      <td>schema_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的 schema 名称。</td>
    </tr>
    <tr>
      <td>table_name</td>
      <td>STRING NOT NULL</td>
      <td>包含该行的表名。</td>
    </tr>
    <tr>
      <td>op_ts</td>
      <td>TIMESTAMP_LTZ(3) NOT NULL</td>
      <td>该变更在数据库中发生的时间。对于快照记录，该值始终为 0。</td>
    </tr>
  </tbody>
</table>

## 启动读取位置

<code>scan.startup.mode</code> 用于指定 SQL Server CDC 消费者的启动模式。

* <code>initial</code>（默认）：先读取表结构和表数据快照，然后继续读取增量变更。
* <code>snapshot</code>：只读取表结构和表数据快照，快照阶段结束后停止。
* <code>latest-offset</code>：从最新的 changelog offset 开始读取。
* <code>timestamp</code>：从 commit time 大于或等于 <code>scan.startup.timestamp-millis</code> 的第一个可用 LSN 开始读取。

## 限制

### 单数据库

<code>tables</code> 中的所有条目必须属于同一个字面量数据库。schema 和 table 名称支持正则表达式，但 database 部分不支持正则表达式。

### 快照读取期间的 Checkpoint

快照读取阶段需要扫描表 chunk 后再切换到 changelog 阶段。对于大表，建议谨慎配置 checkpoint 超时时间和重启策略，避免长时间运行的快照 chunk 导致不必要的 failover。

{{< top >}}
