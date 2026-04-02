# Nebula 点边模型与映射说明

## 1. 文档目标

本文档用于说明当前项目中 Nebula 图谱的：

- 点模型（有哪些点、各自承载什么语义）
- 边模型（有哪些边、方向是什么、作用是什么）
- 代码对象到图模型的映射关系（`NormalizedLineageResult` / `TableLineageEdge` / `LineageGraphEdge` 如何落图）
- 导入链路中的字段顺序、类型映射、幂等与覆盖规则

本文档对应当前代码实现，不是目标态设计稿。

## 2. 入图链路总览

当前离线导入链路可概括为：

1. 解析层产出 `NormalizedLineageResult`、`ExecutionCaptureEvent`。
2. `NebulaImporterBundleWriter` 将点边写入 bundle CSV。
3. Spark Connector 程序读取 CSV 并写入 Nebula。

关键代码位置：

- 模型定义与写 bundle：`src/main/java/com/zhuanzhuan/lineage/storage/nebula/NebulaImporterBundleWriter.java`
- Spark 导入主程序：
  - `src/main/scala/com/zhuanzhuan/lineage/app/SparkConnectorImportVerticesMain.scala`
  - `src/main/scala/com/zhuanzhuan/lineage/app/SparkConnectorImportEdgesMain.scala`
- Schema 启动与在线写入版本：`src/main/java/com/zhuanzhuan/lineage/storage/nebula/NebulaLineageStorage.java`

## 3. 点模型（Vertex）

点定义来自 `VERTEX_DEFINITIONS`，共 12 类。

| 点类型 | 主要语义 | 关键属性 |
|---|---|---|
| `task_node` | 调度任务实体 | `task_name`, `owner`, `script_path`, `biz_date` |
| `run_node` | 一次任务运行实例 | `run_id`, `biz_date` |
| `capture_event` | 一次 Spark 采集事件 | `func_name`, `status`, `capture_time`, `statement_type`, `error_message` |
| `table_node` | 物理/逻辑表实体 | `normalized_name`, `catalog_name`, `database_name`, `table_name`, `source_type` |
| `column_node` | 字段语义实体 | `column_name`, `owner_id`, `owner_type`, `data_type`, `qualifier` |
| `expression_node` | 表达式语义实体 | `expression_type`, `expression_sql`, `normalized_expression` |
| `scope_node` | 查询作用域（root/cte/subquery/branch） | `scope_name`, `scope_type`, `parent_scope_id`, `plan_node_name` |
| `literal_node` | 常量语义实体 | `literal_type`, `literal_value`, `normalized_value` |
| `operator_instance_node` | 逻辑算子实例 | `scope_id`, `operator_type`, `operator_sub_type`, `operator_path`, `parent_operator_id`, `plan_node_name` |
| `column_instance_node` | 字段在上下文中的实例 | `column_id`, `column_name`, `scope_id`, `relation_instance_id`, `instance_type`, `data_type`, `ordinal` |
| `relation_instance_node` | 表在上下文中的读取实例 | `instance_name`, `scope_id`, `source_table_id`, `source_type`, `alias_name`, `plan_node_name` |
| `predicate_node` | 过滤/关联谓词实体 | `predicate_type`, `predicate_sql`, `normalized_predicate`, `scope_id`, `plan_node_name` |

## 4. 边模型（Edge）分类与作用

边定义来自 `EDGE_DEFINITIONS`。可以按作用分为 5 组。

### 4.1 执行上下文边

用于把任务、run、执行事件、读写表关联起来：

- `task_has_run`
- `task_has_execution`
- `run_has_execution`
- `task_reads_table`
- `task_writes_table`
- `execution_reads_table`
- `execution_writes_table`

### 4.2 执行观测边

用于把一次执行事件与“本次看到的语义节点”关联：

- `execution_emits_column`
- `execution_observes_expression`
- `execution_observes_scope`
- `execution_observes_literal`
- `execution_observes_operator_instance`
- `execution_observes_column_instance`
- `execution_observes_relation_instance`
- `execution_observes_predicate`

### 4.3 结构边（结构归属）

用于表达 SQL 内部结构关系，不直接表达“血缘依赖”：

- `table_has_column`
- `scope_contains_scope`
- `scope_outputs_column`
- `scope_uses_expression`
- `scope_contains_operator_instance`
- `operator_precedes_operator_instance`
- `operator_outputs_column_instance`
- `operator_uses_expression`
- `operator_uses_predicate`
- `operator_reads_relation_instance`
- `scope_contains_column_instance`
- `scope_reads_relation_instance`
- `relation_instance_exposes_column_instance`
- `relation_instance_of_table`
- `column_has_instance`
- `scope_uses_predicate`
- `relation_instance_filtered_by_predicate`

### 4.4 事实边（Fact）

用于保留每次事件对应的依赖事实：

- `table_flows_to_table`
- `relation_instance_joins_relation_instance`
- `column_depends_on_column`
- `column_uses_expression`
- `expression_depends_on_column`
- `expression_depends_on_expression`
- `expression_depends_on_literal`
- `expression_depends_on_column_instance`
- `column_instance_uses_expression`
- `column_instance_depends_on_column_instance`
- `predicate_depends_on_column`
- `predicate_depends_on_column_instance`
- `column_instance_depends_on_relation_instance`
- `column_instance_filtered_by_predicate`
- `predicate_depends_on_literal`

### 4.5 最新态边（Latest）

用于维护同语义关系的“最新视图”：

- `latest_table_flows_to_table`
- `latest_relation_instance_joins_relation_instance`
- `latest_column_depends_on_column`
- `latest_column_flows_to_column`
- `latest_column_uses_expression`
- `latest_expression_flows_to_column`
- `latest_expression_depends_on_column`
- `latest_column_flows_to_expression`
- `latest_expression_depends_on_expression`
- `latest_expression_flows_to_expression`
- `latest_expression_depends_on_literal`
- `latest_literal_flows_to_expression`
- `latest_expression_depends_on_column_instance`
- `latest_column_instance_flows_to_expression`
- `latest_column_instance_uses_expression`
- `latest_expression_flows_to_column_instance`
- `latest_column_instance_depends_on_column_instance`
- `latest_column_instance_flows_to_column_instance`
- `latest_predicate_depends_on_column`
- `latest_column_flows_to_predicate`
- `latest_predicate_depends_on_column_instance`
- `latest_column_instance_flows_to_predicate`
- `latest_column_instance_depends_on_relation_instance`
- `latest_relation_instance_flows_to_column_instance`
- `latest_column_instance_filtered_by_predicate`
- `latest_predicate_flows_to_column_instance`
- `latest_predicate_depends_on_literal`
- `latest_literal_flows_to_predicate`

## 5. 模型对象到点边的映射关系

### 5.1 `ExecutionCaptureEvent` 映射

| 输入对象字段 | 点/边 | 说明 |
|---|---|---|
| `event_id` + 执行信息 | `capture_event` | 记录一次采集事件 |
| `task_context.task_id` | `task_node` | 若有 task_id 则 upsert |
| `task_context.run_id` | `run_node` | 若有 run_id 则 upsert |
| task -> run | `task_has_run` | 任务与运行关联 |
| task -> capture | `task_has_execution` | 任务与采集事件关联 |
| run -> capture | `run_has_execution` | 运行与采集事件关联 |

### 5.2 `NormalizedLineageResult` 映射

| 输入集合 | 点 | 边 |
|---|---|---|
| `inputTables` | `table_node` | `execution_reads_table` |
| `outputTables` | `table_node` | `execution_writes_table` |
| `columnNodes` | `column_node` | `execution_emits_column` +（条件满足时）`table_has_column` |
| `expressionNodes` | `expression_node` | `execution_observes_expression` |
| `scopeNodes` | `scope_node` | `execution_observes_scope` |
| `literalNodes` | `literal_node` | `execution_observes_literal` |
| `operatorInstanceNodes` | `operator_instance_node` | `execution_observes_operator_instance` |
| `columnInstanceNodes` | `column_instance_node` | `execution_observes_column_instance` |
| `relationInstanceNodes` | `relation_instance_node` | `execution_observes_relation_instance` |
| `predicateNodes` | `predicate_node` | `execution_observes_predicate` |

### 5.3 `TableLineageEdge` 映射

| 映射 | 方向 | 作用 |
|---|---|---|
| `table_flows_to_table` | 源表 -> 目标表 | 保留事件级表流转事实 |
| `latest_table_flows_to_table` | 源表 -> 目标表 | 覆盖为最新表流转 |
| `task_reads_table` | task -> 源表 | 任务读表关系 |
| `task_writes_table` | task -> 目标表 | 任务写表关系 |

### 5.4 `LineageGraphEdge.edgeType` 映射

| `edgeType` | 映射边 |
|---|---|
| `SCOPE_TO_SCOPE` | `scope_contains_scope` |
| `SCOPE_TO_COLUMN` | `scope_outputs_column` |
| `SCOPE_TO_EXPRESSION` | `scope_uses_expression` |
| `SCOPE_TO_OPERATOR_INSTANCE` | `scope_contains_operator_instance` |
| `OPERATOR_TO_OPERATOR_INSTANCE` | `operator_precedes_operator_instance` |
| `OPERATOR_TO_COLUMN_INSTANCE` | `operator_outputs_column_instance` |
| `OPERATOR_TO_EXPRESSION` | `operator_uses_expression` |
| `OPERATOR_TO_PREDICATE` | `operator_uses_predicate` |
| `OPERATOR_TO_RELATION_INSTANCE` | `operator_reads_relation_instance` |
| `SCOPE_TO_COLUMN_INSTANCE` | `scope_contains_column_instance` |
| `SCOPE_TO_RELATION_INSTANCE` | `scope_reads_relation_instance` |
| `RELATION_INSTANCE_TO_COLUMN_INSTANCE` | `relation_instance_exposes_column_instance` |
| `RELATION_INSTANCE_TO_TABLE` | `relation_instance_of_table` |
| `COLUMN_TO_COLUMN_INSTANCE` | `column_has_instance` |
| `SCOPE_TO_PREDICATE` | `scope_uses_predicate` |
| `RELATION_INSTANCE_TO_PREDICATE` | `relation_instance_filtered_by_predicate` |
| `RELATION_INSTANCE_TO_RELATION_INSTANCE` | `relation_instance_joins_relation_instance` + `latest_relation_instance_joins_relation_instance` |
| `COLUMN_TO_COLUMN` | `column_depends_on_column` + `latest_column_depends_on_column` + `latest_column_flows_to_column` |
| `COLUMN_TO_PREDICATE` | `predicate_depends_on_column` + `latest_predicate_depends_on_column` + `latest_column_flows_to_predicate` |
| `COLUMN_INSTANCE_TO_PREDICATE` | `predicate_depends_on_column_instance` + `latest_predicate_depends_on_column_instance` + `latest_column_instance_flows_to_predicate` |
| `RELATION_INSTANCE_TO_DERIVED_COLUMN_INSTANCE` | `column_instance_depends_on_relation_instance` + `latest_column_instance_depends_on_relation_instance` + `latest_relation_instance_flows_to_column_instance` |
| `PREDICATE_TO_DERIVED_COLUMN_INSTANCE` | `column_instance_filtered_by_predicate` + `latest_column_instance_filtered_by_predicate` + `latest_predicate_flows_to_column_instance` |
| `EXPRESSION_TO_COLUMN_INSTANCE` | `column_instance_uses_expression` + `latest_column_instance_uses_expression` + `latest_expression_flows_to_column_instance` |
| `EXPRESSION_TO_COLUMN` | `column_uses_expression` + `latest_column_uses_expression` + `latest_expression_flows_to_column` |
| `COLUMN_TO_EXPRESSION` | `expression_depends_on_column` + `latest_expression_depends_on_column` + `latest_column_flows_to_expression` |
| `COLUMN_INSTANCE_TO_EXPRESSION` | `expression_depends_on_column_instance` + `latest_expression_depends_on_column_instance` + `latest_column_instance_flows_to_expression` |
| `COLUMN_INSTANCE_TO_COLUMN_INSTANCE` | `column_instance_depends_on_column_instance` + `latest_column_instance_depends_on_column_instance` + `latest_column_instance_flows_to_column_instance` |
| `EXPRESSION_TO_EXPRESSION` | `expression_depends_on_expression` + `latest_expression_depends_on_expression` + `latest_expression_flows_to_expression` |
| `LITERAL_TO_EXPRESSION` | `expression_depends_on_literal` + `latest_expression_depends_on_literal` + `latest_literal_flows_to_expression` |
| `LITERAL_TO_PREDICATE` | `predicate_depends_on_literal` + `latest_predicate_depends_on_literal` + `latest_literal_flows_to_predicate` |

## 6. 关系方向规则（重点）

结构边和流向边通常是“source -> target”。

但依赖边中有一个关键规则：

- `putDependencyAndFlowEdge` 会把“依赖事实边”写成 `target -> source`。
- 同时把“latest_flow”写成 `source -> target`。

这意味着：

- `*_depends_on_*` 读取时语义是“左侧节点依赖右侧节点”。
- `latest_*flows_to_*` 读取时语义是“数据从左侧流向右侧”。

## 7. ID 与 Rank 规则

### 7.1 主要 VID 规则

- `task_node`：`task:{taskId}`
- `run_node`：`run:{runId}`
- `capture_event`：`capture:{eventId}`
- `table_node`：`table:{normalizedTableName}`
- 其它语义节点：使用模型内自带 `nodeId`

### 7.2 Rank 规则

- 普通事实边 rank：`edgeRank(...)`，基于 `sha1` 前缀生成 long。
- latest 语义边 rank：使用 `latestSemanticRank(edgeName, role)`，同角色覆盖更新。
- `table_has_column` 固定 rank `0`。

## 8. CSV 与 Spark 导入字段映射

### 8.1 CSV 列顺序

- 点 CSV：`[vid, property1, property2, ...]`
- 边 CSV：`[src, dst, rank, property1, property2, ...]`

### 8.2 Spark Schema 对齐

- 点导入固定读取 `vid` 字段，并设置 `VERTEX_FIELD=vid`。
- 边导入固定读取 `src/dst/rank`，并设置 `SRC_VERTEX_FIELD`、`DST_VERTEX_FIELD`、`RANK_FIELD`。
- 类型映射：
  - `int` -> Spark `IntegerType`
  - `timestamp` / `long` -> Spark `LongType`
  - 其它 -> Spark `StringType`
- 空值占位：`__ZZ_LINEAGE_NULL__`。

## 9. 幂等与覆盖语义

- bundle 内部去重键：
  - 点：`vid`
  - 边：`src + dst + rank`
- 对于同键重复写入：
  - 点会被后写覆盖（同 session 内 map 覆盖）
  - 边也会被后写覆盖
- 导入到 Nebula 时：
  - Spark Connector 配置 `overwrite=true`（可配置）
  - latest 边通过稳定 rank 实现“同语义最新覆盖”

## 10. 当前实现差异与注意点

### 10.1 Schema 差异

`NebulaLineageStorage.bootstrapSchema()` 中包含：

- `table_depends_on_table`
- `latest_table_depends_on_table`

但 `NebulaImporterBundleWriter.EDGE_DEFINITIONS` 未定义这两类边，因此当前 bundle 导入链路不会产出它们。

### 10.2 双写路径差异

项目内存在两条写图路径：

- 在线 nGQL 写入：`NebulaLineageStorage`
- 离线 bundle + Spark Connector 写入：`NebulaImporterBundleWriter` + `SparkConnectorImport*`

两者模型大体一致，但应持续对齐 schema 列表，避免边类型漂移。

## 11. 推荐查询路径（落地使用）

### 11.1 表级来源追溯

优先用：

- `latest_table_flows_to_table`

必要时回看事件明细：

- `table_flows_to_table`

### 11.2 字段级依赖解释

组合查询：

- 依赖事实：`column_depends_on_column` / `expression_depends_on_column` / `predicate_depends_on_column*`
- 最新主链：`latest_column_flows_to_column` / `latest_column_flows_to_expression` / `latest_expression_flows_to_column`
- 结构补充：`scope_*`、`operator_*`、`relation_instance_*`

### 11.3 条件口径解释

重点边：

- `predicate_depends_on_literal`
- `latest_literal_flows_to_predicate`
- `relation_instance_filtered_by_predicate`
- `column_instance_filtered_by_predicate`

这条链可以回答“某字段结果是否受某常量条件影响”。
