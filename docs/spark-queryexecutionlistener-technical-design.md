# Spark QueryExecutionListener 技术设计文档

## 1. 文档目标

本文档基于需求文档，进一步定义基于 Spark `QueryExecutionListener` 的采集、解析、建模与入库方案，目标是把“表级血缘、字段级血缘、口径沉淀、自增长图谱”落成可开发的技术设计。

对应需求文档见 [warehouse-lineage-requirements.md](./warehouse-lineage-requirements.md)。

## 2. 设计范围

本文档重点覆盖以下内容：

- Spark 侧监听器接入方式
- 执行事件采集模型
- 逻辑计划与表达式解析策略
- 表级 / 字段级血缘构建方案
- 图谱入库与幂等更新策略
- 最小可用版本实施路径

本文档暂不展开：

- 最终图数据库选型的采购或部署细节
- 问答服务的 Prompt 设计
- 前端展示实现细节

## 3. 设计原则

- 以执行结果为准：只基于真实执行到的 Spark 查询沉淀血缘。
- 以逻辑计划为主：血缘解析优先使用逻辑计划，物理计划仅作为调试与补充。
- 以可解释为目标：字段血缘保留表达式节点，不退化为纯字段到字段关系。
- 以增量更新为默认：每次任务执行都是一次图谱增量补充。
- 以回放为兜底：原始采集事件必须可持久化，支持后续重放解析。
- 以版本适配隔离变化：Spark SQL 内部 API 存在版本差异，解析层必须做适配封装。

## 4. 关键约束与假设

### 4.1 Spark 版本约束

- `QueryExecutionListener` 属于 Spark SQL 的 Developer API。
- Spark 3.x 与 4.x 的内部计划节点命名、部分字段和树结构存在差异。
- 第一版建议以 Spark 3.5.x 作为主适配版本。
- 设计上需通过 `spark-adapter` 层封装版本差异，避免解析逻辑散落在业务代码中。

说明：官方文档确认 `QueryExecutionListener` 提供 `onSuccess` 和 `onFailure` 两个回调，并强调实现需要线程安全。

### 4.2 任务元数据注入假设

仅靠 `QueryExecutionListener` 无法完整表达调度任务语义，因此需要外部任务上下文注入，至少包括：

- `task_id`
- `task_name`
- `run_id`
- `biz_date`
- `owner`
- `script_path`

推荐通过以下方式之一注入：

- SparkConf
- Spark local properties
- 启动作业时的环境变量并在驱动端转入上下文

### 4.3 采集边界假设

- 一次任务运行中可能触发多个 `QueryExecution` 事件。
- 并非所有事件都需要入图，重点是与数仓表读写相关的事件。
- 临时视图、中间结果、缓存命中等事件需要区分是否入图库。

## 5. 总体架构

整体采用“四段式流水线”：

1. Listener Capture  
   在 Spark 驱动端监听执行事件，采集原始上下文。

2. Plan Parse  
   解析逻辑计划和表达式树，构建标准化血缘对象。

3. Snapshot Persist  
   将原始事件和解析结果写入快照存储，保证可回放和幂等。

4. Graph Upsert  
   将标准化对象增量写入图谱存储，生成节点、边和聚合视图。

## 6. 模块拆分

建议按以下模块拆分：

- `lineage-spark-listener`
  - 负责监听器注册、任务上下文读取、原始事件封装。
- `lineage-spark-adapter`
  - 封装 Spark 版本差异，输出统一计划访问接口。
- `lineage-parser`
  - 负责计划扫描、表达式解析、字段血缘构建、口径摘要生成。
- `lineage-model`
  - 定义事件模型、图谱模型、标准 DTO。
- `lineage-storage`
  - 负责快照落库、图谱 upsert、幂等控制。
- `lineage-service`
  - 后续提供查询接口，不属于第一版采集核心链路。

## 7. Spark 侧接入设计

### 7.1 注册方式

推荐在 Spark 应用初始化时，通过 `listenerManager.register(...)` 注册自定义 `QueryExecutionListener`。

接入原则：

- 注册行为在 Driver 端完成。
- Listener 本身只做轻量采集和异步投递，不做重解析。
- Listener 实现必须线程安全。

### 7.2 监听事件

系统需要同时处理：

- `onSuccess(funcName, qe, durationNs)`
- `onFailure(funcName, qe, exception)`

其中：

- `onSuccess` 用于主血缘构建。
- `onFailure` 用于失败事件留痕、问题定位和后续解析调试，不直接更新“最新可用血缘”。

### 7.3 采集字段

每个执行事件建议采集以下元数据：

- Spark 应用信息
  - `spark_app_id`
  - `spark_user`
  - `spark_master`
- 任务上下文
  - `task_id`
  - `task_name`
  - `run_id`
  - `biz_date`
  - `owner`
  - `script_path`
- 执行上下文
  - `func_name`
  - `duration_ns`
  - `capture_time`
  - `status`
  - `error_message`
- 计划快照
  - `logical_plan_text`
  - `analyzed_plan_text`
  - `optimized_plan_text`
  - `executed_plan_text`
- 解析辅助信息
  - 输入表集合
  - 输出表集合
  - 计划类型
  - 语句类型

### 7.4 原始 SQL 文本采集策略

`QueryExecutionListener` 可以稳定拿到 `QueryExecution`，但不保证所有场景都能直接提供原始 SQL 文本。设计上采用以下策略：

- 优先采集任务脚本路径和任务标识。
- 如果调用链明确来自 `spark.sql(...)`，可在上层封装器中补充原始 SQL。
- 若无法拿到原始 SQL，不阻塞血缘构建，仍以逻辑计划为准。

结论：原始 SQL 是增强信息，不作为第一版主依赖。

## 8. 采集事件标准模型

为支持异步解析与回放，监听器输出应先标准化为 `ExecutionCaptureEvent`。

### 8.1 核心结构

```text
ExecutionCaptureEvent
├── event_id
├── task_context
├── execution_context
├── raw_plan_snapshot
├── table_hints
├── output_hints
└── parse_status
```

### 8.2 唯一键设计

不建议仅依赖 Spark 内部执行对象地址做幂等键。推荐采用组合键：

- `run_id`
- `statement_fingerprint`
- `capture_status`

其中：

- `run_id` 来自调度系统或外部上下文注入。
- `statement_fingerprint` 由规范化逻辑计划、目标表集合、动作名组成。
- `capture_status` 取值如 `SUCCESS`、`FAILURE`。

建议事件主键：

`event_id = sha1(run_id + func_name + statement_fingerprint + capture_status)`

如果后续确认 Spark 版本中存在可稳定暴露的 SQL 执行 ID，可将其纳入主键，但设计上不强依赖。

## 9. 计划解析总体策略

### 9.1 为什么以逻辑计划为主

物理计划更接近执行优化结果，但常常丢失一部分字段别名、子查询边界和表达式原貌。为了同时兼顾准确性与可解释性，采用双计划策略：

- `analyzed plan`
  - 用于字段血缘主解析
  - 保留较完整的属性引用、别名和子查询语义
- `optimized plan`
  - 用于表达式标准化、去重和归一化
- `executed plan`
  - 用于调试、性能分析和异常排查

### 9.2 解析流程

1. 识别语句类型和写入目标。
2. 从计划树提取来源表和目标表。
3. 构建子查询作用域。
4. 建立输出字段与 `NamedExpression` 的映射关系。
5. 递归解析 `NamedExpression` 的输入依赖。
6. 生成字段、表达式、子查询之间的 DAG。
7. 输出表级血缘、字段级血缘、技术口径摘要。

## 10. 表级血缘解析设计

### 10.1 输入表识别

输入表识别基于逻辑计划叶子节点进行，重点适配以下关系节点：

- Hive 表关系
- `LogicalRelation`
- `DataSourceV2Relation`
- 带别名的 `SubqueryAlias`
- 视图或临时视图包装节点

处理规则：

- 如果是实际物理表，落 `Table` 节点。
- 如果是临时视图或 CTE，中间层落 `Subquery` 节点，不直接当成真实表。
- 如果计划最终来自外部文件系统但未注册表，第一版可先记为“外部数据源关系”，不强制映射到数仓表。

### 10.2 输出表识别

输出表识别需要根据写入语句节点进行分类，第一版重点支持：

- `INSERT INTO`
- `INSERT OVERWRITE`
- `CREATE TABLE AS SELECT`
- `REPLACE TABLE AS SELECT`
- DataFrame `saveAsTable`

处理原则：

- 一次执行可能对应一个或多个目标表。
- 如果目标是临时视图，不作为正式表级血缘终点。
- 仅当识别到真实目标表时，才更新正式表级上下游图。

### 10.3 表级边生成

生成规则如下：

- 每个目标表与每个来源表之间生成 `TABLE_LINEAGE` 边。
- 边上挂接以下属性：
  - `task_id`
  - `run_id`
  - `event_id`
  - `write_mode`
  - `capture_time`
  - `confidence`
- 同时建立：
  - `Task -> Table` 的 `READS_FROM`
  - `Task -> Table` 的 `WRITES_TO`
  - `Execution -> Table` 的实例级依赖边

## 11. 字段级血缘解析设计

### 11.1 建模目标

字段级血缘不是简单输出“目标字段依赖来源字段”，而是保留完整推导链：

```text
源字段 -> 表达式 -> 中间字段/子查询字段 -> 表达式 -> 目标字段
```

这样可以同时满足：

- 技术追溯
- 可视化展示
- 口径摘要生成
- 大模型知识供给

### 11.2 解析核心

字段解析基于输出字段对应的 `NamedExpression` 递归展开。

主要处理对象：

- `AttributeReference`
- `Alias`
- `Literal`
- `Cast`
- `CaseWhen`
- `If`
- 算术表达式
- 比较表达式
- `AggregateExpression`
- `WindowExpression`
- 常见内建函数

### 11.3 递归解析规则

#### 规则一：属性引用

- 当表达式为 `AttributeReference` 时，视为叶子输入。
- 根据限定名、`exprId`、当前作用域解析到来源字段节点。

#### 规则二：别名

- 当表达式为 `Alias(child, name)` 时，创建一个别名表达式节点。
- 目标字段先依赖该别名表达式，再继续解析 `child`。

#### 规则三：函数或运算

- 对函数、算术、比较、聚合、窗口等表达式创建 `Expression` 节点。
- 将其子表达式递归展开。
- 表达式节点记录：
  - 原始表达式文本
  - 归一化表达式文本
  - 表达式类型
  - 聚合 / 窗口等附加配置

#### 规则四：常量

- `Literal` 不生成来源字段边。
- 若目标字段完全由常量生成，记录“无来源字段，来源类型为常量表达式”。

#### 规则五：条件表达式

- `CASE WHEN` / `IF` 既依赖条件字段，也依赖结果分支字段。
- 为便于后续问答，可在边属性中标注依赖角色：
  - `condition`
  - `value`

#### 规则六：聚合表达式

- 聚合结果字段依赖聚合输入字段。
- 如果需要支持更强解释性，可额外记录分组键字段为“上下文依赖”。

#### 规则七：窗口表达式

- 窗口函数输出依赖：
  - 函数输入字段
  - 分区键
  - 排序键
- 其中分区键和排序键建议在边属性中标为 `window_context`。

### 11.4 子查询处理

子查询是字段血缘完整性的关键。设计上采用“显式子查询节点”策略：

- 每个 `SubqueryAlias` 或 CTE 生成一个 `Subquery` 节点。
- 子查询输出字段单独生成 `Column` 节点，归属该 `Subquery`。
- 外层查询字段依赖子查询字段，子查询字段再继续向下解析。

这样可以避免：

- 中间重命名后链路断裂
- 多层嵌套后来源字段不可追溯
- 展示时丢失“中间处理层”的业务语义

### 11.5 Join 处理

Join 处理分两部分：

- 输出字段来源解析
  - 输出字段来自左表还是右表，由 `AttributeReference` 指向决定。
- 关联条件解析
  - Join 条件中的字段不一定是目标字段直接值依赖，但对业务口径有价值。

第一版建议：

- 字段血缘主链仅保留值依赖。
- Join 条件额外作为表达式上下文记录在子查询或计划节点摘要中。

### 11.6 UDF 处理

UDF 是字段血缘难点。第一版建议：

- 保留 UDF 表达式节点。
- 记录函数名与参数字段。
- 将 UDF 视为黑盒表达式，不尝试展开内部逻辑。
- 对 UDF 解析结果打上较低 `confidence`。

## 12. 表达式标准化设计

为支持去重、比较和问答摘要，表达式需要标准化。

标准化规则建议包括：

- 去除无意义空白和格式差异
- 统一函数名大小写
- 统一限定名表现形式
- 对可交换运算按稳定顺序排序
- 对常量值做脱敏或归一化表示

需要注意：

- 标准化用于去重和索引，不替代原始表达式文本。
- 原始文本和标准化文本都需要保留。

## 13. 图模型落地设计

### 13.1 推荐双层存储

为了同时满足可回放和查询效率，建议采用双层存储：

1. 快照层  
   存储原始采集事件与标准化解析结果，建议 append-only。

2. 图谱层  
   存储最新可查询的节点和边，支持增量 upsert。

这样做的好处：

- 解析规则迭代后可基于快照重放。
- 图谱层可以专注查询性能。
- 原始数据和图谱视图解耦。

### 13.2 节点唯一键建议

- `Task`
  - `task_id`
- `Execution`
  - `run_id`
- `Table`
  - `catalog.db.table`
- `Column`
  - `catalog.db.table.column`
- `Subquery`
  - `event_id + subquery_id`
- `Expression`
  - `event_id + expression_hash + expression_role`

### 13.3 边唯一键建议

边唯一键建议包含：

- `src_key`
- `dst_key`
- `edge_type`
- `event_id`

如果图谱层维护“最新视图”，则可额外维护一张聚合边：

- `latest_src_key`
- `latest_dst_key`
- `edge_type`
- `latest_event_id`
- `last_seen_time`
- `seen_count`

## 14. 入库与幂等策略

### 14.1 Listener 与存储解耦

不要在 `onSuccess` / `onFailure` 中直接同步写图库。推荐流程：

1. Listener 生成 `ExecutionCaptureEvent`
2. 投递到内存队列或轻量缓冲层
3. 后台 worker 完成解析与入库

这样可以降低对 Spark 主流程的影响。

### 14.2 幂等写入

幂等分两层：

- 快照层幂等
  - 相同 `event_id` 只写一次
- 图谱层幂等
  - 相同节点按唯一键 upsert
  - 相同边按唯一键 upsert

### 14.3 最新视图更新

图谱层建议区分：

- `execution snapshot`
  - 保存每次执行事实
- `latest lineage view`
  - 聚合任务或表的最新可用关系

更新规则：

- 新执行到达后先写快照。
- 如果解析成功且置信度满足阈值，再刷新最新视图。
- 失败事件不覆盖最新成功血缘。

## 15. 置信度与质量标记

每条字段血缘和表级边都建议打质量标：

- `HIGH`
  - 常规投影、别名、聚合、窗口、子查询均成功解析
- `MEDIUM`
  - 存在部分黑盒表达式，但主链路清晰
- `LOW`
  - UDF、复杂自定义逻辑较多，或部分字段无法完全定位

同时保留以下质量字段：

- `parse_warning_count`
- `unresolved_attribute_count`
- `contains_udf`
- `contains_temp_view`

## 16. 口径摘要生成设计

字段和子查询的口径沉淀建议在解析完成后顺带生成，不额外重复遍历计划树。

### 16.1 技术口径摘要

针对字段或指标，生成结构化摘要：

- 输出字段名
- 最终表达式
- 来源字段列表
- 聚合方式
- 过滤条件摘要
- 分组维度
- 窗口定义

### 16.2 子查询 / 表级业务摘要

第一版不追求自动生成自然语言，而是先沉淀结构化摘要：

- 子查询读取哪些表
- 做了哪些过滤
- 做了哪些聚合
- 产出哪些关键字段

后续再在知识服务层将结构化摘要转换为自然语言说明。

## 17. 建议代码结构

基于当前 Maven Java 项目，建议后续代码按以下 package 组织：

```text
com.zhuanzhuan.lineage
├── app
├── spark
│   ├── listener
│   ├── adapter
│   └── context
├── parser
│   ├── plan
│   ├── expr
│   ├── lineage
│   └── summary
├── model
│   ├── capture
│   ├── normalized
│   └── graph
├── storage
│   ├── snapshot
│   ├── graph
│   └── idempotency
└── common
```

## 18. 最小可用版本实施建议

### 18.1 第一迭代

目标：打通从 Spark 执行到表级血缘入库的最短链路。

- 注册 `QueryExecutionListener`
- 采集成功事件
- 识别输入表 / 输出表
- 生成表级边
- 落快照层和最新视图

### 18.2 第二迭代

目标：支持基础字段映射。

- 解析 `Project`
- 解析 `Alias`
- 解析 `AttributeReference`
- 支持目标字段回溯到来源字段

### 18.3 第三迭代

目标：增强表达式可解释性。

- 增加 `Expression` 节点
- 支持 `CaseWhen`
- 支持聚合与窗口
- 支持显式子查询节点

### 18.4 第四迭代

目标：补足口径与问答底座。

- 生成技术口径摘要
- 沉淀子查询结构化摘要
- 输出问答上下文组装接口

## 19. 测试设计

### 19.1 单元测试

- 表级来源 / 去向提取测试
- 字段表达式递归解析测试
- 子查询作用域解析测试
- 表达式标准化测试

### 19.2 集成测试

基于本地 SparkSession 构造典型 SQL：

- 简单投影
- 多表 Join
- 聚合
- 窗口函数
- 多层子查询
- `INSERT OVERWRITE TABLE`

断言输出：

- 输入表集合
- 输出表集合
- 目标字段链路
- 表达式节点数量

### 19.3 回放测试

快照层保留的原始事件可用于构造回放测试：

- 升级解析规则后回放旧事件
- 比较解析前后差异
- 验证幂等更新

## 20. 风险与未决项

- `QueryExecutionListener` 是 Developer API，Spark 升级可能带来适配成本。
- 原始 SQL 文本不一定稳定可得，需要任务侧补充。
- UDF 无法在第一版做到强语义展开。
- 不同数据源和写表命令的逻辑计划形态可能不同，需要逐步补齐适配。
- 若直接依赖图库查询，早期可能受索引与建模策略影响，需要预留快照重算能力。

## 21. 下一步输出建议

基于本文档，建议继续补两份设计：

1. 图谱存储与查询模型设计文档
2. MVP 实施清单与开发任务拆解

## 22. 参考资料

- Spark 官方 `QueryExecutionListener` 文档：
  - https://spark.apache.org/docs/3.5.7/api/java/org/apache/spark/sql/util/QueryExecutionListener.html
  - https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/util/QueryExecutionListener.html
- Spark 官方 `ExecutionListenerManager` 文档：
  - https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/util/ExecutionListenerManager.html
