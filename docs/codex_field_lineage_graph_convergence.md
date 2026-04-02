# 字段口径解析与图收敛设计文档（Codex 实施版）

## 1. 文档目标

本文档用于指导 Codex 对当前基于 Spark 执行计划解析并写入 Nebula 的字段级血缘图进行收敛改造。

本次改造只服务一个核心目标：

> 给定某次执行中的目标输出字段，能够递归追踪其所有上游参与字段，并保留当前版本下完整的字段口径解释信息，包括表达式、条件、常量、关联关系与必要的来源上下文。

本文档是**目标态实施说明**，不是当前实现说明。

---

## 2. 本次目标边界

### 2.1 In Scope

本次只做以下能力：

1. **只保留当前版本，不保留历史版本**
2. **只服务字段口径解释，不做审计与历史回放**
3. **只保留能支撑字段解释主链的点边**
4. **查询入口固定为输出字段实例（column instance）**
5. **查询方向统一为上游 -> 下游（flow）**

### 2.2 Out of Scope

本次明确不做：

1. 历史版本血缘回放
2. 任务级、run 级、execution 级审计查询
3. Spark 原始执行计划的 1:1 完整存档
4. 复杂 runtime metrics / AQE / exchange / broadcast 等物理计划归档
5. 以 table / column 语义节点为主入口的模糊追溯

---

## 3. 当前问题总结

当前图模型的问题不在于“信息太少”，而在于“语义太杂”。现状同时混合了：

1. 执行上下文边
2. 执行观测边
3. 结构边
4. 事实边
5. latest 边

直接结果：

- 字段主链查询容易串到无关节点
- 查询路径混入结构边和观测边，产生杂音
- fact 与 latest 双轨并存，方向体系不统一
- 当前目标只是“当前口径解释”，但模型仍背着历史事实与执行上下文负担
- `depends_on` 与 `flows_to` 同时存在，增加查询复杂度

因此本次收敛的核心原则是：

> 从“全语义 provenance 图”收敛为“当前有效字段口径解释图”。

---

## 4. 目标态总原则

### 4.1 只保留一套 Current Flow 主边

不再保留 fact + latest 双套体系。

目标态只保留一套**当前有效边**，逻辑上等价于现在的 latest flow 边，但不再保留“latest 只是辅助视图”的概念。

建议统一命名为 `current_*`；如果改名成本过高，可暂时沿用 `latest_*flows_to_*`，但语义上将其视为唯一主边。

### 4.2 查询统一采用 instance-first

主查询入口不是 `column_node`，而是：

- 某次解析结果中的目标输出 `column_instance_node`

原因：

- 字段口径解释天然依赖上下文
- 同名字段可能出现在多个 scope / relation / branch 中
- 直接从抽象 `column_node` 起查容易串义

### 4.3 统一使用 source -> target 方向

目标态不再保留 `*_depends_on_*` 这类 target -> source 风格的边。

统一为：

- 上游字段/常量/关系/表达式/条件 -> 下游表达式/字段

这样查询、可视化和递归展开都会更简单。

### 4.4 主链与解释链分层，但仍在同一张 current 图中

主链负责回答：

- 字段值从哪里来

解释链负责回答：

- 为什么这条值成立
- 受哪些条件影响
- 来自哪个 relation instance
- 是否经过 join / case when / coalesce / filter 等逻辑

但两者都使用 current flow 边体系，不再通过结构边绕路。

---

## 5. 目标态点模型

## 5.1 主图保留点

### A. `column_instance_node`

主入口节点，表示字段在某个上下文中的实例。

建议保留属性：

- `node_id`
- `column_id`
- `column_name`
- `scope_id`
- `relation_instance_id`
- `instance_type`
- `data_type`
- `ordinal`
- `instance_role`（新增，见下文）

建议新增属性：

- `instance_role`
  - `source`
  - `derived`
  - `output`
  - `group_key`
  - `agg_input`
  - `window_input`
- `is_output`
- `branch_id`（可选）

---

### B. `expression_node`

表示派生逻辑、函数、case when、算术逻辑、聚合逻辑、窗口逻辑等。

建议保留属性：

- `node_id`
- `expression_type`
- `expression_sql`
- `normalized_expression`

建议新增属性：

- `expression_category`
  - `projection`
  - `arithmetic`
  - `case_when`
  - `aggregate`
  - `window`
  - `cast`
  - `coalesce`
  - `udf`
- `is_black_box`
- `display_sql`

---

### C. `predicate_node`

表示 where / join on / having / qualify / branch condition 等条件逻辑。

建议保留属性：

- `node_id`
- `predicate_type`
- `predicate_sql`
- `normalized_predicate`
- `scope_id`

建议新增属性：

- `predicate_role`
  - `where`
  - `join_on`
  - `having`
  - `qualify`
  - `branch_condition`
  - `exists_condition`
- `display_sql`

---

### D. `literal_node`

表示常量、默认值、枚举值、参数值。

建议保留属性：

- `node_id`
- `literal_type`
- `literal_value`
- `normalized_value`

建议新增属性：

- `literal_role`
  - `constant`
  - `default_value`
  - `enum_value`
  - `runtime_param`

---

### E. `relation_instance_node`

表示某个表/子查询/别名在当前上下文中的读取实例。

建议保留属性：

- `node_id`
- `instance_name`
- `scope_id`
- `source_table_id`
- `source_type`
- `alias_name`
- `plan_node_name`

建议新增属性：

- `join_type`
- `null_supply_side`
- `is_subquery_source`

---

## 5.2 展示归属点（可保留，但不参与主推理）

### F. `column_node`

用途：

- 展示抽象字段语义
- 做实例归并展示

原则：

- 不再作为字段解释主查询入口
- 不再依赖它承载主推理路径

### G. `table_node`

用途：

- 展示表级归属
- 最终 UI 展示与跳转

原则：

- 不参与主链递归

---

## 5.3 从主图剥离的点

以下点不再参与当前字段口径解释主图：

- `task_node`
- `run_node`
- `capture_event`
- `scope_node`
- `operator_instance_node`

处理原则：

- 可保留在旁路调试图或元数据表中
- 不参与字段口径主链查询
- 若当前实现高度耦合，可先保留入图但从主查询层彻底屏蔽

---

## 6. 目标态边模型

## 6.1 必保留主边（Current Flow）

以下边构成字段口径解释主骨架。

### 1. 字段到表达式

`current_column_instance_flows_to_expression`

语义：

- 某字段实例作为输入，进入某个表达式

用途：

- 保留直接值输入
- 表示表达式依赖哪些字段实例

来源：

- 由现有 `latest_column_instance_flows_to_expression` 收编而来

---

### 2. 表达式到字段

`current_expression_flows_to_column_instance`

语义：

- 某表达式产出某字段实例

用途：

- 保留字段的直接计算规则

来源：

- 由现有 `latest_expression_flows_to_column_instance` 收编而来

---

### 3. 字段到字段

`current_column_instance_flows_to_column_instance`

语义：

- 某字段实例直接透传或映射到另一个字段实例

用途：

- 表达 select 透传、alias 透传、简单映射

来源：

- 由现有 `latest_column_instance_flows_to_column_instance` 收编而来

---

### 4. 字段到条件

`current_column_instance_flows_to_predicate`

语义：

- 某字段实例参与某个条件逻辑

用途：

- 表示 where / join on / having 等条件的字段输入

来源：

- 由现有 `latest_column_instance_flows_to_predicate` 收编而来

---

### 5. 常量到条件

`current_literal_flows_to_predicate`

语义：

- 某常量参与某个条件逻辑

用途：

- 回答“字段结果是否受某常量条件影响”

来源：

- 由现有 `latest_literal_flows_to_predicate` 收编而来

---

### 6. 条件到字段

`current_predicate_flows_to_column_instance`

语义：

- 某条件对某个派生字段实例产生控制/过滤影响

用途：

- 让解释结果中能明确呈现条件依赖链

来源：

- 由现有 `latest_predicate_flows_to_column_instance` 收编而来

---

### 7. 常量到表达式

`current_literal_flows_to_expression`

语义：

- 某常量参与某表达式计算

用途：

- 支持 case when 常量、coalesce 默认值、枚举值等解释

来源：

- 由现有 `latest_literal_flows_to_expression` 收编而来

---

### 8. 关系实例到字段

`current_relation_instance_flows_to_column_instance`

语义：

- 某字段实例来自某个 relation instance 的读取上下文

用途：

- 解释字段来自哪个输入别名/子查询/表读取实例

来源：

- 由现有 `latest_relation_instance_flows_to_column_instance` 收编而来

---

### 9. 关系实例到关系实例

`current_relation_instance_joins_relation_instance`

语义：

- 两个 relation instance 之间存在 join 关系

用途：

- 解释字段上下文来自哪些输入关系的 join

来源：

- 由现有 `latest_relation_instance_joins_relation_instance` 收编而来

---

## 6.2 可选保留的展示辅助边

以下边不是主链必需，但如前端展示确有需要，可保留为辅助边：

- `relation_instance_of_table`
- `column_has_instance`
- `table_has_column`

优先建议：

- 能转属性则转属性
- 不纳入默认递归

---

## 6.3 必须删除或从主图剥离的边

### A. 删除全部 Fact 边

删除原因：

- 当前版本不要历史
- fact 用于保留事件级依赖事实
- 会导致无用历史边不断累积
- 与 current flow 在当前目标下形成重复表达

删除范围：

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

---

### B. 删除全部执行上下文边

删除范围：

- `task_has_run`
- `task_has_execution`
- `run_has_execution`
- `task_reads_table`
- `task_writes_table`
- `execution_reads_table`
- `execution_writes_table`

删除原因：

- 这些边服务任务/run/execution 追踪
- 与字段口径主链无关
- 容易引入表级或任务级绕路噪声

---

### C. 删除全部执行观测边

删除范围：

- `execution_emits_column`
- `execution_observes_expression`
- `execution_observes_scope`
- `execution_observes_literal`
- `execution_observes_operator_instance`
- `execution_observes_column_instance`
- `execution_observes_relation_instance`
- `execution_observes_predicate`

删除原因：

- 表达的是“本次执行看到了什么”，不是“字段由什么构成”
- 是字段解释查询的主要噪声来源之一

---

### D. 删除绝大多数结构边

删除范围：

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
- `scope_uses_predicate`
- `relation_instance_exposes_column_instance`
- `relation_instance_filtered_by_predicate`（如已能通过 current predicate 主链解释，可删除）

删除原因：

- 这些边主要表达 SQL 内部结构导航
- 不直接表达字段解释主链
- 会显著增加多跳时的噪声与歧义

---

### E. 删除全部 `*_depends_on_*` latest 边

删除原因：

- 当前模型只保留一种方向体系
- `depends_on` 是 target -> source
- `flow` 是 source -> target
- 两套同时保留会增加查询复杂度并导致语义重复

删除范围包括但不限于：

- `latest_column_depends_on_column`
- `latest_expression_depends_on_column`
- `latest_expression_depends_on_expression`
- `latest_expression_depends_on_literal`
- `latest_expression_depends_on_column_instance`
- `latest_predicate_depends_on_column`
- `latest_predicate_depends_on_column_instance`
- `latest_column_instance_depends_on_relation_instance`
- `latest_column_instance_filtered_by_predicate`
- `latest_predicate_depends_on_literal`
- `latest_column_instance_depends_on_column_instance`

---

## 7. 建议的属性替代策略

以下关系优先改为属性，而不是继续保留边：

### 7.1 `column_has_instance`

不再需要单独边。

改用：

- `column_instance_node.column_id`

### 7.2 `relation_instance_of_table`

不再需要单独边。

改用：

- `relation_instance_node.source_table_id`

### 7.3 `table_has_column`

如果 UI 只是展示字段归属，可由字段元数据服务或离线表元数据补充，不强依赖边。

---

## 8. 目标态查询返回契约

给定一个目标输出字段实例，返回的不是“若干边”，而是一份结构化解释结果。

建议返回对象：

```json
{
  "target": {
    "column_instance_id": "...",
    "column_name": "a",
    "table": "...",
    "relation_alias": "...",
    "data_type": "..."
  },
  "value_path": {
    "direct_expression": {
      "expression_id": "...",
      "expression_sql": "case when ... then ... end",
      "expression_category": "case_when"
    },
    "direct_inputs": [
      {
        "type": "column_instance",
        "id": "...",
        "column_name": "create_time",
        "source_relation": "t2"
      },
      {
        "type": "literal",
        "id": "...",
        "value": "0"
      }
    ]
  },
  "conditions": [
    {
      "predicate_id": "...",
      "predicate_role": "where",
      "predicate_sql": "b.state in (3,4) and b.pay_money > 0",
      "inputs": [
        {"type": "column_instance", "column_name": "state", "source_relation": "b"},
        {"type": "column_instance", "column_name": "pay_money", "source_relation": "b"},
        {"type": "literal", "value": "3"},
        {"type": "literal", "value": "4"},
        {"type": "literal", "value": "0"}
      ]
    }
  ],
  "relations": [
    {
      "relation_instance_id": "...",
      "alias_name": "t2",
      "source_table": "db.table",
      "join_type": "left"
    }
  ],
  "upstream": [
    {
      "column_instance_id": "...",
      "column_name": "create_time",
      "explain": "direct value source"
    }
  ]
}
```

---

## 9. 口径解释的语义分类要求

为避免“参与计算”语义混乱，Codex 必须在结果组装层显式区分以下角色：

### 9.1 值来源

- 直接提供值的字段实例
- 通过 `current_column_instance_flows_to_expression`
- 或 `current_column_instance_flows_to_column_instance`
- 或 `current_relation_instance_flows_to_column_instance` 识别

### 9.2 表达式来源

- 通过 `current_expression_flows_to_column_instance` 识别
- 必须输出表达式原文

### 9.3 条件来源

- 通过 `current_column_instance_flows_to_predicate`
- `current_literal_flows_to_predicate`
- `current_predicate_flows_to_column_instance`
- 识别为条件控制链

### 9.4 常量来源

- `current_literal_flows_to_expression`
- `current_literal_flows_to_predicate`

### 9.5 关系来源

- `current_relation_instance_flows_to_column_instance`
- `current_relation_instance_joins_relation_instance`

注意：

- join key、group key、window order 等更细粒度角色，如当前解析器尚未产出，可在本期先不强制入图，但结果结构需预留扩展位。

---

## 10. 迁移实施方案

## Phase 1：模型收敛（必须）

1. 定义目标态 current 边集合
2. 停止写入 Fact 边
3. 停止写入执行上下文边
4. 停止写入执行观测边
5. 停止写入绝大多数结构边
6. 停止写入所有 latest depends_on 边
7. 保留 current flow 主边

如果短期内不便改名，可先：

- 物理上继续产出 `latest_*flows_to_*`
- 逻辑上把它们视为 current 主边
- 查询层只允许访问 flow 白名单

---

## Phase 2：属性补强（强烈建议）

为提升解释质量，需要补充如下属性：

### `expression_node`

- `expression_category`
- `display_sql`
- `is_black_box`

### `predicate_node`

- `predicate_role`
- `display_sql`

### `relation_instance_node`

- `join_type`
- `null_supply_side`

### `column_instance_node`

- `instance_role`
- `is_output`

---

## Phase 3：查询层收敛（必须）

查询层只允许以下边参与递归：

- `current_column_instance_flows_to_expression`
- `current_expression_flows_to_column_instance`
- `current_column_instance_flows_to_column_instance`
- `current_column_instance_flows_to_predicate`
- `current_literal_flows_to_predicate`
- `current_predicate_flows_to_column_instance`
- `current_literal_flows_to_expression`
- `current_relation_instance_flows_to_column_instance`
- `current_relation_instance_joins_relation_instance`

查询层禁止默认访问：

- task/run/execution 相关边
- scope/operator 相关边
- 任何 `depends_on` 边
- 任何 fact 边
- 非白名单结构边

---

## Phase 4：结果组装层（必须）

实现一个解释组装器，将图遍历结果转为结构化字段口径解释对象，而不是直接返回“边列表”。

建议组件名：

- `FieldExplainAssembler`
- 或 `ColumnLineageExplainService`

输出必须包含：

1. 目标字段实例
2. 直接表达式
3. 直接值输入
4. 条件输入
5. 常量输入
6. 来源 relation instance
7. 递归上游字段列表

---

## 11. 导入与幂等要求

当前图已有通过 rank 覆盖更新的思路。目标态下保留“覆盖当前有效关系”的机制，但不再保留历史事实。

实施要求：

1. 同语义 current 边必须稳定覆盖
2. 不允许同一语义长期累积多条历史边
3. 对旧 fact / observe / execution / scope/operator 边提供清理脚本
4. 若短期内沿用 `latest_*flows_to_*` 物理表名，需保证写入逻辑只保留这一套

---

## 12. 验收标准

以下标准满足后，视为本次收敛完成。

### 12.1 模型层验收

1. 主图中不再写入 fact 边
2. 主图中不再写入 execution 上下文边
3. 主图中不再写入 execution 观测边
4. 主图中不再写入 latest depends_on 边
5. 主图只保留 current flow 白名单边

### 12.2 查询层验收

给定目标输出字段实例，查询结果：

1. 能返回直接表达式
2. 能返回直接值来源字段实例
3. 能返回参与条件的字段与常量
4. 能返回来源 relation instance
5. 递归展开时不会绕到 task/run/execution/scope/operator 节点
6. 多跳结果噪声显著降低

### 12.3 解释层验收

对于典型 SQL 场景，至少支持正确解释：

1. 直接字段透传
2. alias 映射
3. case when
4. coalesce
5. cast
6. where 条件
7. join on 条件
8. 常量比较条件
9. 简单多跳派生链

---

## 13. 给 Codex 的明确实施任务

### Task 1：收敛边定义

修改边定义与写入逻辑，只保留 current flow 白名单边。

### Task 2：停写无关边

在 bundle writer / online writer 中停写：

- fact
- execution context
- execution observe
- majority structure
- latest depends_on

### Task 3：补充属性

为 expression / predicate / relation_instance / column_instance 增加当前文档要求的属性。

### Task 4：实现 explain 查询服务

实现按 `output column instance` 为入口的解释查询服务，禁止默认走非白名单边。

### Task 5：实现结果组装器

输出结构化字段口径解释 JSON，而不是原始图边列表。

### Task 6：清理旧图

提供一次性清理脚本或迁移方案，删除旧 fact / observe / execution / depends_on 边。

---

## 14. 给 Codex 的补充约束

1. 不要再新增“历史版本”能力
2. 不要把 execution 维度重新引回主图
3. 不要继续扩张 scope/operator 结构边
4. 不要混用 flow 与 depends_on 两种方向
5. 查询层必须基于白名单边递归
6. 任何新增点边都必须证明其对“字段口径解释”有直接价值

---

## 15. 最终结论

本次目标态不是构建一张“全量 Spark provenance 图”，而是构建一张：

> **只保留当前版本、只服务字段口径解释、以字段实例为主入口、以 current flow 为唯一方向体系的解释图。**

换句话说：

- 从“全语义图”收敛到“当前解释图”
- 从“fact + latest 双轨”收敛到“single current flow”
- 从“column 语义入口”收敛到“column instance 入口”
- 从“图边直接暴露”收敛到“结构化字段解释结果”

这就是本次 Codex 实施的唯一目标。
