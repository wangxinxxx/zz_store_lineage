# 完整口径图模型设计

## 1. 文档目标

本文档定义“完整指标计算口径”所需的图模型，并说明当前代码已实现的范围。

目标不是只回答“字段依赖哪些字段”，而是进一步回答：

- 指标在哪个脚本、哪个子查询、哪个分支里被计算
- 同一张表被读取多次时，每次读取的语义是否不同
- 常量赋值、过滤条件、`UNION ALL` 分支如何参与指标计算
- 如何从 `ads` 一路解释到 `dm/dwd/ods`

对应已有文档：

- [warehouse-lineage-requirements.md](./warehouse-lineage-requirements.md)
- [spark-queryexecutionlistener-technical-design.md](./spark-queryexecutionlistener-technical-design.md)

## 2. 为什么需要完整口径图模型

当前仅靠“表 / 字段 / 表达式”三类对象，已经可以回答：

- 某字段直接依赖哪些上游字段
- 某个复杂字段的大致表达式链是什么

但还不能无歧义回答：

- 这两个聚合是不是来自同一张表的不同读取分支
- `'pay' as order_type` 是哪个 `UNION ALL` 分支注入的
- `refund_type in (1,2)`、`pay_time`、`refund_create_time` 这类条件属于哪个作用域

因此，完整口径图模型必须在“字段 / 表达式层”之上，再补一层“查询上下文层”。

## 3. 设计原则

- 血缘主链仍以物理表字段为主，不把图完全做成 SQL AST。
- 口径上下文要能表达 `CTE`、子查询、`UNION ALL` 分支和常量条件。
- 结构化信息优先入图，业务解释文本由图数据二次生成。
- 图模型分层设计，先落最小可用的上下文层，再补充更细粒度实例层。

## 4. 总体分层

完整口径图模型分三层。

### 4.1 实体语义层

负责回答“谁依赖谁”。

节点：

- `table_node`
- `column_node`
- `expression_node`

边：

- `table_flows_to_table`
- `column_depends_on_column`
- `column_uses_expression`
- `expression_depends_on_column`
- `expression_depends_on_expression`
- 对应 `latest_*` 和 `flows_*` 视图边

### 4.2 查询上下文层

负责回答“这段计算发生在什么作用域、用了哪些常量”。

节点：

- `scope_node`
  - 表示 `ROOT_QUERY`、`CTE`、`SUBQUERY_ALIAS`、`UNION_BRANCH`
- `literal_node`
  - 表示 `'pay'`、`'refund'`、`'线上导流'`、`0` 这类常量
- `column_instance_node`
  - 表示某个字段在某个 `scope` / `relation_instance` 下的一次上下文实例

边：

- `scope_contains_scope`
- `scope_outputs_column`
- `scope_uses_expression`
- `scope_contains_column_instance`
- `column_has_instance`
- `expression_depends_on_literal`
- `latest_expression_depends_on_literal`
- `latest_literal_flows_to_expression`

### 4.3 读取实例层

负责回答“同一张物理表被读了几次，每次读的条件是什么”。

当前已实现节点：

- `relation_instance_node`
  - 某张表在某个作用域里的一次读取实例
- `predicate_node`
  - `where/on/if/case` 中的条件逻辑
- `operator_instance_node`
  - 某个 `Project/Aggregate/Filter/Join/Union` 等逻辑算子在某个作用域中的一次实例
  - 当前已补 `operator_sub_type`，例如 `leftouter`、`inner`

当前已实现边：

- `scope_reads_relation_instance`
- `relation_instance_of_table`
- `relation_instance_joins_relation_instance`
- `relation_instance_filtered_by_predicate`
- `predicate_depends_on_column`
- `predicate_depends_on_literal`
- `column_instance_depends_on_relation_instance`
- `column_instance_filtered_by_predicate`
- `scope_contains_operator_instance`
- `operator_precedes_operator_instance`
- `operator_outputs_column_instance`
- `operator_uses_expression`
- `operator_uses_predicate`
- `operator_reads_relation_instance`

当前代码已经补上了“输出字段实例到读取实例 / 谓词”的直接归因边，并开始把字段、表达式、谓词绑定到具体算子实例上。但在同表多别名、多次读取同时穿透多层投影时，仍然需要继续增强 alias-aware 精度。

## 5. 节点设计

### 5.1 已实现节点

#### `scope_node`

属性：

- `scope_name`
- `scope_type`
- `parent_scope_id`
- `plan_node_name`

当前 `scope_type`：

- `ROOT_QUERY`
- `CTE`
- `SUBQUERY_ALIAS`
- `UNION_BRANCH`

VID 规则：

- `scope:` + `sha1(event_id | scope_type | path | scope_name)`

说明：

- `scope` 是查询上下文对象，不做跨脚本复用，因此以执行事件为边界生成 ID。

#### `literal_node`

属性：

- `literal_type`
- `literal_value`
- `normalized_value`

VID 规则：

- `literal:` + `sha1(literal_type | normalized_value)`

说明：

- 字面量是可复用对象，同一常量允许跨脚本复用顶点。

#### `column_instance_node`

属性：

- `column_id`
- `column_name`
- `scope_id`
- `relation_instance_id`
- `instance_type`
- `data_type`
- `ordinal`

当前 `instance_type`：

- `OUTPUT`
- `EXPRESSION_INPUT`
- `PREDICATE_INPUT`

VID 规则：

- `column_instance:` + `sha1(scope_id | relation_instance_id | column_id | instance_type | ordinal)`

说明：

- `column_instance` 不替代 `column_node`，而是补充“字段在具体查询上下文里的实例化出现”。
- 同一逻辑字段在不同 `UNION_BRANCH`、不同 `scope`、不同读取实例下会产生不同 `column_instance`。

### 5.2 已实现读取实例节点

#### `relation_instance_node`

属性：

- `instance_name`
- `alias_name`
- `source_table_id`
- `scope_id`
- `source_type`
- `plan_node_name`

#### `predicate_node`

属性：

- `predicate_sql`
- `predicate_type`
- `normalized_predicate`
- `scope_id`
- `plan_node_name`

## 6. 边设计

### 6.1 已实现边

#### 结构边

- `scope_contains_scope`
  - `scope -> scope`
- `scope_outputs_column`
  - `scope -> column`
- `scope_uses_expression`
  - `scope -> expression`
- `scope_contains_column_instance`
  - `scope -> column_instance`
- `column_has_instance`
  - `column -> column_instance`
- `scope_contains_operator_instance`
  - `scope -> operator_instance`
- `operator_precedes_operator_instance`
  - `operator_instance -> operator_instance`

#### 口径依赖边

- `expression_depends_on_literal`
  - 依赖语义：`expression -> literal`
- `latest_expression_depends_on_literal`
  - 当前有效依赖视图
- `latest_literal_flows_to_expression`
  - 可视化流向：`literal -> expression`
- `expression_depends_on_column_instance`
  - 依赖语义：`expression -> column_instance`
- `predicate_depends_on_column_instance`
  - 依赖语义：`predicate -> column_instance`
- `latest_column_instance_flows_to_expression`
  - 可视化流向：`column_instance -> expression`
- `latest_column_instance_flows_to_predicate`
  - 可视化流向：`column_instance -> predicate`
- `column_instance_depends_on_relation_instance`
  - 依赖语义：`column_instance -> relation_instance`
- `latest_relation_instance_flows_to_column_instance`
  - 可视化流向：`relation_instance -> column_instance`
- `column_instance_filtered_by_predicate`
  - 依赖语义：`column_instance -> predicate`
- `latest_predicate_flows_to_column_instance`
  - 可视化流向：`predicate -> column_instance`
- `operator_outputs_column_instance`
  - 结构语义：`operator_instance -> column_instance`
- `operator_uses_expression`
  - 结构语义：`operator_instance -> expression`
- `operator_uses_predicate`
  - 结构语义：`operator_instance -> predicate`
- `operator_reads_relation_instance`
  - 结构语义：`operator_instance -> relation_instance`
- `column_instance_uses_expression`
  - 依赖语义：`column_instance -> expression`
- `latest_expression_flows_to_column_instance`
  - 可视化流向：`expression -> column_instance`
- `column_instance_depends_on_column_instance`
  - 依赖语义：`column_instance -> column_instance`
- `latest_column_instance_flows_to_column_instance`
  - 可视化流向：`column_instance -> column_instance`

#### 读取实例 / 谓词边

- `scope_reads_relation_instance`
- `relation_instance_of_table`
- `relation_instance_filtered_by_predicate`
- `predicate_depends_on_column`
- `predicate_depends_on_literal`

## 7. 典型案例

以：

- `hdp_ubu_zhuanzhuan_ads_c2b.ads_bi_offline_store_operating_data_center_v3_full_1d.zonghe_online_rec_net_gmv`

为例，完整口径希望表达成：

1. 最外层 `ROOT_QUERY` 输出 `zonghe_online_rec_net_gmv`
2. 指标来自 `dm_offline_store_income_data_full_1d.online_rec_net_gmv`
3. 该字段在 `t2` / `t_recycle_net` / `t_recycle` 等作用域中逐层传播
4. `rec_online_order_gmv` 和 `refund_rec_online_order_gmv` 分别来自不同逻辑分支
5. 分支中存在：
   - 常量赋值：`'pay'`、`'refund'`
   - 常量过滤：`'线上导流'`
   - 金额基底字段：`total_real_price`

本次实现后，图中已经能结构化表达：

- 作用域层级
- 作用域产出的中间列
- 表达式依赖的常量
- 字段在 `scope` / `relation_instance` 下的上下文实例
- 字段实例与表达式、谓词之间的绑定关系
- 输出字段实例直接归因到读取实例和谓词实例
- 输出字段实例挂到具体算子实例

还不能结构化表达的部分：

- 同一张表在同一 `scope` 下多次读取且带不同别名时的精确字段绑定
- 完整、可执行级 SQL 的无歧义重建

## 8. 本次实现范围

本次代码实现了“完整口径图模型”的前两阶段，新增：

- `scope_node`
- `literal_node`
- `column_instance_node`
- `relation_instance_node`
- `predicate_node`
- `operator_instance_node`
- `scope_contains_scope`
- `scope_outputs_column`
- `scope_uses_expression`
- `scope_contains_column_instance`
- `column_has_instance`
- `scope_reads_relation_instance`
- `relation_instance_of_table`
- `scope_uses_predicate`
- `relation_instance_filtered_by_predicate`
- `scope_contains_operator_instance`
- `operator_precedes_operator_instance`
- `operator_outputs_column_instance`
- `operator_uses_expression`
- `operator_uses_predicate`
- `operator_reads_relation_instance`
- `expression_depends_on_literal`
- `latest_expression_depends_on_literal`
- `latest_literal_flows_to_expression`
- `expression_depends_on_column_instance`
- `column_instance_uses_expression`
- `latest_expression_flows_to_column_instance`
- `column_instance_depends_on_column_instance`
- `latest_column_instance_flows_to_column_instance`
- `predicate_depends_on_column_instance`
- `latest_column_instance_flows_to_expression`
- `latest_column_instance_flows_to_predicate`
- `column_instance_depends_on_relation_instance`
- `latest_relation_instance_flows_to_column_instance`
- `column_instance_filtered_by_predicate`
- `latest_predicate_flows_to_column_instance`

解析层新增能力：

- 自动识别 `ROOT_QUERY`
- 自动识别 `CTE`
- 自动识别 `SUBQUERY_ALIAS`
- 自动识别 `UNION_BRANCH`
- 自动采集表达式中的 `Literal`
- 自动生成 `scope output / expression input / predicate input` 三类字段实例
- 自动采集 `relation_instance` 和 `predicate`
- 自动采集 `operator_instance`
- 自动把输出字段实例挂到表达式主链上
- 自动把输出字段实例直连到上游 `relation_instance` 和 `predicate`
- 将 `scope / literal / column_instance / relation_instance / predicate` 一并写入 `NormalizedLineageResult`

落库层新增能力：

- Nebula 自动建 `scope_node` / `literal_node`
- `saveLineage` 自动写入 scope / literal 顶点
- 自动写入结构边和 literal 依赖边

## 9. 当前代码映射

主要实现位置：

- 解析模型：
  - `src/main/java/com/zhuanzhuan/lineage/model/ScopeNode.java`
  - `src/main/java/com/zhuanzhuan/lineage/model/LiteralNode.java`
  - `src/main/java/com/zhuanzhuan/lineage/model/NormalizedLineageResult.java`
- 解析逻辑：
  - `src/main/java/com/zhuanzhuan/lineage/parser/DefaultSparkLineageParser.java`
- Nebula 落库：
  - `src/main/java/com/zhuanzhuan/lineage/storage/nebula/NebulaLineageStorage.java`

## 10. 下一步建议

如果目标是“纯图数据自动生成完整指标口径文本”，下一阶段应优先补：

1. `column_instance -> relation_instance` 的 alias-aware 精确绑定
2. 输出字段实例到分支级谓词的最终归因
3. 同表多次读取场景下的读取实例消歧
4. 可执行级 SQL 片段重建

做到这一步后，图数据库才能原生回答：

- 同一张表为什么会出现两次
- 哪一次代表支付分支，哪一次代表退款分支
- 每个分支的过滤条件到底是什么
