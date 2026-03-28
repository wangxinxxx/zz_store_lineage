package com.zhuanzhuan.lineage.demo;

import com.zhuanzhuan.lineage.model.ColumnNode;
import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExecutionStatus;
import com.zhuanzhuan.lineage.model.ExpressionNode;
import com.zhuanzhuan.lineage.model.LineageGraphEdge;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.RawPlanSnapshot;
import com.zhuanzhuan.lineage.model.SparkAppContext;
import com.zhuanzhuan.lineage.model.TableLineageEdge;
import com.zhuanzhuan.lineage.model.TableRef;
import com.zhuanzhuan.lineage.storage.LineageStorage;
import com.zhuanzhuan.lineage.storage.LineageStorageFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class ComplexLineageDemo {
    private ComplexLineageDemo() {
    }

    public static void main(String[] args) {
        DemoScenario scenario = buildScenario();
        LineageStorage storage = LineageStorageFactory.createDefault();
        try {
            storage.saveCapture(scenario.captureEvent);
            storage.saveLineage(scenario.result);

            runAssertions(scenario);
            printSummary(storage, scenario);
        } finally {
            closeQuietly(storage);
        }
    }

    private static DemoScenario buildScenario() {
        String eventId = "demo-complex-lineage-event-001";
        long captureTimeEpochMs = System.currentTimeMillis();
        String taskId = "ads_shop_trade_metric_di";
        String runId = "manual-demo-run-001";

        TableRef orderTable = new TableRef(null, "dwd", "fact_order_detail", "table");
        TableRef paymentTable = new TableRef(null, "dwd", "fact_payment_detail", "table");
        TableRef shopDimTable = new TableRef(null, "dim", "dim_shop_snapshot", "table");
        TableRef targetTable = new TableRef(null, "ads", "ads_shop_trade_metric_di", "table");

        ColumnNode orderBizDate = column("dwd.fact_order_detail", "TABLE_OR_SUBQUERY", "dt", "string", "dwd", "fact_order_detail");
        ColumnNode orderShopId = column("dwd.fact_order_detail", "TABLE_OR_SUBQUERY", "shop_id", "bigint", "dwd", "fact_order_detail");
        ColumnNode orderId = column("dwd.fact_order_detail", "TABLE_OR_SUBQUERY", "order_id", "bigint", "dwd", "fact_order_detail");
        ColumnNode orderUserId = column("dwd.fact_order_detail", "TABLE_OR_SUBQUERY", "user_id", "bigint", "dwd", "fact_order_detail");
        ColumnNode orderStatus = column("dwd.fact_order_detail", "TABLE_OR_SUBQUERY", "order_status", "string", "dwd", "fact_order_detail");
        ColumnNode paymentOrderId = column("dwd.fact_payment_detail", "TABLE_OR_SUBQUERY", "order_id", "bigint", "dwd", "fact_payment_detail");
        ColumnNode paymentAmount = column("dwd.fact_payment_detail", "TABLE_OR_SUBQUERY", "pay_amount", "decimal(18,2)", "dwd", "fact_payment_detail");
        ColumnNode paymentTime = column("dwd.fact_payment_detail", "TABLE_OR_SUBQUERY", "pay_time", "timestamp", "dwd", "fact_payment_detail");
        ColumnNode shopDimShopId = column("dim.dim_shop_snapshot", "TABLE_OR_SUBQUERY", "shop_id", "bigint", "dim", "dim_shop_snapshot");
        ColumnNode shopDimName = column("dim.dim_shop_snapshot", "TABLE_OR_SUBQUERY", "shop_name", "string", "dim", "dim_shop_snapshot");

        ColumnNode baseBizDate = column("subquery:base_order", "TABLE_OR_SUBQUERY", "biz_date", "string", "base_order");
        ColumnNode baseShopId = column("subquery:base_order", "TABLE_OR_SUBQUERY", "shop_id", "bigint", "base_order");
        ColumnNode baseOrderId = column("subquery:base_order", "TABLE_OR_SUBQUERY", "order_id", "bigint", "base_order");
        ColumnNode baseUserId = column("subquery:base_order", "TABLE_OR_SUBQUERY", "user_id", "bigint", "base_order");
        ColumnNode basePayAmount = column("subquery:base_order", "TABLE_OR_SUBQUERY", "pay_amount", "decimal(18,2)", "base_order");
        ColumnNode baseIsPaid = column("subquery:base_order", "TABLE_OR_SUBQUERY", "is_paid", "int", "base_order");

        ColumnNode latestBizDate = column("subquery:latest_paid_order", "TABLE_OR_SUBQUERY", "biz_date", "string", "latest_paid_order");
        ColumnNode latestShopId = column("subquery:latest_paid_order", "TABLE_OR_SUBQUERY", "shop_id", "bigint", "latest_paid_order");
        ColumnNode latestOrderId = column("subquery:latest_paid_order", "TABLE_OR_SUBQUERY", "order_id", "bigint", "latest_paid_order");
        ColumnNode latestUserId = column("subquery:latest_paid_order", "TABLE_OR_SUBQUERY", "user_id", "bigint", "latest_paid_order");
        ColumnNode latestPayAmount = column("subquery:latest_paid_order", "TABLE_OR_SUBQUERY", "pay_amount", "decimal(18,2)", "latest_paid_order");
        ColumnNode latestIsPaid = column("subquery:latest_paid_order", "TABLE_OR_SUBQUERY", "is_paid", "int", "latest_paid_order");

        ColumnNode shopAggBizDate = column("subquery:shop_agg", "TABLE_OR_SUBQUERY", "biz_date", "string", "shop_agg");
        ColumnNode shopAggShopId = column("subquery:shop_agg", "TABLE_OR_SUBQUERY", "shop_id", "bigint", "shop_agg");
        ColumnNode shopAggOrderCnt = column("subquery:shop_agg", "TABLE_OR_SUBQUERY", "order_cnt", "bigint", "shop_agg");
        ColumnNode shopAggPaidOrderCnt = column("subquery:shop_agg", "TABLE_OR_SUBQUERY", "paid_order_cnt", "bigint", "shop_agg");
        ColumnNode shopAggPaidGmv = column("subquery:shop_agg", "TABLE_OR_SUBQUERY", "paid_gmv", "decimal(18,2)", "shop_agg");
        ColumnNode shopAggBuyerCnt = column("subquery:shop_agg", "TABLE_OR_SUBQUERY", "buyer_cnt", "bigint", "shop_agg");

        ColumnNode targetBizDate = column("ads.ads_shop_trade_metric_di", "TABLE", "biz_date", "string", "ads", "ads_shop_trade_metric_di");
        ColumnNode targetShopId = column("ads.ads_shop_trade_metric_di", "TABLE", "shop_id", "bigint", "ads", "ads_shop_trade_metric_di");
        ColumnNode targetShopName = column("ads.ads_shop_trade_metric_di", "TABLE", "shop_name", "string", "ads", "ads_shop_trade_metric_di");
        ColumnNode targetOrderCnt = column("ads.ads_shop_trade_metric_di", "TABLE", "order_cnt", "bigint", "ads", "ads_shop_trade_metric_di");
        ColumnNode targetPaidOrderCnt = column("ads.ads_shop_trade_metric_di", "TABLE", "paid_order_cnt", "bigint", "ads", "ads_shop_trade_metric_di");
        ColumnNode targetPaidGmv = column("ads.ads_shop_trade_metric_di", "TABLE", "paid_gmv", "decimal(18,2)", "ads", "ads_shop_trade_metric_di");
        ColumnNode targetBuyerCnt = column("ads.ads_shop_trade_metric_di", "TABLE", "buyer_cnt", "bigint", "ads", "ads_shop_trade_metric_di");
        ColumnNode targetPayRate = column("ads.ads_shop_trade_metric_di", "TABLE", "pay_rate", "decimal(10,4)", "ads", "ads_shop_trade_metric_di");
        ColumnNode targetGmvBand = column("ads.ads_shop_trade_metric_di", "TABLE", "gmv_band", "string", "ads", "ads_shop_trade_metric_di");

        ExpressionNode coalescePayAmount = expression(
                "expr:coalesce_pay_amount",
                "coalesce",
                "COALESCE(p.pay_amount, 0D)",
                "coalesce(p.pay_amount, 0d)"
        );
        ExpressionNode caseIsPaid = expression(
                "expr:case_is_paid",
                "casewhen",
                "CASE WHEN o.order_status IN ('PAID', 'FINISHED') AND p.pay_time IS NOT NULL THEN 1 ELSE 0 END",
                "case when o.order_status in ('paid','finished') and p.pay_time is not null then 1 else 0 end"
        );
        ExpressionNode countOrderCnt = expression(
                "expr:count_distinct_order_cnt",
                "aggregate",
                "COUNT(DISTINCT latest_paid_order.order_id)",
                "count(distinct latest_paid_order.order_id)"
        );
        ExpressionNode casePaidOrderId = expression(
                "expr:case_paid_order_id",
                "casewhen",
                "CASE WHEN latest_paid_order.is_paid = 1 THEN latest_paid_order.order_id ELSE NULL END",
                "case when latest_paid_order.is_paid = 1 then latest_paid_order.order_id else null end"
        );
        ExpressionNode countPaidOrderCnt = expression(
                "expr:count_distinct_paid_order_cnt",
                "aggregate",
                "COUNT(DISTINCT CASE WHEN latest_paid_order.is_paid = 1 THEN latest_paid_order.order_id END)",
                "count(distinct case when latest_paid_order.is_paid = 1 then latest_paid_order.order_id end)"
        );
        ExpressionNode sumPaidGmv = expression(
                "expr:sum_paid_gmv",
                "aggregate",
                "SUM(latest_paid_order.pay_amount)",
                "sum(latest_paid_order.pay_amount)"
        );
        ExpressionNode countBuyerCnt = expression(
                "expr:count_distinct_buyer_cnt",
                "aggregate",
                "COUNT(DISTINCT latest_paid_order.user_id)",
                "count(distinct latest_paid_order.user_id)"
        );
        ExpressionNode dividePayRate = expression(
                "expr:divide_pay_rate",
                "divide",
                "shop_agg.paid_order_cnt / shop_agg.order_cnt",
                "shop_agg.paid_order_cnt / shop_agg.order_cnt"
        );
        ExpressionNode roundPayRate = expression(
                "expr:round_pay_rate",
                "round",
                "ROUND(shop_agg.paid_order_cnt / shop_agg.order_cnt, 4)",
                "round(shop_agg.paid_order_cnt / shop_agg.order_cnt, 4)"
        );
        ExpressionNode caseGmvBand = expression(
                "expr:case_gmv_band",
                "casewhen",
                "CASE WHEN shop_agg.paid_gmv >= 100000 THEN 'S' WHEN shop_agg.paid_gmv >= 10000 THEN 'A' WHEN shop_agg.paid_gmv >= 1000 THEN 'B' ELSE 'C' END",
                "case when shop_agg.paid_gmv >= 100000 then 's' when shop_agg.paid_gmv >= 10000 then 'a' when shop_agg.paid_gmv >= 1000 then 'b' else 'c' end"
        );

        List<ColumnNode> columns = Arrays.asList(
                orderBizDate, orderShopId, orderId, orderUserId, orderStatus,
                paymentOrderId, paymentAmount, paymentTime, shopDimShopId, shopDimName,
                baseBizDate, baseShopId, baseOrderId, baseUserId, basePayAmount, baseIsPaid,
                latestBizDate, latestShopId, latestOrderId, latestUserId, latestPayAmount, latestIsPaid,
                shopAggBizDate, shopAggShopId, shopAggOrderCnt, shopAggPaidOrderCnt, shopAggPaidGmv, shopAggBuyerCnt,
                targetBizDate, targetShopId, targetShopName, targetOrderCnt, targetPaidOrderCnt, targetPaidGmv, targetBuyerCnt, targetPayRate, targetGmvBand
        );

        List<ExpressionNode> expressions = Arrays.asList(
                coalescePayAmount, caseIsPaid, countOrderCnt, casePaidOrderId, countPaidOrderCnt,
                sumPaidGmv, countBuyerCnt, dividePayRate, roundPayRate, caseGmvBand
        );

        List<LineageGraphEdge> graphEdges = Arrays.asList(
                edge(orderBizDate, baseBizDate, "COLUMN_TO_COLUMN", "value", eventId),
                edge(orderShopId, baseShopId, "COLUMN_TO_COLUMN", "value", eventId),
                edge(orderId, baseOrderId, "COLUMN_TO_COLUMN", "value", eventId),
                edge(orderUserId, baseUserId, "COLUMN_TO_COLUMN", "value", eventId),
                edge(paymentAmount, coalescePayAmount, "COLUMN_TO_EXPRESSION", "value", eventId),
                edge(coalescePayAmount, basePayAmount, "EXPRESSION_TO_COLUMN", "value", eventId),
                edge(orderStatus, caseIsPaid, "COLUMN_TO_EXPRESSION", "condition", eventId),
                edge(paymentTime, caseIsPaid, "COLUMN_TO_EXPRESSION", "condition", eventId),
                edge(caseIsPaid, baseIsPaid, "EXPRESSION_TO_COLUMN", "value", eventId),

                edge(baseBizDate, latestBizDate, "COLUMN_TO_COLUMN", "value", eventId),
                edge(baseShopId, latestShopId, "COLUMN_TO_COLUMN", "value", eventId),
                edge(baseOrderId, latestOrderId, "COLUMN_TO_COLUMN", "value", eventId),
                edge(baseUserId, latestUserId, "COLUMN_TO_COLUMN", "value", eventId),
                edge(basePayAmount, latestPayAmount, "COLUMN_TO_COLUMN", "value", eventId),
                edge(baseIsPaid, latestIsPaid, "COLUMN_TO_COLUMN", "value", eventId),

                edge(latestBizDate, shopAggBizDate, "COLUMN_TO_COLUMN", "group_key", eventId),
                edge(latestShopId, shopAggShopId, "COLUMN_TO_COLUMN", "group_key", eventId),

                edge(latestOrderId, countOrderCnt, "COLUMN_TO_EXPRESSION", "aggregate_input", eventId),
                edge(countOrderCnt, shopAggOrderCnt, "EXPRESSION_TO_COLUMN", "value", eventId),
                edge(latestIsPaid, casePaidOrderId, "COLUMN_TO_EXPRESSION", "condition", eventId),
                edge(latestOrderId, casePaidOrderId, "COLUMN_TO_EXPRESSION", "true_value", eventId),
                edge(casePaidOrderId, countPaidOrderCnt, "EXPRESSION_TO_EXPRESSION", "aggregate_input", eventId),
                edge(countPaidOrderCnt, shopAggPaidOrderCnt, "EXPRESSION_TO_COLUMN", "value", eventId),
                edge(latestPayAmount, sumPaidGmv, "COLUMN_TO_EXPRESSION", "aggregate_input", eventId),
                edge(sumPaidGmv, shopAggPaidGmv, "EXPRESSION_TO_COLUMN", "value", eventId),
                edge(latestUserId, countBuyerCnt, "COLUMN_TO_EXPRESSION", "aggregate_input", eventId),
                edge(countBuyerCnt, shopAggBuyerCnt, "EXPRESSION_TO_COLUMN", "value", eventId),

                edge(shopAggBizDate, targetBizDate, "COLUMN_TO_COLUMN", "value", eventId),
                edge(shopAggShopId, targetShopId, "COLUMN_TO_COLUMN", "value", eventId),
                edge(shopDimName, targetShopName, "COLUMN_TO_COLUMN", "value", eventId),
                edge(shopAggOrderCnt, targetOrderCnt, "COLUMN_TO_COLUMN", "value", eventId),
                edge(shopAggPaidOrderCnt, targetPaidOrderCnt, "COLUMN_TO_COLUMN", "value", eventId),
                edge(shopAggPaidGmv, targetPaidGmv, "COLUMN_TO_COLUMN", "value", eventId),
                edge(shopAggBuyerCnt, targetBuyerCnt, "COLUMN_TO_COLUMN", "value", eventId),
                edge(shopAggPaidOrderCnt, dividePayRate, "COLUMN_TO_EXPRESSION", "numerator", eventId),
                edge(shopAggOrderCnt, dividePayRate, "COLUMN_TO_EXPRESSION", "denominator", eventId),
                edge(dividePayRate, roundPayRate, "EXPRESSION_TO_EXPRESSION", "value", eventId),
                edge(roundPayRate, targetPayRate, "EXPRESSION_TO_COLUMN", "value", eventId),
                edge(shopAggPaidGmv, caseGmvBand, "COLUMN_TO_EXPRESSION", "condition", eventId),
                edge(caseGmvBand, targetGmvBand, "EXPRESSION_TO_COLUMN", "value", eventId)
        );

        List<TableLineageEdge> tableEdges = Arrays.asList(
                new TableLineageEdge(orderTable, targetTable, taskId, runId, eventId, "OVERWRITE", captureTimeEpochMs, "HIGH"),
                new TableLineageEdge(paymentTable, targetTable, taskId, runId, eventId, "OVERWRITE", captureTimeEpochMs, "HIGH"),
                new TableLineageEdge(shopDimTable, targetTable, taskId, runId, eventId, "OVERWRITE", captureTimeEpochMs, "HIGH")
        );

        ExecutionCaptureEvent captureEvent = new ExecutionCaptureEvent(
                eventId,
                ExecutionStatus.SUCCESS,
                new LineageTaskContext(taskId, "ads_shop_trade_metric_di", runId, "2026-03-19", "bi-platform", "/warehouse/ads/ads_shop_trade_metric_di.sql"),
                new SparkAppContext("local-demo-app", "complex-lineage-demo", "codex", "local[*]"),
                "insertOverwriteAdsShopTradeMetric",
                135_000_000L,
                captureTimeEpochMs,
                null,
                new RawPlanSnapshot(loadSql(), loadSql(), "", "")
        );

        NormalizedLineageResult result = new NormalizedLineageResult(
                eventId,
                captureTimeEpochMs,
                "INSERT_OVERWRITE",
                Arrays.asList(orderTable, paymentTable, shopDimTable),
                Collections.singletonList(targetTable),
                tableEdges,
                columns,
                expressions,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                graphEdges,
                Collections.emptyList()
        );

        return new DemoScenario(
                loadSql(),
                captureEvent,
                result,
                targetPayRate,
                targetGmvBand,
                orderId,
                orderStatus,
                paymentTime,
                paymentAmount
        );
    }

    private static void runAssertions(DemoScenario scenario) {
        NormalizedLineageResult result = scenario.result;

        assertTrue(result.getInputTables().size() == 3, "Expected three physical input tables.");
        assertTrue(result.getOutputTables().size() == 1, "Expected one output table.");
        assertTrue(result.getTableEdges().size() == 3, "Expected three table-level lineage edges.");
        assertTrue(result.getColumnNodes().size() >= 30, "Expected at least thirty column nodes in the complex demo.");
        assertTrue(result.getExpressionNodes().size() >= 8, "Expected at least eight expression nodes in the complex demo.");

        Map<String, Integer> edgeTypeCounts = edgeTypeCounts(result.getGraphEdges());
        assertTrue(edgeTypeCounts.containsKey("COLUMN_TO_COLUMN"), "Missing COLUMN_TO_COLUMN edges.");
        assertTrue(edgeTypeCounts.containsKey("COLUMN_TO_EXPRESSION"), "Missing COLUMN_TO_EXPRESSION edges.");
        assertTrue(edgeTypeCounts.containsKey("EXPRESSION_TO_COLUMN"), "Missing EXPRESSION_TO_COLUMN edges.");
        assertTrue(edgeTypeCounts.containsKey("EXPRESSION_TO_EXPRESSION"), "Missing EXPRESSION_TO_EXPRESSION edges.");

        Set<String> payRateLeaves = collectLeafSources(result, scenario.targetPayRate.getNodeId());
        assertContains(payRateLeaves, scenario.orderId.getNodeId(), "pay_rate should eventually depend on source order_id.");
        assertContains(payRateLeaves, scenario.orderStatus.getNodeId(), "pay_rate should eventually depend on source order_status.");
        assertContains(payRateLeaves, scenario.paymentTime.getNodeId(), "pay_rate should eventually depend on source pay_time.");

        Set<String> gmvBandLeaves = collectLeafSources(result, scenario.targetGmvBand.getNodeId());
        assertContains(gmvBandLeaves, scenario.paymentAmount.getNodeId(), "gmv_band should eventually depend on source pay_amount.");
    }

    private static void printSummary(LineageStorage storage, DemoScenario scenario) {
        NormalizedLineageResult result = scenario.result;

        System.out.println("Scenario: complex_shop_trade_metric");
        System.out.println("Storage : " + storage.getClass().getSimpleName());
        System.out.println("Event   : " + scenario.captureEvent.getEventId());
        System.out.println("Inputs  : " + joinTableNames(result.getInputTables()));
        System.out.println("Output  : " + joinTableNames(result.getOutputTables()));
        System.out.println("Counts  : tables=" + result.getTableEdges().size()
                + ", columns=" + result.getColumnNodes().size()
                + ", expressions=" + result.getExpressionNodes().size()
                + ", graphEdges=" + result.getGraphEdges().size());
        System.out.println("SQL:");
        System.out.println(indent(scenario.sql, "  "));
        System.out.println("Graph edge types: " + edgeTypeCounts(result.getGraphEdges()));
        System.out.println("Assertions: OK");
        System.out.println("Lineage paths for " + scenario.targetPayRate.getNodeId() + ":");
        for (String path : collectLineagePaths(result, scenario.targetPayRate.getNodeId(), 6)) {
            System.out.println("  " + path);
        }
        System.out.println("Lineage paths for " + scenario.targetGmvBand.getNodeId() + ":");
        for (String path : collectLineagePaths(result, scenario.targetGmvBand.getNodeId(), 4)) {
            System.out.println("  " + path);
        }

        if (storage instanceof LineageStorage.InMemoryLineageStorage) {
            LineageStorage.InMemoryLineageStorage memoryStorage = (LineageStorage.InMemoryLineageStorage) storage;
            System.out.println("In-memory captures: " + memoryStorage.captures().size());
            System.out.println("In-memory results : " + memoryStorage.results().size());
        } else {
            System.out.println("External storage demo write completed.");
        }
    }

    private static Map<String, Integer> edgeTypeCounts(List<LineageGraphEdge> edges) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        List<LineageGraphEdge> sorted = new ArrayList<>(edges);
        Collections.sort(sorted, Comparator.comparing(LineageGraphEdge::getEdgeType));
        for (LineageGraphEdge edge : sorted) {
            Integer current = counts.get(edge.getEdgeType());
            counts.put(edge.getEdgeType(), current == null ? 1 : current + 1);
        }
        return counts;
    }

    private static Set<String> collectLeafSources(NormalizedLineageResult result, String targetNodeId) {
        Map<String, List<LineageGraphEdge>> incomingEdges = incomingEdges(result.getGraphEdges());
        LinkedHashSet<String> leaves = new LinkedHashSet<>();
        collectLeafSources(targetNodeId, incomingEdges, new LinkedHashSet<String>(), leaves);
        return leaves;
    }

    private static void collectLeafSources(
            String nodeId,
            Map<String, List<LineageGraphEdge>> incomingEdges,
            Set<String> visiting,
            Set<String> leaves
    ) {
        if (!visiting.add(nodeId)) {
            return;
        }

        List<LineageGraphEdge> edges = incomingEdges.get(nodeId);
        if (edges == null || edges.isEmpty()) {
            leaves.add(nodeId);
            visiting.remove(nodeId);
            return;
        }

        for (LineageGraphEdge edge : edges) {
            collectLeafSources(edge.getSourceNodeId(), incomingEdges, visiting, leaves);
        }
        visiting.remove(nodeId);
    }

    private static List<String> collectLineagePaths(NormalizedLineageResult result, String targetNodeId, int limit) {
        Map<String, List<LineageGraphEdge>> incomingEdges = incomingEdges(result.getGraphEdges());
        Map<String, String> nodeLabels = nodeLabels(result);
        List<String> paths = new ArrayList<>();
        collectLineagePaths(targetNodeId, incomingEdges, nodeLabels, new LinkedHashSet<String>(), nodeLabels.get(targetNodeId), paths, limit);
        return paths;
    }

    private static void collectLineagePaths(
            String currentNodeId,
            Map<String, List<LineageGraphEdge>> incomingEdges,
            Map<String, String> nodeLabels,
            Set<String> visiting,
            String path,
            List<String> paths,
            int limit
    ) {
        if (paths.size() >= limit || !visiting.add(currentNodeId)) {
            return;
        }

        List<LineageGraphEdge> edges = incomingEdges.get(currentNodeId);
        if (edges == null || edges.isEmpty()) {
            paths.add(path);
            visiting.remove(currentNodeId);
            return;
        }

        for (LineageGraphEdge edge : edges) {
            if (paths.size() >= limit) {
                break;
            }
            String nextLabel = nodeLabels.get(edge.getSourceNodeId());
            String nextPath = path + " <-[" + edge.getRole() + "/" + edge.getEdgeType() + "]- " + nextLabel;
            collectLineagePaths(edge.getSourceNodeId(), incomingEdges, nodeLabels, visiting, nextPath, paths, limit);
        }
        visiting.remove(currentNodeId);
    }

    private static Map<String, List<LineageGraphEdge>> incomingEdges(List<LineageGraphEdge> edges) {
        Map<String, List<LineageGraphEdge>> incoming = new LinkedHashMap<>();
        for (LineageGraphEdge edge : edges) {
            List<LineageGraphEdge> values = incoming.get(edge.getTargetNodeId());
            if (values == null) {
                values = new ArrayList<>();
                incoming.put(edge.getTargetNodeId(), values);
            }
            values.add(edge);
        }
        return incoming;
    }

    private static Map<String, String> nodeLabels(NormalizedLineageResult result) {
        Map<String, String> labels = new LinkedHashMap<>();
        for (ColumnNode column : result.getColumnNodes()) {
            labels.put(column.getNodeId(), "COLUMN(" + column.getOwnerId() + "." + column.getName() + ")");
        }
        for (ExpressionNode expression : result.getExpressionNodes()) {
            labels.put(expression.getNodeId(), "EXPR(" + expression.getExpressionType() + ":" + compact(expression.getExpressionSql()) + ")");
        }
        return labels;
    }

    private static String joinTableNames(List<TableRef> tables) {
        List<String> names = new ArrayList<>(tables.size());
        for (TableRef table : tables) {
            names.add(table.normalizedName());
        }
        return String.join(", ", names);
    }

    private static String compact(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.replaceAll("\\s+", " ").trim();
        return normalized.length() <= 72 ? normalized : normalized.substring(0, 69) + "...";
    }

    private static String indent(String value, String prefix) {
        String[] lines = value.split("\\r?\\n");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                builder.append(System.lineSeparator());
            }
            builder.append(prefix).append(lines[i]);
        }
        return builder.toString();
    }

    private static ColumnNode column(String ownerId, String ownerType, String name, String dataType, String... qualifier) {
        return new ColumnNode(ownerId + "." + name, ownerId, ownerType, name, dataType, Arrays.asList(qualifier));
    }

    private static ExpressionNode expression(String nodeId, String type, String sql, String normalizedSql) {
        return new ExpressionNode(nodeId, type, sql, normalizedSql);
    }

    private static LineageGraphEdge edge(ColumnNode source, ColumnNode target, String edgeType, String role, String eventId) {
        return new LineageGraphEdge(source.getNodeId(), target.getNodeId(), edgeType, role, eventId);
    }

    private static LineageGraphEdge edge(ColumnNode source, ExpressionNode target, String edgeType, String role, String eventId) {
        return new LineageGraphEdge(source.getNodeId(), target.getNodeId(), edgeType, role, eventId);
    }

    private static LineageGraphEdge edge(ExpressionNode source, ColumnNode target, String edgeType, String role, String eventId) {
        return new LineageGraphEdge(source.getNodeId(), target.getNodeId(), edgeType, role, eventId);
    }

    private static LineageGraphEdge edge(ExpressionNode source, ExpressionNode target, String edgeType, String role, String eventId) {
        return new LineageGraphEdge(source.getNodeId(), target.getNodeId(), edgeType, role, eventId);
    }

    private static void closeQuietly(LineageStorage storage) {
        if (storage instanceof AutoCloseable) {
            try {
                ((AutoCloseable) storage).close();
            } catch (Exception ignored) {
            }
        }
    }

    private static void assertContains(Set<String> values, String expected, String message) {
        assertTrue(values.contains(expected), message + " leaves=" + values);
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static String loadSql() {
        try (InputStream inputStream = ComplexLineageDemo.class.getResourceAsStream("/sql/complex_lineage_demo.sql")) {
            if (inputStream == null) {
                throw new IllegalStateException("Missing resource /sql/complex_lineage_demo.sql");
            }
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                outputStream.write(buffer, 0, read);
            }
            return new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
        } catch (IOException error) {
            throw new IllegalStateException("Failed to load demo SQL.", error);
        }
    }

    private static final class DemoScenario {
        private final String sql;
        private final ExecutionCaptureEvent captureEvent;
        private final NormalizedLineageResult result;
        private final ColumnNode targetPayRate;
        private final ColumnNode targetGmvBand;
        private final ColumnNode orderId;
        private final ColumnNode orderStatus;
        private final ColumnNode paymentTime;
        private final ColumnNode paymentAmount;

        private DemoScenario(
                String sql,
                ExecutionCaptureEvent captureEvent,
                NormalizedLineageResult result,
                ColumnNode targetPayRate,
                ColumnNode targetGmvBand,
                ColumnNode orderId,
                ColumnNode orderStatus,
                ColumnNode paymentTime,
                ColumnNode paymentAmount
        ) {
            this.sql = sql;
            this.captureEvent = captureEvent;
            this.result = result;
            this.targetPayRate = targetPayRate;
            this.targetGmvBand = targetGmvBand;
            this.orderId = orderId;
            this.orderStatus = orderStatus;
            this.paymentTime = paymentTime;
            this.paymentAmount = paymentAmount;
        }
    }
}
