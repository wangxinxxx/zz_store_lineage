package com.zhuanzhuan.lineage.demo;

import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExecutionStatus;
import com.zhuanzhuan.lineage.model.LineageGraphEdge;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.OperatorInstanceNode;
import com.zhuanzhuan.lineage.model.PredicateNode;
import com.zhuanzhuan.lineage.model.RawPlanSnapshot;
import com.zhuanzhuan.lineage.model.RelationInstanceNode;
import com.zhuanzhuan.lineage.model.ScopeNode;
import com.zhuanzhuan.lineage.model.SparkAppContext;
import com.zhuanzhuan.lineage.parser.DefaultSparkLineageParser;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public final class NamedPlanJoinAggregateLineageDemo {
    private NamedPlanJoinAggregateLineageDemo() {
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("named-plan-join-aggregate-lineage-demo")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            prepareCatalog(spark);

            String sql = "WITH t_income AS (\n"
                    + "    SELECT month, store_id, SUM(sale_net_gmv) AS sale_net_gmv\n"
                    + "    FROM dwd.demo_income\n"
                    + "    GROUP BY month, store_id\n"
                    + "),\n"
                    + "rpc AS (\n"
                    + "    SELECT stat_month,\n"
                    + "           ROUND(SUM(store_gmv_n) / SUM(czc_mn_gmv), 4) AS rpc1\n"
                    + "    FROM tmp.retail_price_table2\n"
                    + "    WHERE dt = '20260327'\n"
                    + "    GROUP BY stat_month\n"
                    + "),\n"
                    + "t2 AS (\n"
                    + "    SELECT t1.month,\n"
                    + "           t1.store_id,\n"
                    + "           SUM(t1.sale_net_gmv) * (((6.5 / 100) + MAX(t2.rpc1)) - 1) AS offline_sale_income\n"
                    + "    FROM t_income t1\n"
                    + "    LEFT JOIN rpc t2\n"
                    + "      ON t1.month = t2.stat_month\n"
                    + "    GROUP BY t1.month, t1.store_id\n"
                    + ")\n"
                    + "INSERT OVERWRITE TABLE ads.demo_metric\n"
                    + "SELECT month, store_id, offline_sale_income\n"
                    + "FROM t2";

            LogicalPlan logicalPlan = parsePlan(spark, sql);
            AnalysisBundle analysisBundle = analyzePlan(spark, logicalPlan);

            DefaultSparkLineageParser parser = new DefaultSparkLineageParser();
            NormalizedLineageResult result = parser.parse(buildEvent(spark, sql, analysisBundle), logicalPlan, analysisBundle.analyzedPlan, analysisBundle.optimizedPlan);

            assertNamedScopes(result);
            assertNamedRelationInstances(result);
            assertJoinPredicate(result);
            assertAggregateMetadata(result);

            System.out.println("Named plan join/aggregate lineage demo succeeded.");
            System.out.println("scopes=" + result.getScopeNodes().size());
            System.out.println("relations=" + result.getRelationInstanceNodes().size());
            System.out.println("operators=" + result.getOperatorInstanceNodes().size());
            System.out.println("predicates=" + result.getPredicateNodes().size());
            System.out.println("edges=" + result.getGraphEdges().size());
        } finally {
            spark.stop();
        }
    }

    private static void prepareCatalog(SparkSession spark) {
        spark.sql("CREATE DATABASE IF NOT EXISTS dwd");
        spark.sql("CREATE DATABASE IF NOT EXISTS tmp");
        spark.sql("CREATE DATABASE IF NOT EXISTS ads");
        spark.sql("CREATE TABLE IF NOT EXISTS dwd.demo_income (month string, store_id string, sale_net_gmv decimal(18,2)) USING parquet");
        spark.sql("CREATE TABLE IF NOT EXISTS tmp.retail_price_table2 (stat_month string, store_gmv_n decimal(18,2), czc_mn_gmv decimal(18,2), dt string) USING parquet");
        spark.sql("CREATE TABLE IF NOT EXISTS ads.demo_metric (month string, store_id string, offline_sale_income decimal(18,2)) USING parquet");
    }

    private static void assertNamedScopes(NormalizedLineageResult result) {
        boolean foundT2Scope = false;
        boolean foundTIncomeScope = false;
        boolean foundRpcScope = false;
        for (ScopeNode scopeNode : result.getScopeNodes()) {
            if ("t2".equals(scopeNode.getScopeName())) {
                foundT2Scope = true;
            }
            if ("t_income".equals(scopeNode.getScopeName())) {
                foundTIncomeScope = true;
            }
            if ("rpc".equals(scopeNode.getScopeName())) {
                foundRpcScope = true;
            }
        }
        if (!foundT2Scope || !foundTIncomeScope || !foundRpcScope) {
            throw new IllegalStateException(
                    "Expected named scopes missing: t2=" + foundT2Scope
                            + ", t_income=" + foundTIncomeScope
                            + ", rpc=" + foundRpcScope
            );
        }
    }

    private static void assertNamedRelationInstances(NormalizedLineageResult result) {
        boolean foundNamedTIncome = false;
        boolean foundNamedRpc = false;
        boolean foundJoinEdge = false;
        List<String> namedSources = new ArrayList<String>();
        for (RelationInstanceNode relationInstanceNode : result.getRelationInstanceNodes()) {
            if ("named_plan".equals(relationInstanceNode.getSourceType())) {
                namedSources.add(relationInstanceNode.getSourceTableId());
            }
            if ("t_income".equals(relationInstanceNode.getSourceTableId())) {
                foundNamedTIncome = true;
            }
            if ("rpc".equals(relationInstanceNode.getSourceTableId())) {
                foundNamedRpc = true;
            }
        }
        for (LineageGraphEdge edge : result.getGraphEdges()) {
            if (!"RELATION_INSTANCE_TO_RELATION_INSTANCE".equals(edge.getEdgeType())) {
                continue;
            }
            if (edge.getRole() == null || !edge.getRole().contains("join")) {
                continue;
            }
            RelationInstanceNode source = findRelation(result, edge.getSourceNodeId());
            RelationInstanceNode target = findRelation(result, edge.getTargetNodeId());
            if (source == null || target == null) {
                continue;
            }
            if (("t_income".equals(source.getSourceTableId()) && "rpc".equals(target.getSourceTableId()))
                    || ("rpc".equals(source.getSourceTableId()) && "t_income".equals(target.getSourceTableId()))) {
                foundJoinEdge = true;
                break;
            }
        }
        if (!foundNamedTIncome || !foundNamedRpc || !foundJoinEdge) {
            throw new IllegalStateException(
                    "Expected named relation/join metadata missing: t_income=" + foundNamedTIncome
                            + ", rpc=" + foundNamedRpc
                            + ", join=" + foundJoinEdge
                            + ", namedSources=" + namedSources
            );
        }
    }

    private static void assertJoinPredicate(NormalizedLineageResult result) {
        boolean foundJoinPredicate = false;
        for (PredicateNode predicateNode : result.getPredicateNodes()) {
            String predicateSql = predicateNode.getPredicateSql() == null ? "" : predicateNode.getPredicateSql().toLowerCase();
            if ("JOIN".equalsIgnoreCase(predicateNode.getPredicateType())
                    && predicateSql.contains("month")
                    && predicateSql.contains("stat_month")) {
                foundJoinPredicate = true;
                break;
            }
        }
        if (!foundJoinPredicate) {
            throw new IllegalStateException("Expected named-plan join predicate with month/stat_month.");
        }
    }

    private static void assertAggregateMetadata(NormalizedLineageResult result) {
        boolean foundAggregateOperator = false;
        boolean foundGroupKey = false;
        boolean foundAggregateExpression = false;
        for (OperatorInstanceNode operatorInstanceNode : result.getOperatorInstanceNodes()) {
            if ("aggregate".equals(operatorInstanceNode.getOperatorType())) {
                foundAggregateOperator = true;
            }
        }
        for (LineageGraphEdge edge : result.getGraphEdges()) {
            if (!"OPERATOR_TO_EXPRESSION".equals(edge.getEdgeType())) {
                continue;
            }
            if ("group_key".equals(edge.getRole())) {
                foundGroupKey = true;
            }
            if ("aggregate_expression".equals(edge.getRole())) {
                foundAggregateExpression = true;
            }
        }
        if (!foundAggregateOperator || !foundGroupKey || !foundAggregateExpression) {
            throw new IllegalStateException(
                    "Expected aggregate metadata missing: operator=" + foundAggregateOperator
                            + ", group_key=" + foundGroupKey
                            + ", aggregate_expression=" + foundAggregateExpression
            );
        }
    }

    private static RelationInstanceNode findRelation(NormalizedLineageResult result, String nodeId) {
        for (RelationInstanceNode relationInstanceNode : result.getRelationInstanceNodes()) {
            if (relationInstanceNode.getNodeId().equals(nodeId)) {
                return relationInstanceNode;
            }
        }
        return null;
    }

    private static LogicalPlan parsePlan(SparkSession spark, String sql) throws Exception {
        Object sessionState = invokeNoArg(spark, "sessionState");
        Object sqlParser = invokeNoArg(sessionState, "sqlParser");
        Object logicalPlan = invokeOneArg(sqlParser, "parsePlan", String.class, sql);
        return (LogicalPlan) logicalPlan;
    }

    private static AnalysisBundle analyzePlan(SparkSession spark, LogicalPlan logicalPlan) throws Exception {
        Object sessionState = invokeNoArg(spark, "sessionState");
        Object queryExecution = invokeCompatibleOneArg(sessionState, "executePlan", logicalPlan);
        LogicalPlan analyzedPlan = (LogicalPlan) invokeNoArg(queryExecution, "analyzed");
        LogicalPlan optimizedPlan = (LogicalPlan) invokeNoArg(queryExecution, "optimizedPlan");
        return new AnalysisBundle(analyzedPlan, optimizedPlan);
    }

    private static ExecutionCaptureEvent buildEvent(SparkSession spark, String sql, AnalysisBundle analysisBundle) {
        long captureTimeMs = System.currentTimeMillis();
        return new ExecutionCaptureEvent(
                "offline:named-plan-join-aggregate-demo",
                ExecutionStatus.SUCCESS,
                new LineageTaskContext(
                        "named_plan_join_aggregate_demo",
                        "named_plan_join_aggregate_demo.sql",
                        "named_plan_join_aggregate_demo_run",
                        "20260327",
                        "sql_script_importer",
                        "inline"
                ),
                new SparkAppContext(
                        spark.sparkContext().applicationId(),
                        spark.sparkContext().appName(),
                        spark.sparkContext().sparkUser(),
                        spark.sparkContext().master()
                ),
                "offline_parse",
                null,
                captureTimeMs,
                null,
                new RawPlanSnapshot(
                        analysisBundle.analyzedPlan.toString(),
                        analysisBundle.analyzedPlan.toString(),
                        analysisBundle.optimizedPlan.toString(),
                        ""
                )
        );
    }

    private static Object invokeNoArg(Object target, String methodName) throws Exception {
        try {
            Method method = target.getClass().getMethod(methodName);
            method.setAccessible(true);
            return method.invoke(target);
        } catch (Exception ignored) {
        }
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }

    private static Object invokeOneArg(Object target, String methodName, Class<?> argumentType, Object argumentValue) throws Exception {
        try {
            Method method = target.getClass().getMethod(methodName, argumentType);
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        } catch (Exception ignored) {
        }
        Method method = target.getClass().getDeclaredMethod(methodName, argumentType);
        method.setAccessible(true);
        return method.invoke(target, argumentValue);
    }

    private static Object invokeCompatibleOneArg(Object target, String methodName, Object argumentValue) throws Exception {
        for (Method method : target.getClass().getMethods()) {
            if (!methodName.equals(method.getName()) || method.getParameterCount() != 1) {
                continue;
            }
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        }
        for (Method method : target.getClass().getDeclaredMethods()) {
            if (!methodName.equals(method.getName()) || method.getParameterCount() != 1) {
                continue;
            }
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        }
        throw new IllegalStateException("Compatible method not found: " + methodName);
    }

    private static final class AnalysisBundle {
        private final LogicalPlan analyzedPlan;
        private final LogicalPlan optimizedPlan;

        private AnalysisBundle(LogicalPlan analyzedPlan, LogicalPlan optimizedPlan) {
            this.analyzedPlan = analyzedPlan;
            this.optimizedPlan = optimizedPlan;
        }
    }
}
