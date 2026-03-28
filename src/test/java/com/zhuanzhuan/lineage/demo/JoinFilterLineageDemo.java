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
import com.zhuanzhuan.lineage.model.SparkAppContext;
import com.zhuanzhuan.lineage.parser.DefaultSparkLineageParser;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public final class JoinFilterLineageDemo {
    private JoinFilterLineageDemo() {
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("join-filter-lineage-demo")
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

            String sql = "INSERT OVERWRITE TABLE ads.demo_join_filter_metric\n"
                    + "SELECT o.dt,\n"
                    + "       s.shop_name,\n"
                    + "       SUM(CASE WHEN p.pay_status = 'SUCCESS' THEN o.amount ELSE 0 END) AS paid_amount\n"
                    + "FROM dwd.demo_order_detail o\n"
                    + "LEFT JOIN dwd.demo_payment_detail p\n"
                    + "  ON o.order_id = p.order_id\n"
                    + "LEFT JOIN dim.demo_shop_snapshot s\n"
                    + "  ON o.shop_id = s.shop_id\n"
                    + "WHERE o.dt = '20260327'\n"
                    + "  AND o.order_status IN ('PAID', 'FINISHED')\n"
                    + "GROUP BY o.dt, s.shop_name";

            LogicalPlan logicalPlan = parsePlan(spark, sql);
            AnalysisBundle analysisBundle = analyzePlan(spark, logicalPlan);

            DefaultSparkLineageParser parser = new DefaultSparkLineageParser();
            NormalizedLineageResult result = parser.parse(buildEvent(spark, sql, analysisBundle), logicalPlan, analysisBundle.analyzedPlan, analysisBundle.optimizedPlan);

            assertOperatorMetadata(result);
            assertPredicateMetadata(result);
            assertRelationMetadata(result);
            assertEdgeMetadata(result);

            System.out.println("Join/filter lineage demo succeeded.");
            System.out.println("operators=" + result.getOperatorInstanceNodes().size());
            System.out.println("predicates=" + result.getPredicateNodes().size());
            System.out.println("relations=" + result.getRelationInstanceNodes().size());
            System.out.println("edges=" + result.getGraphEdges().size());
        } finally {
            spark.stop();
        }
    }

    private static void prepareCatalog(SparkSession spark) {
        spark.sql("CREATE DATABASE IF NOT EXISTS dwd");
        spark.sql("CREATE DATABASE IF NOT EXISTS dim");
        spark.sql("CREATE DATABASE IF NOT EXISTS ads");
        spark.sql("CREATE TABLE IF NOT EXISTS dwd.demo_order_detail (dt string, order_id string, shop_id string, order_status string, amount decimal(18,2)) USING parquet");
        spark.sql("CREATE TABLE IF NOT EXISTS dwd.demo_payment_detail (order_id string, pay_status string) USING parquet");
        spark.sql("CREATE TABLE IF NOT EXISTS dim.demo_shop_snapshot (shop_id string, shop_name string) USING parquet");
        spark.sql("CREATE TABLE IF NOT EXISTS ads.demo_join_filter_metric (dt string, shop_name string, paid_amount decimal(18,2)) USING parquet");
    }

    private static void assertOperatorMetadata(NormalizedLineageResult result) {
        boolean foundLeftJoin = false;
        for (OperatorInstanceNode operatorInstanceNode : result.getOperatorInstanceNodes()) {
            if ("join".equals(operatorInstanceNode.getOperatorType())
                    && operatorInstanceNode.getOperatorSubType() != null
                    && operatorInstanceNode.getOperatorSubType().contains("left")) {
                foundLeftJoin = true;
                break;
            }
        }
        if (!foundLeftJoin) {
            throw new IllegalStateException("Expected at least one left join operator instance.");
        }
    }

    private static void assertPredicateMetadata(NormalizedLineageResult result) {
        boolean foundJoinPredicate = false;
        boolean foundFilterPredicate = false;
        for (PredicateNode predicateNode : result.getPredicateNodes()) {
            if ("JOIN".equalsIgnoreCase(predicateNode.getPredicateType())) {
                foundJoinPredicate = true;
            }
            if ("FILTER".equalsIgnoreCase(predicateNode.getPredicateType())) {
                foundFilterPredicate = true;
            }
        }
        if (!foundJoinPredicate || !foundFilterPredicate) {
            throw new IllegalStateException("Expected both JOIN and FILTER predicates.");
        }
    }

    private static void assertRelationMetadata(NormalizedLineageResult result) {
        List<String> sourceTables = new ArrayList<String>();
        for (RelationInstanceNode relationInstanceNode : result.getRelationInstanceNodes()) {
            sourceTables.add(relationInstanceNode.getSourceTableId());
        }
        if (!sourceTables.contains("dwd.demo_order_detail")
                || !sourceTables.contains("dwd.demo_payment_detail")
                || !sourceTables.contains("dim.demo_shop_snapshot")) {
            throw new IllegalStateException("Expected join inputs missing from relation instances: " + sourceTables);
        }
    }

    private static void assertEdgeMetadata(NormalizedLineageResult result) {
        boolean foundLeftInput = false;
        boolean foundRightInput = false;
        boolean foundJoinRelation = false;
        boolean foundFilterConstraint = false;
        for (LineageGraphEdge edge : result.getGraphEdges()) {
            if ("OPERATOR_TO_RELATION_INSTANCE".equals(edge.getEdgeType()) && "left_input".equals(edge.getRole())) {
                foundLeftInput = true;
            }
            if ("OPERATOR_TO_RELATION_INSTANCE".equals(edge.getEdgeType()) && "right_input".equals(edge.getRole())) {
                foundRightInput = true;
            }
            if ("RELATION_INSTANCE_TO_RELATION_INSTANCE".equals(edge.getEdgeType()) && edge.getRole() != null && edge.getRole().contains("join")) {
                foundJoinRelation = true;
            }
            if ("RELATION_INSTANCE_TO_PREDICATE".equals(edge.getEdgeType()) && "filter_condition".equals(edge.getRole())) {
                foundFilterConstraint = true;
            }
        }
        if (!foundLeftInput || !foundRightInput || !foundJoinRelation || !foundFilterConstraint) {
            throw new IllegalStateException(
                    "Expected join/filter edges missing: left=" + foundLeftInput
                            + ", right=" + foundRightInput
                            + ", joinRelation=" + foundJoinRelation
                            + ", filterConstraint=" + foundFilterConstraint
            );
        }
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
                "offline:join-filter-demo",
                ExecutionStatus.SUCCESS,
                new LineageTaskContext(
                        "join_filter_demo",
                        "join_filter_demo.sql",
                        "join_filter_demo_run",
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
