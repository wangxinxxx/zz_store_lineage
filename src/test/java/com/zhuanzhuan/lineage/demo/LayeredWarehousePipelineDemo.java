package com.zhuanzhuan.lineage.demo;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.zhuanzhuan.lineage.app.DefaultEventIdGenerator;
import com.zhuanzhuan.lineage.app.LineageCaptureService;
import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import com.zhuanzhuan.lineage.model.LineageWarning;
import com.zhuanzhuan.lineage.model.LineageGraphEdge;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.TableRef;
import com.zhuanzhuan.lineage.parser.DefaultSparkLineageParser;
import com.zhuanzhuan.lineage.spark.context.LineageTaskContextHolder;
import com.zhuanzhuan.lineage.spark.context.TaskContextProvider;
import com.zhuanzhuan.lineage.spark.listener.StoreLineageListenerRegistrar;
import com.zhuanzhuan.lineage.storage.LineageStorage;
import com.zhuanzhuan.lineage.storage.LineageStorageFactory;
import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class LayeredWarehousePipelineDemo {
    private static final String BIZ_DATE = "2026-03-20";
    private static final String OWNER = "codex";
    private static final String ODS_ORDER_TABLE = "ods.ods_order_event_di";
    private static final String ODS_PAY_TABLE = "ods.ods_pay_event_di";
    private static final String ODS_REFUND_TABLE = "ods.ods_refund_event_di";
    private static final String DIM_SHOP_TABLE = "dim.dim_shop_region_snapshot";
    private static final String DIM_USER_TABLE = "dim.dim_user_tag_snapshot";
    private static final String DWD_ORDER_TABLE = "dwd.dwd_trade_order_detail_di";
    private static final String DWD_REFUND_TABLE = "dwd.dwd_trade_refund_detail_di";
    private static final String DWS_SHOP_TABLE = "dws.dws_shop_trade_day_1d";
    private static final String DWS_USER_TABLE = "dws.dws_user_trade_day_1d";
    private static final String ADS_TABLE = "ads.ads_shop_trade_dashboard_di";

    private static final SqlTaskCase[] TASK_CASES = new SqlTaskCase[]{
            new SqlTaskCase(
                    "task_ods_to_dwd_trade_order_detail",
                    "ods_to_dwd_trade_order_detail",
                    "/warehouse/layered/ods_to_dwd_trade_order_detail.sql",
                    "/sql/layered/ods_to_dwd_trade_order_detail.sql",
                    DWD_ORDER_TABLE
            ),
            new SqlTaskCase(
                    "task_ods_to_dwd_trade_refund_detail",
                    "ods_to_dwd_trade_refund_detail",
                    "/warehouse/layered/ods_to_dwd_trade_refund_detail.sql",
                    "/sql/layered/ods_to_dwd_trade_refund_detail.sql",
                    DWD_REFUND_TABLE
            ),
            new SqlTaskCase(
                    "task_dwd_to_dws_shop_trade_day",
                    "dwd_to_dws_shop_trade_day",
                    "/warehouse/layered/dwd_to_dws_shop_trade_day.sql",
                    "/sql/layered/dwd_to_dws_shop_trade_day.sql",
                    DWS_SHOP_TABLE
            ),
            new SqlTaskCase(
                    "task_dwd_to_dws_user_trade_day",
                    "dwd_to_dws_user_trade_day",
                    "/warehouse/layered/dwd_to_dws_user_trade_day.sql",
                    "/sql/layered/dwd_to_dws_user_trade_day.sql",
                    DWS_USER_TABLE
            ),
            new SqlTaskCase(
                    "task_dws_to_ads_shop_trade_dashboard",
                    "dws_to_ads_shop_trade_dashboard",
                    "/warehouse/layered/dws_to_ads_shop_trade_dashboard.sql",
                    "/sql/layered/dws_to_ads_shop_trade_dashboard.sql",
                    ADS_TABLE
            )
    };

    private LayeredWarehousePipelineDemo() {
    }

    public static void main(String[] args) throws Exception {
        ensureNebulaMode();
        Path warehouseDir = Files.createTempDirectory("spark-layered-lineage-warehouse-");
        SparkSession spark = null;
        ObservabilityLineageStorage storage = null;
        try {
            spark = buildSparkSession(warehouseDir);
            bootstrapTables(spark);

            storage = new ObservabilityLineageStorage(LineageStorageFactory.createDefault());

            LineageCaptureService captureService = new LineageCaptureService(
                    new DefaultSparkLineageParser(),
                    storage,
                    new DefaultEventIdGenerator(),
                    TaskContextProvider.DEFAULT
            );
            StoreLineageListenerRegistrar.register(spark, captureService);

            for (SqlTaskCase taskCase : TASK_CASES) {
                executeTask(spark, taskCase, renderSql(taskCase));
            }

            Map<String, NormalizedLineageResult> latestResults = waitForExpectedResults(storage.shadow());
            runAssertions(latestResults);
            NebulaVerificationSummary nebulaSummary = verifyNebulaWrite();
            printSummary(storage, latestResults, nebulaSummary);
        } finally {
            if (spark != null) {
                spark.stop();
            }
            closeQuietly(storage);
            deleteRecursively(warehouseDir);
        }
    }

    private static SparkSession buildSparkSession(Path warehouseDir) {
        return SparkSession.builder()
                .appName("layered-warehouse-pipeline-demo")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .getOrCreate();
    }

    private static void bootstrapTables(SparkSession spark) {
        spark.sql("CREATE DATABASE IF NOT EXISTS ods");
        spark.sql("CREATE DATABASE IF NOT EXISTS dim");
        spark.sql("CREATE DATABASE IF NOT EXISTS dwd");
        spark.sql("CREATE DATABASE IF NOT EXISTS dws");
        spark.sql("CREATE DATABASE IF NOT EXISTS ads");

        spark.sql("DROP TABLE IF EXISTS ods.ods_order_event_di");
        spark.sql("DROP TABLE IF EXISTS ods.ods_pay_event_di");
        spark.sql("DROP TABLE IF EXISTS ods.ods_refund_event_di");
        spark.sql("DROP TABLE IF EXISTS dim.dim_shop_region_snapshot");
        spark.sql("DROP TABLE IF EXISTS dim.dim_user_tag_snapshot");

        spark.sql("DROP TABLE IF EXISTS dwd.dwd_trade_order_detail_di");
        spark.sql("DROP TABLE IF EXISTS dwd.dwd_trade_refund_detail_di");
        spark.sql("DROP TABLE IF EXISTS dws.dws_shop_trade_day_1d");
        spark.sql("DROP TABLE IF EXISTS dws.dws_user_trade_day_1d");
        spark.sql("DROP TABLE IF EXISTS ads.ads_shop_trade_dashboard_di");

        spark.sql(
                "CREATE TABLE " + ODS_ORDER_TABLE + " USING parquet AS "
                        + "SELECT * FROM VALUES "
                        + "('2026-03-20', 1001L, 201L, 3001L, 'CREATED', CAST(128.00 AS DECIMAL(18,2)), CAST(18.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 09:00:00')), "
                        + "('2026-03-20', 1001L, 201L, 3001L, 'PAID', CAST(128.00 AS DECIMAL(18,2)), CAST(18.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 10:05:00')), "
                        + "('2026-03-20', 1002L, 201L, 3002L, 'SIGNED', CAST(560.00 AS DECIMAL(18,2)), CAST(60.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 11:30:00')), "
                        + "('2026-03-20', 1003L, 202L, 3003L, 'PAID', CAST(86.00 AS DECIMAL(18,2)), CAST(6.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 12:00:00')), "
                        + "('2026-03-20', 1004L, 202L, 3004L, 'FINISHED', CAST(1280.00 AS DECIMAL(18,2)), CAST(180.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 14:00:00')) "
                        + "AS t(dt, order_id, shop_id, user_id, order_status, order_amount, discount_amount, event_time)"
        );
        spark.sql(
                "CREATE TABLE " + ODS_PAY_TABLE + " USING parquet AS "
                        + "SELECT * FROM VALUES "
                        + "('2026-03-20', 50001L, 1001L, 'SUCCESS', CAST(110.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 10:00:00')), "
                        + "('2026-03-20', 50002L, 1002L, 'SUCCESS', CAST(500.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 11:00:00')), "
                        + "('2026-03-20', 50003L, 1003L, 'FAIL', CAST(80.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 11:59:00')), "
                        + "('2026-03-20', 50004L, 1003L, 'SUCCESS', CAST(80.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 12:10:00')), "
                        + "('2026-03-20', 50005L, 1004L, 'SUCCESS', CAST(1100.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 14:10:00')) "
                        + "AS t(dt, pay_id, order_id, pay_status, pay_amount, pay_time)"
        );
        spark.sql(
                "CREATE TABLE " + ODS_REFUND_TABLE + " USING parquet AS "
                        + "SELECT * FROM VALUES "
                        + "('2026-03-20', 90001L, 1002L, 'APPLY', CAST(120.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 15:00:00')), "
                        + "('2026-03-20', 90001L, 1002L, 'SUCCESS', CAST(120.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 16:00:00')), "
                        + "('2026-03-20', 90002L, 1004L, 'SUCCESS', CAST(200.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-20 18:00:00')) "
                        + "AS t(dt, refund_id, order_id, refund_status, refund_amount, apply_time)"
        );
        spark.sql(
                "CREATE TABLE " + DIM_SHOP_TABLE + " USING parquet AS "
                        + "SELECT * FROM VALUES "
                        + "(201L, 'North Star Shop', 'North', 'flagship', 'alice', 'T1', '2026-03-20'), "
                        + "(202L, 'River Mall Shop', 'East', 'franchise', 'bob', 'T2', '2026-03-20') "
                        + "AS t(shop_id, shop_name, region_name, shop_type, operation_owner, city_tier, dt)"
        );
        spark.sql(
                "CREATE TABLE " + DIM_USER_TABLE + " USING parquet AS "
                        + "SELECT * FROM VALUES "
                        + "(3001L, 'new', 'P1', '2026-03-20'), "
                        + "(3002L, 'core', 'VIP', '2026-03-20'), "
                        + "(3003L, 'growth', 'P2', '2026-03-20'), "
                        + "(3004L, 'core', 'SVIP', '2026-03-20') "
                        + "AS t(user_id, user_tag, member_level, dt)"
        );

        spark.sql(
                "CREATE TABLE " + DWD_ORDER_TABLE + " ("
                        + "biz_date STRING, "
                        + "order_id BIGINT, "
                        + "shop_id BIGINT, "
                        + "shop_name STRING, "
                        + "region_name STRING, "
                        + "user_id BIGINT, "
                        + "order_status STRING, "
                        + "order_amount DECIMAL(18,2), "
                        + "discount_amount DECIMAL(18,2), "
                        + "pay_amount DECIMAL(18,2), "
                        + "pay_success_cnt BIGINT, "
                        + "is_paid INT, "
                        + "is_signed INT, "
                        + "amount_band STRING"
                        + ") USING parquet"
        );
        spark.sql(
                "CREATE TABLE " + DWD_REFUND_TABLE + " ("
                        + "biz_date STRING, "
                        + "refund_id BIGINT, "
                        + "order_id BIGINT, "
                        + "shop_id BIGINT, "
                        + "user_id BIGINT, "
                        + "refund_amount DECIMAL(18,2), "
                        + "refund_finish_flag INT, "
                        + "valid_refund_flag INT"
                        + ") USING parquet"
        );
        spark.sql(
                "CREATE TABLE " + DWS_SHOP_TABLE + " ("
                        + "biz_date STRING, "
                        + "shop_id BIGINT, "
                        + "shop_name STRING, "
                        + "region_name STRING, "
                        + "order_cnt BIGINT, "
                        + "paid_order_cnt BIGINT, "
                        + "paid_gmv DECIMAL(18,2), "
                        + "buyer_cnt BIGINT, "
                        + "refund_cnt BIGINT, "
                        + "refund_amount DECIMAL(18,2), "
                        + "avg_paid_order_amount DOUBLE"
                        + ") USING parquet"
        );
        spark.sql(
                "CREATE TABLE " + DWS_USER_TABLE + " ("
                        + "biz_date STRING, "
                        + "user_id BIGINT, "
                        + "last_shop_id BIGINT, "
                        + "paid_order_cnt BIGINT, "
                        + "paid_gmv DECIMAL(18,2), "
                        + "has_high_value_order INT, "
                        + "vip_user_flag INT"
                        + ") USING parquet"
        );
        spark.sql(
                "CREATE TABLE " + ADS_TABLE + " ("
                        + "biz_date STRING, "
                        + "shop_id BIGINT, "
                        + "shop_name STRING, "
                        + "region_name STRING, "
                        + "operation_owner STRING, "
                        + "city_tier STRING, "
                        + "order_cnt BIGINT, "
                        + "paid_order_cnt BIGINT, "
                        + "paid_gmv DECIMAL(18,2), "
                        + "refund_amount DECIMAL(18,2), "
                        + "net_paid_gmv DECIMAL(18,2), "
                        + "active_buyer_cnt BIGINT, "
                        + "vip_buyer_cnt BIGINT, "
                        + "pay_rate DOUBLE, "
                        + "refund_rate DOUBLE, "
                        + "gmv_band STRING"
                        + ") USING parquet"
        );
    }

    private static void executeTask(SparkSession spark, SqlTaskCase taskCase, String sql) {
        LineageTaskContext context = new LineageTaskContext(
                taskCase.taskId,
                taskCase.taskName,
                taskCase.taskId + "_run_001",
                BIZ_DATE,
                OWNER,
                taskCase.scriptPath
        );
        withTaskContext(spark.sparkContext(), context, new Runnable() {
            @Override
            public void run() {
                spark.sql(sql).collect();
            }
        });
    }

    private static void withTaskContext(SparkContext sparkContext, LineageTaskContext context, Runnable runnable) {
        setLocalProperty(sparkContext, TaskContextProvider.Keys.TASK_ID, context.getTaskId());
        setLocalProperty(sparkContext, TaskContextProvider.Keys.TASK_NAME, context.getTaskName());
        setLocalProperty(sparkContext, TaskContextProvider.Keys.RUN_ID, context.getRunId());
        setLocalProperty(sparkContext, TaskContextProvider.Keys.BIZ_DATE, context.getBizDate());
        setLocalProperty(sparkContext, TaskContextProvider.Keys.OWNER, context.getOwner());
        setLocalProperty(sparkContext, TaskContextProvider.Keys.SCRIPT_PATH, context.getScriptPath());
        setConfProperty(sparkContext, TaskContextProvider.Keys.TASK_ID, context.getTaskId());
        setConfProperty(sparkContext, TaskContextProvider.Keys.TASK_NAME, context.getTaskName());
        setConfProperty(sparkContext, TaskContextProvider.Keys.RUN_ID, context.getRunId());
        setConfProperty(sparkContext, TaskContextProvider.Keys.BIZ_DATE, context.getBizDate());
        setConfProperty(sparkContext, TaskContextProvider.Keys.OWNER, context.getOwner());
        setConfProperty(sparkContext, TaskContextProvider.Keys.SCRIPT_PATH, context.getScriptPath());

        LineageTaskContextHolder.set(context);
        try {
            runnable.run();
        } finally {
            LineageTaskContextHolder.clear();
            clearLocalProperty(sparkContext, TaskContextProvider.Keys.TASK_ID);
            clearLocalProperty(sparkContext, TaskContextProvider.Keys.TASK_NAME);
            clearLocalProperty(sparkContext, TaskContextProvider.Keys.RUN_ID);
            clearLocalProperty(sparkContext, TaskContextProvider.Keys.BIZ_DATE);
            clearLocalProperty(sparkContext, TaskContextProvider.Keys.OWNER);
            clearLocalProperty(sparkContext, TaskContextProvider.Keys.SCRIPT_PATH);
        }
    }

    private static void setLocalProperty(SparkContext sparkContext, String key, String value) {
        if (value != null) {
            sparkContext.setLocalProperty(key, value);
        }
    }

    private static void clearLocalProperty(SparkContext sparkContext, String key) {
        sparkContext.setLocalProperty(key, null);
    }

    private static void setConfProperty(SparkContext sparkContext, String key, String value) {
        if (value != null) {
            sparkContext.getConf().set(key, value);
        }
    }

    private static String renderSql(SqlTaskCase taskCase) {
        return loadSql(taskCase.resourcePath).replace("${biz_date}", BIZ_DATE);
    }

    private static String loadSql(String resourcePath) {
        try (InputStream inputStream = LayeredWarehousePipelineDemo.class.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IllegalStateException("Missing resource " + resourcePath);
            }
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                outputStream.write(buffer, 0, read);
            }
            return new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
        } catch (IOException error) {
            throw new IllegalStateException("Failed to load SQL resource " + resourcePath, error);
        }
    }

    private static Map<String, NormalizedLineageResult> latestResultsByOutput(List<NormalizedLineageResult> results) {
        Map<String, NormalizedLineageResult> latestByOutput = new LinkedHashMap<>();
        for (NormalizedLineageResult result : results) {
            for (TableRef outputTable : result.getOutputTables()) {
                String key = outputTable.normalizedName();
                NormalizedLineageResult current = latestByOutput.get(key);
                if (current == null || current.getCaptureTimeEpochMs() <= result.getCaptureTimeEpochMs()) {
                    latestByOutput.put(key, result);
                }
            }
        }
        return latestByOutput;
    }

    private static Map<String, NormalizedLineageResult> waitForExpectedResults(LineageStorage.InMemoryLineageStorage storage) {
        Map<String, NormalizedLineageResult> latestResults = latestResultsByOutput(storage.results());
        for (int attempt = 0; attempt < 30; attempt++) {
            if (latestResults.size() >= TASK_CASES.length) {
                return latestResults;
            }
            sleepQuietly(200L);
            latestResults = latestResultsByOutput(storage.results());
        }
        return latestResults;
    }

    private static void runAssertions(Map<String, NormalizedLineageResult> latestResults) {
        if (latestResults.size() != TASK_CASES.length) {
            throw new IllegalStateException("Expected " + TASK_CASES.length + " distinct output results but found " + latestResults.keySet());
        }

        NormalizedLineageResult dwdOrder = requireResult(latestResults, "dwd.dwd_trade_order_detail_di");
        NormalizedLineageResult dwdRefund = requireResult(latestResults, "dwd.dwd_trade_refund_detail_di");
        NormalizedLineageResult dwsShop = requireResult(latestResults, "dws.dws_shop_trade_day_1d");
        NormalizedLineageResult dwsUser = requireResult(latestResults, "dws.dws_user_trade_day_1d");
        NormalizedLineageResult ads = requireResult(latestResults, "ads.ads_shop_trade_dashboard_di");

        assertInputTables(dwdOrder, "dim.dim_shop_region_snapshot", "ods.ods_order_event_di", "ods.ods_pay_event_di");
        assertInputTables(dwdRefund, "dwd.dwd_trade_order_detail_di", "ods.ods_refund_event_di");
        assertInputTables(dwsShop, "dwd.dwd_trade_order_detail_di", "dwd.dwd_trade_refund_detail_di");
        assertInputTables(dwsUser, "dim.dim_user_tag_snapshot", "dwd.dwd_trade_order_detail_di");
        assertInputTables(ads, "dim.dim_shop_region_snapshot", "dws.dws_shop_trade_day_1d", "dws.dws_user_trade_day_1d");

        assertSourceColumnsContain(
                dwsUser,
                DWS_USER_TABLE + ".vip_user_flag",
                "member_level"
        );
        assertHasUpstreamSources(ads, ADS_TABLE + ".net_paid_gmv");
        assertHasUpstreamSources(ads, ADS_TABLE + ".pay_rate");
        assertHasUpstreamSources(ads, ADS_TABLE + ".refund_rate");

        assertWarningsEmpty(dwdOrder);
        assertWarningsEmpty(dwdRefund);
        assertWarningsEmpty(dwsShop);
        assertWarningsEmpty(dwsUser);
        assertWarningsEmpty(ads);
    }

    private static NormalizedLineageResult requireResult(Map<String, NormalizedLineageResult> latestResults, String outputTable) {
        NormalizedLineageResult result = latestResults.get(outputTable);
        if (result == null) {
            throw new IllegalStateException("Missing lineage result for output table " + outputTable + ", available=" + latestResults.keySet());
        }
        return result;
    }

    private static void assertInputTables(NormalizedLineageResult result, String... expectedTables) {
        Set<String> actual = new LinkedHashSet<>();
        for (TableRef tableRef : result.getInputTables()) {
            actual.add(tableRef.normalizedName());
        }
        Set<String> expected = new LinkedHashSet<>(Arrays.asList(expectedTables));
        if (!actual.equals(expected)) {
            throw new IllegalStateException("Unexpected input tables for "
                    + result.getOutputTables().get(0).normalizedName()
                    + ", expected=" + expected
                    + ", actual=" + actual);
        }
    }

    private static void assertSourceColumnsContain(NormalizedLineageResult result, String targetColumnId, String... expectedSources) {
        Set<String> actualSources = sourceColumnsForTarget(result, targetColumnId);
        List<String> missing = new ArrayList<>();
        for (String expectedSource : expectedSources) {
            if (!containsSourceColumn(actualSources, expectedSource)) {
                missing.add(expectedSource);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("Missing expected source columns for "
                    + targetColumnId
                    + ", missing=" + missing
                    + ", actual=" + actualSources);
        }
    }

    private static boolean containsSourceColumn(Set<String> actualSources, String expectedSource) {
        for (String actualSource : actualSources) {
            if (actualSource.equals(expectedSource) || actualSource.endsWith("." + expectedSource)) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> sourceColumnsForTarget(NormalizedLineageResult result, String targetColumnId) {
        Set<String> logicNodeIds = new LinkedHashSet<>();
        Set<String> sourceColumns = new LinkedHashSet<>();
        for (LineageGraphEdge edge : result.getGraphEdges()) {
            if ("COLUMN_TO_COLUMN".equals(edge.getEdgeType()) && targetColumnId.equals(edge.getTargetNodeId())) {
                sourceColumns.add(edge.getSourceNodeId());
            }
            if ("EXPRESSION_TO_COLUMN".equals(edge.getEdgeType()) && targetColumnId.equals(edge.getTargetNodeId())) {
                logicNodeIds.add(edge.getSourceNodeId());
            }
        }
        for (String logicNodeId : logicNodeIds) {
            for (LineageGraphEdge edge : result.getGraphEdges()) {
                if ("COLUMN_TO_EXPRESSION".equals(edge.getEdgeType()) && logicNodeId.equals(edge.getTargetNodeId())) {
                    sourceColumns.add(edge.getSourceNodeId());
                }
            }
        }
        return sourceColumns;
    }

    private static void assertWarningsEmpty(NormalizedLineageResult result) {
        List<String> warnings = new ArrayList<>();
        for (LineageWarning warning : result.getWarnings()) {
            warnings.add(warning.getCode());
        }
        if (!warnings.isEmpty()) {
            throw new IllegalStateException("Unexpected warnings for "
                    + result.getOutputTables().get(0).normalizedName()
                    + ": " + warnings);
        }
    }

    private static void assertHasUpstreamSources(NormalizedLineageResult result, String targetColumnId) {
        Set<String> actualSources = sourceColumnsForTarget(result, targetColumnId);
        if (actualSources.isEmpty()) {
            throw new IllegalStateException("Expected upstream source columns for "
                    + targetColumnId
                    + " but found none.");
        }
    }

    private static void printSummary(LineageStorage.InMemoryLineageStorage storage, Map<String, NormalizedLineageResult> latestResults) {
        System.out.println("Layered warehouse pipeline demo completed.");
        System.out.println("Biz date             : " + BIZ_DATE);
        System.out.println("Captured events      : " + storage.captures().size());
        System.out.println("Lineage results      : " + storage.results().size());
        List<String> outputs = new ArrayList<>(latestResults.keySet());
        Collections.sort(outputs);
        for (String output : outputs) {
            NormalizedLineageResult result = latestResults.get(output);
            List<String> inputs = new ArrayList<>();
            for (TableRef tableRef : result.getInputTables()) {
                inputs.add(tableRef.normalizedName());
            }
            Collections.sort(inputs);
            System.out.println(output
                    + " <- " + inputs
                    + " | columns=" + result.getColumnNodes().size()
                    + ", logicNodes=" + result.getExpressionNodes().size()
                    + ", edges=" + result.getGraphEdges().size());
        }
    }

    private static void ensureNebulaMode() {
        if (System.getProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY) == null
                && System.getenv(LineageStorageFactory.STORAGE_TYPE_ENV) == null) {
            System.setProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY, "nebula");
        }
    }

    private static NebulaVerificationSummary verifyNebulaWrite() throws Exception {
        NebulaGraphConfig config = NebulaGraphConfig.fromSystem();
        NebulaPool pool = new NebulaPool();
        NebulaPoolConfig poolConfig = new NebulaPoolConfig()
                .setMinConnSize(config.getMinConnSize())
                .setMaxConnSize(config.getMaxConnSize())
                .setTimeout(config.getTimeoutMs())
                .setMinClusterHealthRate(0.0d);
        pool.init(Arrays.asList(new HostAddress(config.getHost(), config.getPort())), poolConfig);

        try {
            NebulaVerificationSummary lastSummary = null;
            Exception lastError = null;
            for (int attempt = 0; attempt < 30; attempt++) {
                try (Session session = pool.getSession(config.getUsername(), config.getPassword(), true)) {
                    execute(session, "USE `" + config.getSpace() + "`;");

                    List<String> visibleTables = new ArrayList<>();
                    for (SqlTaskCase taskCase : TASK_CASES) {
                        List<String> columns = firstColumnValues(execute(
                                session,
                                "GO FROM \"table:" + taskCase.outputTable + "\" OVER table_has_column YIELD dst(edge) AS column_vid;"
                        ));
                        if (!columns.isEmpty()) {
                            visibleTables.add("table:" + taskCase.outputTable);
                        }
                    }

                    NebulaVerificationSummary summary = new NebulaVerificationSummary(
                            distinctSorted(visibleTables),
                            firstColumnValues(execute(
                                    session,
                                    "MATCH (upstream)-[:latest_table_flows_to_table]->(target) "
                                            + "WHERE id(target) == \"table:" + ADS_TABLE + "\" "
                                            + "RETURN DISTINCT id(upstream) AS upstream_vid;"
                            )),
                            firstColumnValues(execute(
                                    session,
                                    "GO FROM \"" + ADS_TABLE + ".pay_rate\" OVER latest_column_uses_expression YIELD dst(edge) AS logic_vid;"
                            )),
                            firstColumnValues(execute(
                                    session,
                                    "GO FROM \"" + ADS_TABLE + ".refund_rate\" OVER latest_column_uses_expression YIELD dst(edge) AS logic_vid;"
                            ))
                    );
                    lastSummary = summary;
                    if (summary.isComplete()) {
                        return summary;
                    }
                } catch (Exception error) {
                    lastError = error;
                }
                sleepQuietly(1000L);
            }

            if (lastSummary != null) {
                throw new IllegalStateException("Nebula verification timed out. Last summary=" + lastSummary.describe(), lastError);
            }
            throw new IllegalStateException("Nebula verification timed out before any lineage data was visible.", lastError);
        } finally {
            pool.close();
        }
    }

    private static ResultSet execute(Session session, String statement) {
        try {
            ResultSet resultSet = session.execute(statement);
            if (!resultSet.isSucceeded()) {
                throw new IllegalStateException("Nebula query failed: " + statement + ", error=" + resultSet.getErrorMessage());
            }
            return resultSet;
        } catch (Exception error) {
            throw new IllegalStateException("Nebula query execution failed: " + statement, error);
        }
    }

    private static List<String> firstColumnValues(ResultSet resultSet) throws UnsupportedEncodingException {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            ValueWrapper value = resultSet.rowValues(i).get(0);
            if (value.isString()) {
                values.add(value.asString());
            } else if (value.isLong()) {
                values.add(String.valueOf(value.asLong()));
            } else {
                values.add(value.toString());
            }
        }
        values.sort(Comparator.naturalOrder());
        return values;
    }

    private static List<String> distinctSorted(List<String> values) {
        List<String> distinct = new ArrayList<>(new LinkedHashSet<>(values));
        Collections.sort(distinct);
        return distinct;
    }

    private static void printSummary(
            ObservabilityLineageStorage storage,
            Map<String, NormalizedLineageResult> latestResults,
            NebulaVerificationSummary nebulaSummary
    ) {
        printSummary(storage.shadow(), latestResults);
        System.out.println("Storage type         : " + System.getProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY, "nebula"));
        System.out.println("Nebula visible tables: " + nebulaSummary.visibleTables);
        System.out.println("Nebula ads upstreams : " + nebulaSummary.adsUpstreamTables);
        System.out.println("Nebula pay_rate logic: " + nebulaSummary.payRateLogicNodes);
        System.out.println("Nebula refund logic  : " + nebulaSummary.refundRateLogicNodes);
    }

    private static void closeQuietly(LineageStorage storage) {
        if (storage instanceof AutoCloseable) {
            try {
                ((AutoCloseable) storage).close();
            } catch (Exception ignored) {
            }
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private static void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try {
            Files.walk(root)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException ignored) {
        }
    }

    private static final class SqlTaskCase {
        private final String taskId;
        private final String taskName;
        private final String scriptPath;
        private final String resourcePath;
        private final String outputTable;

        private SqlTaskCase(String taskId, String taskName, String scriptPath, String resourcePath, String outputTable) {
            this.taskId = taskId;
            this.taskName = taskName;
            this.scriptPath = scriptPath;
            this.resourcePath = resourcePath;
            this.outputTable = outputTable;
        }
    }

    private static final class NebulaVerificationSummary {
        private final List<String> visibleTables;
        private final List<String> adsUpstreamTables;
        private final List<String> payRateLogicNodes;
        private final List<String> refundRateLogicNodes;

        private NebulaVerificationSummary(
                List<String> visibleTables,
                List<String> adsUpstreamTables,
                List<String> payRateLogicNodes,
                List<String> refundRateLogicNodes
        ) {
            this.visibleTables = visibleTables;
            this.adsUpstreamTables = adsUpstreamTables;
            this.payRateLogicNodes = payRateLogicNodes;
            this.refundRateLogicNodes = refundRateLogicNodes;
        }

        private boolean isComplete() {
            return visibleTables.contains("table:" + DWD_ORDER_TABLE)
                    && visibleTables.contains("table:" + DWD_REFUND_TABLE)
                    && visibleTables.contains("table:" + DWS_SHOP_TABLE)
                    && visibleTables.contains("table:" + DWS_USER_TABLE)
                    && visibleTables.contains("table:" + ADS_TABLE)
                    && adsUpstreamTables.contains("table:" + DWS_SHOP_TABLE)
                    && adsUpstreamTables.contains("table:" + DWS_USER_TABLE)
                    && adsUpstreamTables.contains("table:" + DIM_SHOP_TABLE)
                    && !payRateLogicNodes.isEmpty()
                    && !refundRateLogicNodes.isEmpty();
        }

        private String describe() {
            return "visibleTables=" + visibleTables
                    + ", adsUpstreamTables=" + adsUpstreamTables
                    + ", payRateLogicNodes=" + payRateLogicNodes
                    + ", refundRateLogicNodes=" + refundRateLogicNodes;
        }
    }

    private static final class ObservabilityLineageStorage implements LineageStorage, AutoCloseable {
        private final LineageStorage delegate;
        private final LineageStorage.InMemoryLineageStorage shadow = new LineageStorage.InMemoryLineageStorage();

        private ObservabilityLineageStorage(LineageStorage delegate) {
            this.delegate = delegate;
        }

        @Override
        public void saveCapture(ExecutionCaptureEvent event) {
            shadow.saveCapture(event);
            delegate.saveCapture(event);
        }

        @Override
        public void saveLineage(NormalizedLineageResult result) {
            shadow.saveLineage(result);
            delegate.saveLineage(result);
        }

        private LineageStorage.InMemoryLineageStorage shadow() {
            return shadow;
        }

        @Override
        public void close() throws Exception {
            if (delegate instanceof AutoCloseable) {
                ((AutoCloseable) delegate).close();
            }
        }
    }
}
