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
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.parser.DefaultSparkLineageParser;
import com.zhuanzhuan.lineage.spark.context.LineageTaskContextHolder;
import com.zhuanzhuan.lineage.spark.context.TaskContextProvider;
import com.zhuanzhuan.lineage.spark.listener.StoreLineageListenerRegistrar;
import com.zhuanzhuan.lineage.storage.LineageStorage;
import com.zhuanzhuan.lineage.storage.LineageStorageFactory;
import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.execution.QueryExecution;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class SparkSessionNebulaDemo {
    private static final String TASK_ID = "spark_sql_nebula_demo_task";
    private static final String TASK_NAME = "spark_sql_nebula_demo";
    private static final String RUN_ID = "spark_sql_nebula_demo_run_001";
    private static final String BIZ_DATE = "2026-03-19";
    private static final String OWNER = "codex";
    private static final String SCRIPT_PATH = "/warehouse/demo/spark_session_nebula_demo.sql";

    private static final String SOURCE_ORDER_TABLE = "dwd.demo_fact_order_detail";
    private static final String SOURCE_PAYMENT_TABLE = "dwd.demo_fact_payment_detail";
    private static final String SOURCE_SHOP_TABLE = "dim.demo_dim_shop_snapshot";
    private static final String TARGET_TABLE = "ads.demo_ads_shop_trade_metric";

    private SparkSessionNebulaDemo() {
    }

    public static void main(String[] args) throws Exception {
        ensureNebulaMode();
        cleanupNebulaArtifacts();

        Path warehouseDir = Files.createTempDirectory("spark-lineage-warehouse-");
        SparkSession spark = null;
        ObservabilityLineageStorage storage = null;
        try {
            spark = buildSparkSession(warehouseDir);
            bootstrapTables(spark);

            final LineageTaskContext context = new LineageTaskContext(TASK_ID, TASK_NAME, RUN_ID, BIZ_DATE, OWNER, SCRIPT_PATH);
            storage = new ObservabilityLineageStorage(LineageStorageFactory.createDefault());
            LineageCaptureService captureService = new LineageCaptureService(
                    new DefaultSparkLineageParser(),
                    storage,
                    new DefaultEventIdGenerator(),
                    new TaskContextProvider() {
                        @Override
                        public LineageTaskContext current(org.apache.spark.sql.execution.QueryExecution qe) {
                            return context;
                        }
                    }
            );
            StoreLineageListenerRegistrar.register(spark, captureService);

            String insertSql = renderSql(BIZ_DATE);
            if (Boolean.getBoolean("zz.lineage.demo.debug")) {
                printCommandDiagnostics(spark, insertSql);
            }
            executeInsert(spark, context, insertSql);

            VerificationSummary summary = verifyNebulaWrite();
            printSummary(warehouseDir, insertSql, summary, storage);
        } finally {
            closeQuietly(storage);
            if (spark != null) {
                spark.stop();
            }
            deleteRecursively(warehouseDir);
        }
    }

    private static void ensureNebulaMode() {
        if (System.getProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY) == null
                && System.getenv(LineageStorageFactory.STORAGE_TYPE_ENV) == null) {
            System.setProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY, "nebula");
        }
    }

    private static SparkSession buildSparkSession(Path warehouseDir) {
        return SparkSession.builder()
                .appName("spark-session-nebula-demo")
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
        spark.sql("CREATE DATABASE IF NOT EXISTS dwd");
        spark.sql("CREATE DATABASE IF NOT EXISTS dim");
        spark.sql("CREATE DATABASE IF NOT EXISTS ads");

        spark.sql("DROP TABLE IF EXISTS " + SOURCE_ORDER_TABLE);
        spark.sql("DROP TABLE IF EXISTS " + SOURCE_PAYMENT_TABLE);
        spark.sql("DROP TABLE IF EXISTS " + SOURCE_SHOP_TABLE);
        spark.sql("DROP TABLE IF EXISTS " + TARGET_TABLE);

        spark.sql(
                "CREATE TABLE " + SOURCE_ORDER_TABLE + " USING parquet AS "
                        + "SELECT * FROM VALUES "
                        + "('2026-03-19', 101L, 90001L, 7001L, 'PAID'), "
                        + "('2026-03-19', 101L, 90002L, 7002L, 'FINISHED'), "
                        + "('2026-03-19', 102L, 90003L, 7003L, 'CREATED'), "
                        + "('2026-03-19', 102L, 90004L, 7004L, 'PAID') "
                        + "AS t(dt, shop_id, order_id, user_id, order_status)"
        );
        spark.sql(
                "CREATE TABLE " + SOURCE_PAYMENT_TABLE + " USING parquet AS "
                        + "SELECT * FROM VALUES "
                        + "(90001L, CAST(188.50 AS DECIMAL(18,2)), TIMESTAMP('2026-03-19 10:00:00')), "
                        + "(90002L, CAST(66.80 AS DECIMAL(18,2)), TIMESTAMP('2026-03-19 12:30:00')), "
                        + "(90004L, CAST(3200.00 AS DECIMAL(18,2)), TIMESTAMP('2026-03-19 14:45:00')) "
                        + "AS t(order_id, pay_amount, pay_time)"
        );
        spark.sql(
                "CREATE TABLE " + SOURCE_SHOP_TABLE + " USING parquet AS "
                        + "SELECT * FROM VALUES "
                        + "(101L, 'Demo Shop A', '2026-03-19'), "
                        + "(102L, 'Demo Shop B', '2026-03-19') "
                        + "AS t(shop_id, shop_name, dt)"
        );
        spark.sql(
                "CREATE TABLE " + TARGET_TABLE + " ("
                        + "biz_date STRING, "
                        + "shop_id BIGINT, "
                        + "shop_name STRING, "
                        + "order_cnt BIGINT, "
                        + "paid_order_cnt BIGINT, "
                        + "paid_gmv DECIMAL(18,2), "
                        + "buyer_cnt BIGINT, "
                        + "pay_rate DOUBLE, "
                        + "gmv_band STRING"
                        + ") USING parquet"
        );
    }

    private static void executeInsert(SparkSession spark, LineageTaskContext context, String insertSql) {
        withTaskContext(spark.sparkContext(), context, new Runnable() {
            @Override
            public void run() {
                spark.sql(insertSql).collect();
            }
        });
    }

    private static void printCommandDiagnostics(SparkSession spark, String insertSql) {
        Dataset<Row> dataset = spark.sql(insertSql);
        QueryExecution qe = dataset.queryExecution();
        Object logical = qe.logical();
        Object analyzed = qe.analyzed();
        System.out.println("DIAG logicalClass   : " + logical.getClass().getName());
        System.out.println("DIAG analyzedClass  : " + analyzed.getClass().getName());
        printCommandFields("logical", logical);
        printCommandFields("analyzed", analyzed);
    }

    private static void printCommandFields(String prefix, Object plan) {
        System.out.println("DIAG " + prefix + ".nodeName          : " + invokeString(plan, "nodeName"));
        System.out.println("DIAG " + prefix + ".outputColumns     : " + attributeNames(invoke(plan, "outputColumns")));
        System.out.println("DIAG " + prefix + ".outputColumnNames : " + stringValues(invoke(plan, "outputColumnNames")));
        Object query = invoke(plan, "query");
        if (query != null) {
            System.out.println("DIAG " + prefix + ".queryClass        : " + query.getClass().getName());
            System.out.println("DIAG " + prefix + ".queryNodeName     : " + invokeString(query, "nodeName"));
            System.out.println("DIAG " + prefix + ".queryOutput       : " + attributeNames(invoke(query, "output")));
            System.out.println("DIAG " + prefix + ".query             : " + query);
        }
    }

    private static Object invoke(Object target, String methodName) {
        if (target == null) {
            return null;
        }
        try {
            return target.getClass().getMethod(methodName).invoke(target);
        } catch (Exception ignored) {
        }
        try {
            java.lang.reflect.Method method = target.getClass().getDeclaredMethod(methodName);
            method.setAccessible(true);
            return method.invoke(target);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String invokeString(Object target, String methodName) {
        Object value = invoke(target, methodName);
        return value == null ? null : String.valueOf(value);
    }

    private static List<String> attributeNames(Object value) {
        if (!(value instanceof Seq)) {
            return new ArrayList<>();
        }
        List<String> values = new ArrayList<>();
        for (Object item : JavaConverters.seqAsJavaList((Seq<?>) value)) {
            if (item instanceof Attribute) {
                values.add(((Attribute) item).name());
            } else if (item != null) {
                values.add(String.valueOf(item));
            }
        }
        return values;
    }

    private static List<String> stringValues(Object value) {
        if (!(value instanceof Seq)) {
            return new ArrayList<>();
        }
        List<String> values = new ArrayList<>();
        for (Object item : JavaConverters.seqAsJavaList((Seq<?>) value)) {
            if (item != null) {
                values.add(String.valueOf(item));
            }
        }
        return values;
    }

    private static void cleanupNebulaArtifacts() {
        NebulaGraphConfig config = NebulaGraphConfig.fromSystem();
        NebulaPool pool = new NebulaPool();
        NebulaPoolConfig poolConfig = new NebulaPoolConfig()
                .setMinConnSize(config.getMinConnSize())
                .setMaxConnSize(config.getMaxConnSize())
                .setTimeout(config.getTimeoutMs())
                .setMinClusterHealthRate(0.0d);
        try {
            pool.init(Arrays.asList(new HostAddress(config.getHost(), config.getPort())), poolConfig);
            try (Session session = pool.getSession(config.getUsername(), config.getPassword(), true)) {
                ResultSet useResult = session.execute("USE `" + config.getSpace() + "`;");
                if (!useResult.isSucceeded()) {
                    return;
                }

                Set<String> vertexVids = new LinkedHashSet<>();
                List<String> tableVids = Arrays.asList(
                        "table:" + SOURCE_ORDER_TABLE,
                        "table:" + SOURCE_PAYMENT_TABLE,
                        "table:" + SOURCE_SHOP_TABLE,
                        "table:" + TARGET_TABLE
                );

                vertexVids.add("task:" + TASK_ID);
                vertexVids.add("run:" + RUN_ID);
                vertexVids.addAll(tableVids);

                vertexVids.addAll(firstColumnValues(execute(
                        session,
                        "GO FROM \"task:" + TASK_ID + "\" OVER task_has_execution YIELD dst(edge) AS vid;"
                )));
                vertexVids.addAll(firstColumnValues(execute(
                        session,
                        "GO FROM \"run:" + RUN_ID + "\" OVER run_has_execution YIELD dst(edge) AS vid;"
                )));

                for (String tableVid : tableVids) {
                    vertexVids.addAll(firstColumnValues(execute(
                            session,
                            "GO FROM \"" + tableVid + "\" OVER table_has_column YIELD dst(edge) AS vid;"
                    )));
                }

                List<String> captureVids = new ArrayList<>();
                for (String vertexVid : vertexVids) {
                    if (vertexVid.startsWith("capture:")) {
                        captureVids.add(vertexVid);
                    }
                }
                for (String captureVid : captureVids) {
                    vertexVids.addAll(firstColumnValues(execute(
                            session,
                            "GO FROM \"" + captureVid + "\" OVER execution_emits_column YIELD dst(edge) AS vid;"
                    )));
                    vertexVids.addAll(firstColumnValues(execute(
                            session,
                            "GO FROM \"" + captureVid + "\" OVER execution_observes_expression YIELD dst(edge) AS vid;"
                    )));
                }

                deleteVertices(session, vertexVids);
            }
        } catch (Exception ignored) {
        } finally {
            pool.close();
        }
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

    private static VerificationSummary verifyNebulaWrite() throws Exception {
        NebulaGraphConfig config = NebulaGraphConfig.fromSystem();
        NebulaPool pool = new NebulaPool();
        NebulaPoolConfig poolConfig = new NebulaPoolConfig()
                .setMinConnSize(config.getMinConnSize())
                .setMaxConnSize(config.getMaxConnSize())
                .setTimeout(config.getTimeoutMs())
                .setMinClusterHealthRate(0.0d);
        pool.init(Arrays.asList(new HostAddress(config.getHost(), config.getPort())), poolConfig);

        try {
            VerificationSummary lastSummary = null;
            Exception lastError = null;

            for (int attempt = 0; attempt < 30; attempt++) {
                try (Session session = pool.getSession(config.getUsername(), config.getPassword(), true)) {
                    execute(session, "USE `" + config.getSpace() + "`;");

                    VerificationSummary summary = new VerificationSummary(
                            firstColumnValues(execute(
                                    session,
                                    "GO FROM \"task:" + TASK_ID + "\" OVER task_has_execution YIELD dst(edge) AS execution_vid;"
                            )),
                            firstColumnValues(execute(
                                    session,
                                    "GO FROM \"task:" + TASK_ID + "\" OVER task_reads_table YIELD dst(edge) AS table_vid;"
                            )),
                            firstColumnValues(execute(
                                    session,
                                    "GO FROM \"task:" + TASK_ID + "\" OVER task_writes_table YIELD dst(edge) AS table_vid;"
                            )),
                            firstColumnValues(execute(
                                    session,
                                    "MATCH (upstream)-[:latest_table_flows_to_table]->(target) "
                                            + "WHERE id(target) == \"table:" + TARGET_TABLE + "\" "
                                            + "RETURN DISTINCT id(upstream) AS upstream_vid;"
                            )),
                            firstColumnValues(execute(
                                    session,
                                    "GO FROM \"table:" + TARGET_TABLE + "\" OVER table_has_column YIELD dst(edge) AS column_vid;"
                            )),
                            firstColumnValues(execute(
                                    session,
                                    "GO FROM \"" + findColumnVid(session, "pay_rate") + "\" OVER latest_column_uses_expression YIELD dst(edge) AS expr_vid;"
                            )),
                            firstColumnValues(execute(
                                    session,
                                    "GO FROM \"" + findColumnVid(session, "gmv_band") + "\" OVER latest_column_uses_expression YIELD dst(edge) AS expr_vid;"
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

    private static void deleteVertices(Session session, Set<String> vertexVids) {
        if (vertexVids.isEmpty()) {
            return;
        }

        List<String> batch = new ArrayList<>(32);
        for (String vertexVid : vertexVids) {
            batch.add("\"" + vertexVid + "\"");
            if (batch.size() == 32) {
                execute(session, "DELETE VERTEX " + String.join(", ", batch) + " WITH EDGE;");
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            execute(session, "DELETE VERTEX " + String.join(", ", batch) + " WITH EDGE;");
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

    private static String findColumnVid(Session session, String columnName) throws UnsupportedEncodingException {
        List<String> columnVids = firstColumnValues(execute(
                session,
                "GO FROM \"table:" + TARGET_TABLE + "\" OVER table_has_column YIELD dst(edge) AS column_vid;"
        ));
        for (String columnVid : columnVids) {
            if (columnVid.endsWith("." + columnName)) {
                return columnVid;
            }
        }
        throw new IllegalStateException("Target column not found in Nebula for " + columnName + ", columns=" + columnVids);
    }

    private static void printSummary(Path warehouseDir, String insertSql, VerificationSummary summary, ObservabilityLineageStorage storage) {
        System.out.println("SparkSession Nebula demo completed.");
        System.out.println("Warehouse dir : " + warehouseDir.toAbsolutePath());
        System.out.println("Task id       : " + TASK_ID);
        System.out.println("Run id        : " + RUN_ID);
        System.out.println("Storage type  : " + System.getProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY, "nebula"));
        System.out.println("Target table  : " + TARGET_TABLE);
        System.out.println("Shadow captures/results : "
                + storage.shadow().captures().size()
                + "/"
                + storage.shadow().results().size());
        System.out.println("SQL:");
        System.out.println(indent(insertSql, "  "));
        System.out.println("Nebula task_has_execution count : " + summary.executionVids.size());
        System.out.println("Nebula task_reads_table         : " + summary.readTables);
        System.out.println("Nebula task_writes_table        : " + summary.writeTables);
        System.out.println("Nebula latest upstream tables   : " + summary.upstreamTables);
        System.out.println("Nebula pay_rate expressions     : " + summary.payRateExpressions);
        System.out.println("Nebula gmv_band expressions     : " + summary.gmvBandExpressions);
    }

    private static String renderSql(String bizDate) {
        return loadSql().replace("${biz_date}", bizDate);
    }

    private static String loadSql() {
        try (InputStream inputStream = SparkSessionNebulaDemo.class.getResourceAsStream("/sql/spark_session_nebula_demo.sql")) {
            if (inputStream == null) {
                throw new IllegalStateException("Missing resource /sql/spark_session_nebula_demo.sql");
            }
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                outputStream.write(buffer, 0, read);
            }
            return new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
        } catch (IOException error) {
            throw new IllegalStateException("Failed to load SparkSession Nebula demo SQL.", error);
        }
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

    private static final class VerificationSummary {
        private final List<String> executionVids;
        private final List<String> readTables;
        private final List<String> writeTables;
        private final List<String> upstreamTables;
        private final List<String> targetColumns;
        private final List<String> payRateExpressions;
        private final List<String> gmvBandExpressions;

        private VerificationSummary(
                List<String> executionVids,
                List<String> readTables,
                List<String> writeTables,
                List<String> upstreamTables,
                List<String> targetColumns,
                List<String> payRateExpressions,
                List<String> gmvBandExpressions
        ) {
            this.executionVids = executionVids;
            this.readTables = readTables;
            this.writeTables = writeTables;
            this.upstreamTables = upstreamTables;
            this.targetColumns = targetColumns;
            this.payRateExpressions = payRateExpressions;
            this.gmvBandExpressions = gmvBandExpressions;
        }

        private boolean isComplete() {
            return !executionVids.isEmpty()
                    && readTables.size() >= 3
                    && writeTables.contains("table:" + TARGET_TABLE)
                    && upstreamTables.contains("table:" + SOURCE_ORDER_TABLE)
                    && upstreamTables.contains("table:" + SOURCE_PAYMENT_TABLE)
                    && upstreamTables.contains("table:" + SOURCE_SHOP_TABLE)
                    && !targetColumns.isEmpty()
                    && payRateExpressions.size() == 1
                    && gmvBandExpressions.size() == 1;
        }

        private String describe() {
            return "executionVids=" + executionVids
                    + ", readTables=" + readTables
                    + ", writeTables=" + writeTables
                    + ", upstreamTables=" + upstreamTables
                    + ", targetColumns=" + targetColumns
                    + ", payRateExpressions=" + payRateExpressions
                    + ", gmvBandExpressions=" + gmvBandExpressions;
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
            System.out.println("LINEAGE saveCapture eventId=" + event.getEventId()
                    + ", taskId=" + (event.getTaskContext() == null ? null : event.getTaskContext().getTaskId())
                    + ", func=" + event.getFuncName());
            shadow.saveCapture(event);
            delegate.saveCapture(event);
        }

        @Override
        public void saveLineage(NormalizedLineageResult result) {
            System.out.println("LINEAGE saveLineage eventId=" + result.getEventId()
                    + ", statementType=" + result.getStatementType()
                    + ", inputs=" + result.getInputTables().size()
                    + ", outputs=" + result.getOutputTables().size()
                    + ", columns=" + result.getColumnNodes().size()
                    + ", expressions=" + result.getExpressionNodes().size()
                    + ", graphEdges=" + result.getGraphEdges().size());
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
