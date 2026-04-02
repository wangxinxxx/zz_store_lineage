package com.zhuanzhuan.lineage.storage.nebula;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.connector.NebulaDataSource;
import com.vesoft.nebula.connector.NebulaOptions;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class SparkNebulaConnectorImporter {
    private static final String VERTEX_IMPORT_MAIN_CLASS = "com.zhuanzhuan.lineage.app.SparkConnectorImportVerticesMain$";
    private static final String EDGE_IMPORT_MAIN_CLASS = "com.zhuanzhuan.lineage.app.SparkConnectorImportEdgesMain$";

    public static final String SPARK_MASTER_PROPERTY = "zz.lineage.spark.connector.master";
    public static final String SPARK_MASTER_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_MASTER";
    public static final String REPARTITION_PROPERTY = "zz.lineage.spark.connector.repartition";
    public static final String REPARTITION_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_REPARTITION";
    public static final String SHUFFLE_PARTITIONS_PROPERTY = "zz.lineage.spark.connector.shufflePartitions";
    public static final String SHUFFLE_PARTITIONS_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_SHUFFLE_PARTITIONS";
    public static final String BATCH_PROPERTY = "zz.lineage.spark.connector.batch";
    public static final String BATCH_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_BATCH";
    public static final String CONNECTION_RETRY_PROPERTY = "zz.lineage.spark.connector.connectionRetry";
    public static final String CONNECTION_RETRY_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_CONNECTION_RETRY";
    public static final String EXECUTION_RETRY_PROPERTY = "zz.lineage.spark.connector.executionRetry";
    public static final String EXECUTION_RETRY_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_EXECUTION_RETRY";
    public static final String DISABLE_WRITE_LOG_PROPERTY = "zz.lineage.spark.connector.disableWriteLog";
    public static final String DISABLE_WRITE_LOG_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_DISABLE_WRITE_LOG";
    public static final String OVERWRITE_PROPERTY = "zz.lineage.spark.connector.overwrite";
    public static final String OVERWRITE_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_OVERWRITE";
    public static final String PARALLEL_SCHEMA_PROPERTY = "zz.lineage.spark.connector.parallelSchemas";
    public static final String PARALLEL_SCHEMA_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_PARALLEL_SCHEMAS";
    public static final String VERTICES_ONLY_PROPERTY = "zz.lineage.spark.connector.verticesOnly";
    public static final String VERTICES_ONLY_ENV = "ZZ_LINEAGE_SPARK_CONNECTOR_VERTICES_ONLY";
    public static final String SPARK_CONF_PREFIX = "zz.lineage.spark.conf.";

    private final NebulaGraphConfig graphConfig;
    private final ImportOptions options;

    public SparkNebulaConnectorImporter(NebulaGraphConfig graphConfig, ImportOptions options) {
        this.graphConfig = graphConfig;
        this.options = options == null ? ImportOptions.defaults() : options;
    }

    public ImportSummary importBundle(Path bundleDir) throws IOException {
        Path absoluteDir = bundleDir.toAbsolutePath().normalize();
        if (!Files.isDirectory(absoluteDir)) {
            throw new IllegalArgumentException("Bundle directory does not exist: " + absoluteDir);
        }

        BundleManifest manifest = readManifest(absoluteDir);
        long startedAt = System.currentTimeMillis();
        int importedVertexFiles = countNonEmptyVertexFiles(absoluteDir);
        int importedEdgeFiles = options.verticesOnly ? 0 : countNonEmptyEdgeFiles(absoluteDir);
        Map<String, String> previousProperties = applyRuntimeOverrides();
        try {
            invokeScalaMain(VERTEX_IMPORT_MAIN_CLASS, absoluteDir);
            if (!options.verticesOnly) {
                invokeScalaMain(EDGE_IMPORT_MAIN_CLASS, absoluteDir);
            }
        } finally {
            restoreSystemProperties(previousProperties);
        }

        return new ImportSummary(
                absoluteDir,
                importedVertexFiles,
                importedEdgeFiles,
                manifest.totalVertices,
                manifest.totalEdges,
                System.currentTimeMillis() - startedAt
        );
    }

    public static void ensureSchemaReady(NebulaGraphConfig graphConfig, Path bundleDir) throws IOException {
        NebulaPool pool = new NebulaPool();
        initPool(pool, graphConfig);
        try {
            List<String> statements = loadSchemaStatements(bundleDir, graphConfig.getSpace());
            executeBootstrapStatements(pool, graphConfig, statements);
            waitForSchemaReady(pool, graphConfig);
            reconcileSchema(pool, graphConfig);
            waitForSchemaReady(pool, graphConfig);
        } finally {
            pool.close();
        }
    }

    private Map<String, String> applyRuntimeOverrides() {
        java.util.LinkedHashMap<String, String> previous = new java.util.LinkedHashMap<String, String>();
        overrideSystemProperty(previous, NebulaGraphConfig.HOST_PROPERTY, graphConfig.getHost());
        overrideSystemProperty(previous, NebulaGraphConfig.PORT_PROPERTY, String.valueOf(graphConfig.getPort()));
        overrideSystemProperty(previous, NebulaGraphConfig.META_ADDRESS_PROPERTY, graphConfig.getMetaAddress());
        overrideSystemProperty(previous, NebulaGraphConfig.GRAPH_ADDRESS_PROPERTY, graphConfig.getGraphAddress());
        overrideSystemProperty(previous, NebulaGraphConfig.USERNAME_PROPERTY, graphConfig.getUsername());
        overrideSystemProperty(previous, NebulaGraphConfig.PASSWORD_PROPERTY, graphConfig.getPassword());
        overrideSystemProperty(previous, NebulaGraphConfig.SPACE_PROPERTY, graphConfig.getSpace());
        overrideSystemProperty(previous, NebulaGraphConfig.TIMEOUT_PROPERTY, String.valueOf(Math.max(graphConfig.getTimeoutMs(), 3000)));

        overrideSystemProperty(previous, SPARK_MASTER_PROPERTY, options.sparkMaster);
        overrideSystemProperty(previous, REPARTITION_PROPERTY, String.valueOf(options.repartition));
        overrideSystemProperty(previous, SHUFFLE_PARTITIONS_PROPERTY, String.valueOf(options.shufflePartitions));
        overrideSystemProperty(previous, BATCH_PROPERTY, String.valueOf(options.batch));
        overrideSystemProperty(previous, CONNECTION_RETRY_PROPERTY, String.valueOf(options.connectionRetry));
        overrideSystemProperty(previous, EXECUTION_RETRY_PROPERTY, String.valueOf(options.executionRetry));
        overrideSystemProperty(previous, DISABLE_WRITE_LOG_PROPERTY, String.valueOf(options.disableWriteLog));
        overrideSystemProperty(previous, OVERWRITE_PROPERTY, String.valueOf(options.overwrite));
        overrideSystemProperty(previous, PARALLEL_SCHEMA_PROPERTY, String.valueOf(options.parallelSchemas));
        overrideSystemProperty(previous, VERTICES_ONLY_PROPERTY, String.valueOf(options.verticesOnly));
        return previous;
    }

    private void restoreSystemProperties(Map<String, String> previousProperties) {
        for (Map.Entry<String, String> entry : previousProperties.entrySet()) {
            if (entry.getValue() == null) {
                System.clearProperty(entry.getKey());
            } else {
                System.setProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    private void overrideSystemProperty(Map<String, String> previousProperties, String key, String value) {
        previousProperties.put(key, System.getProperty(key));
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private void invokeScalaMain(String mainClassName, Path bundleDir) throws IOException {
        try {
            Class<?> moduleClass = Class.forName(mainClassName);
            Object module = moduleClass.getField("MODULE$").get(null);
            moduleClass.getMethod("main", String[].class).invoke(module, (Object) new String[]{bundleDir.toString()});
        } catch (InvocationTargetException error) {
            Throwable cause = error.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new IllegalStateException("Spark connector import failed: " + mainClassName, cause);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("Unable to invoke spark connector importer: " + mainClassName, error);
        }
    }

    private int countNonEmptyVertexFiles(Path bundleDir) throws IOException {
        int count = 0;
        for (NebulaImporterBundleWriter.CsvSchema schema : NebulaImporterBundleWriter.vertexSchemas()) {
            Path csvPath = bundleDir.resolve(NebulaImporterBundleWriter.VERTICES_DIR_NAME).resolve(schema.getName() + ".csv");
            if (Files.isRegularFile(csvPath) && Files.size(csvPath) > 0L) {
                count++;
            }
        }
        return count;
    }

    private int countNonEmptyEdgeFiles(Path bundleDir) throws IOException {
        int count = 0;
        for (NebulaImporterBundleWriter.CsvSchema schema : NebulaImporterBundleWriter.edgeSchemas()) {
            Path csvPath = bundleDir.resolve(NebulaImporterBundleWriter.EDGES_DIR_NAME).resolve(schema.getName() + ".csv");
            if (Files.isRegularFile(csvPath) && Files.size(csvPath) > 0L) {
                count++;
            }
        }
        return count;
    }

    private SparkSession createSparkSession(Path warehouseDir) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("spark-nebula-connector-importer")
                .master(options.sparkMaster)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", String.valueOf(options.shufflePartitions))
                .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.catalogImplementation", "in-memory");
        applySparkOverrides(builder);
        return builder.getOrCreate();
    }

    private Dataset<Row> loadCsv(SparkSession spark, Path csvPath, StructType schema) {
        DataFrameReader reader = spark.read()
                .option("header", "false")
                .option("multiLine", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("nullValue", NebulaImporterBundleWriter.NULL_TOKEN)
                .option("mode", "PERMISSIVE")
                .schema(schema);
        Dataset<Row> dataFrame = reader.csv(csvPath.toAbsolutePath().toString());
        if (options.repartition > 0) {
            return dataFrame.repartition(options.repartition);
        }
        return dataFrame;
    }

    private void writeVertices(Dataset<Row> dataFrame, String tagName) {
        dataFrame.write()
                .format(NebulaDataSource.class.getName())
                .mode(SaveMode.Overwrite)
                .option(NebulaOptions.TYPE(), "vertex")
                .option(NebulaOptions.OPERATE_TYPE(), "write")
                .option(NebulaOptions.SPACE_NAME(), graphConfig.getSpace())
                .option(NebulaOptions.LABEL(), tagName)
                .option(NebulaOptions.USER_NAME(), graphConfig.getUsername())
                .option(NebulaOptions.PASSWD(), graphConfig.getPassword())
                .option(NebulaOptions.VERTEX_FIELD(), "vid")
                .option(NebulaOptions.VID_POLICY(), "")
                .option(NebulaOptions.BATCH(), String.valueOf(options.batch))
                .option(NebulaOptions.VID_AS_PROP(), "false")
                .option(NebulaOptions.WRITE_MODE(), "insert")
                .option(NebulaOptions.DELETE_EDGE(), "false")
                .option(NebulaOptions.OVERWRITE(), String.valueOf(options.overwrite))
                .option(NebulaOptions.DISABLE_WRITE_LOG(), String.valueOf(options.disableWriteLog))
                .option(NebulaOptions.META_ADDRESS(), graphConfig.getMetaAddress())
                .option(NebulaOptions.GRAPH_ADDRESS(), graphConfig.getGraphAddress())
                .option(NebulaOptions.TIMEOUT(), String.valueOf(graphConfig.getTimeoutMs()))
                .option(NebulaOptions.CONNECTION_RETRY(), String.valueOf(options.connectionRetry))
                .option(NebulaOptions.EXECUTION_RETRY(), String.valueOf(options.executionRetry))
                .save();
    }

    private void writeEdges(Dataset<Row> dataFrame, String edgeName) {
        dataFrame.write()
                .format(NebulaDataSource.class.getName())
                .mode(SaveMode.Overwrite)
                .option(NebulaOptions.TYPE(), "edge")
                .option(NebulaOptions.OPERATE_TYPE(), "write")
                .option(NebulaOptions.SPACE_NAME(), graphConfig.getSpace())
                .option(NebulaOptions.LABEL(), edgeName)
                .option(NebulaOptions.USER_NAME(), graphConfig.getUsername())
                .option(NebulaOptions.PASSWD(), graphConfig.getPassword())
                .option(NebulaOptions.SRC_VERTEX_FIELD(), "src")
                .option(NebulaOptions.DST_VERTEX_FIELD(), "dst")
                .option(NebulaOptions.SRC_POLICY(), "")
                .option(NebulaOptions.DST_POLICY(), "")
                .option(NebulaOptions.RANK_FIELD(), "rank")
                .option(NebulaOptions.BATCH(), String.valueOf(options.batch))
                .option(NebulaOptions.SRC_AS_PROP(), "false")
                .option(NebulaOptions.DST_AS_PROP(), "false")
                .option(NebulaOptions.RANK_AS_PROP(), "false")
                .option(NebulaOptions.WRITE_MODE(), "insert")
                .option(NebulaOptions.OVERWRITE(), String.valueOf(options.overwrite))
                .option(NebulaOptions.DISABLE_WRITE_LOG(), String.valueOf(options.disableWriteLog))
                .option(NebulaOptions.META_ADDRESS(), graphConfig.getMetaAddress())
                .option(NebulaOptions.GRAPH_ADDRESS(), graphConfig.getGraphAddress())
                .option(NebulaOptions.TIMEOUT(), String.valueOf(graphConfig.getTimeoutMs()))
                .option(NebulaOptions.CONNECTION_RETRY(), String.valueOf(options.connectionRetry))
                .option(NebulaOptions.EXECUTION_RETRY(), String.valueOf(options.executionRetry))
                .save();

    }

    private StructType vertexStruct(NebulaImporterBundleWriter.CsvSchema schema) {
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("vid", DataTypes.StringType, false));
        for (NebulaImporterBundleWriter.PropertySpec property : schema.getProperties()) {
            fields.add(DataTypes.createStructField(property.getName(), sparkType(property.getType()), true));
        }
        return DataTypes.createStructType(fields);
    }

    private StructType edgeStruct(NebulaImporterBundleWriter.CsvSchema schema) {
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("src", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("dst", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("rank", DataTypes.LongType, false));
        for (NebulaImporterBundleWriter.PropertySpec property : schema.getProperties()) {
            fields.add(DataTypes.createStructField(property.getName(), sparkType(property.getType()), true));
        }
        return DataTypes.createStructType(fields);
    }

    private DataType sparkType(String nebulaType) {
        if (nebulaType == null) {
            return DataTypes.StringType;
        }
        String normalized = nebulaType.trim().toLowerCase();
        if ("int".equals(normalized)) {
            return DataTypes.IntegerType;
        }
        if ("bool".equals(normalized) || "boolean".equals(normalized)) {
            return DataTypes.BooleanType;
        }
        if ("timestamp".equals(normalized) || "long".equals(normalized)) {
            return DataTypes.LongType;
        }
        return DataTypes.StringType;
    }

    private BundleManifest readManifest(Path bundleDir) throws IOException {
        Path manifestPath = bundleDir.resolve(NebulaImporterBundleWriter.MANIFEST_FILE_NAME);
        if (!Files.isRegularFile(manifestPath)) {
            return new BundleManifest(-1L, -1L);
        }
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(manifestPath, StandardOpenOption.READ)) {
            properties.load(inputStream);
        }
        return new BundleManifest(
                parseLong(properties.getProperty("total_vertices"), -1L),
                parseLong(properties.getProperty("total_edges"), -1L)
        );
    }

    private static void applySparkOverrides(SparkSession.Builder builder) {
        for (Map.Entry<String, String> entry : prefixedProperties(SPARK_CONF_PREFIX).entrySet()) {
            builder.config(entry.getKey(), entry.getValue());
        }
    }

    private static List<String> loadSchemaStatements(Path bundleDir, String space) throws IOException {
        LinkedHashSet<String> statements = new LinkedHashSet<String>();
        if (bundleDir != null) {
            Path schemaPath = bundleDir.toAbsolutePath().normalize().resolve(NebulaImporterBundleWriter.SCHEMA_FILE_NAME);
            if (Files.isRegularFile(schemaPath)) {
                for (String line : Files.readAllLines(schemaPath, StandardCharsets.UTF_8)) {
                    String statement = line == null ? "" : line.trim();
                    if (statement.isEmpty() || statement.startsWith("#") || statement.startsWith("--")) {
                        continue;
                    }
                    statements.add(statement);
                }
            }
        }
        statements.addAll(NebulaImporterBundleWriter.schemaStatements(space));
        return new ArrayList<String>(statements);
    }

    private static void executeBootstrapStatements(
            NebulaPool pool,
            NebulaGraphConfig graphConfig,
            List<String> statements
    ) {
        if (statements == null || statements.isEmpty()) {
            return;
        }

        try (Session session = pool.getSession(graphConfig.getUsername(), graphConfig.getPassword(), true)) {
            for (String statement : statements) {
                if (isCreateSpaceStatement(statement)) {
                    execute(session, statement);
                }
            }
        } catch (Exception error) {
            throw new IllegalStateException("Failed to bootstrap NebulaGraph space `" + graphConfig.getSpace() + "`.", error);
        }

        waitForSpaceReady(pool, graphConfig);

        try (Session session = pool.getSession(graphConfig.getUsername(), graphConfig.getPassword(), true)) {
            execute(session, "USE `" + graphConfig.getSpace() + "`;");
            for (String statement : statements) {
                if (isCreateSpaceStatement(statement) || isUseStatement(statement)) {
                    continue;
                }
                execute(session, statement);
            }
        } catch (Exception error) {
            throw new IllegalStateException("Failed to create NebulaGraph schema for `" + graphConfig.getSpace() + "`.", error);
        }
    }

    private static void reconcileSchema(NebulaPool pool, NebulaGraphConfig graphConfig) {
        try (Session session = pool.getSession(graphConfig.getUsername(), graphConfig.getPassword(), true)) {
            execute(session, "USE `" + graphConfig.getSpace() + "`;");
            for (NebulaImporterBundleWriter.CsvSchema schema : NebulaImporterBundleWriter.vertexSchemas()) {
                reconcileTag(session, schema);
            }
            for (NebulaImporterBundleWriter.CsvSchema schema : NebulaImporterBundleWriter.edgeSchemas()) {
                reconcileEdge(session, schema);
            }
        } catch (Exception error) {
            throw new IllegalStateException("Failed to reconcile NebulaGraph schema for `" + graphConfig.getSpace() + "`.", error);
        }
    }

    private static void reconcileTag(Session session, NebulaImporterBundleWriter.CsvSchema schema) throws Exception {
        reconcileProperties(session, "TAG", schema);
    }

    private static void reconcileEdge(Session session, NebulaImporterBundleWriter.CsvSchema schema) throws Exception {
        reconcileProperties(session, "EDGE", schema);
    }

    private static void reconcileProperties(
            Session session,
            String entityKind,
            NebulaImporterBundleWriter.CsvSchema schema
    ) throws Exception {
        Map<String, String> existingProperties = describeProperties(session, entityKind, schema.getName());
        List<String> missingDefinitions = new ArrayList<String>();
        for (NebulaImporterBundleWriter.PropertySpec property : schema.getProperties()) {
            String expectedType = normalizeType(property.getType());
            String actualType = existingProperties.get(property.getName());
            if (actualType == null) {
                missingDefinitions.add(renderProperty(property));
                continue;
            }
            if (!expectedType.equals(normalizeType(actualType))) {
                throw new IllegalStateException(
                        entityKind + " `" + schema.getName() + "` property `" + property.getName()
                                + "` type mismatch. expected=" + expectedType + ", actual=" + actualType
                );
            }
        }
        if (missingDefinitions.isEmpty()) {
            return;
        }
        StringBuilder statement = new StringBuilder();
        statement.append("ALTER ").append(entityKind).append(" `").append(schema.getName()).append("` ADD (");
        for (int i = 0; i < missingDefinitions.size(); i++) {
            if (i > 0) {
                statement.append(", ");
            }
            statement.append(missingDefinitions.get(i));
        }
        statement.append(");");
        execute(session, statement.toString());
    }

    private static Map<String, String> describeProperties(Session session, String entityKind, String entityName) throws Exception {
        ResultSet resultSet = execute(session, "DESCRIBE " + entityKind + " `" + entityName + "`;");
        Map<String, String> properties = new LinkedHashMap<String, String>();
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            ResultSet.Record record = resultSet.rowValues(i);
            if (record == null || record.size() < 2) {
                continue;
            }
            String propertyName = stringify(record.get(0));
            String propertyType = stringify(record.get(1));
            if (propertyName == null || propertyName.trim().isEmpty()) {
                continue;
            }
            properties.put(propertyName.trim(), propertyType == null ? "" : propertyType.trim());
        }
        return properties;
    }

    private static void waitForSpaceReady(NebulaPool pool, NebulaGraphConfig graphConfig) {
        RuntimeException lastError = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try (Session session = pool.getSession(graphConfig.getUsername(), graphConfig.getPassword(), true)) {
                ResultSet resultSet = session.execute("USE `" + graphConfig.getSpace() + "`;");
                if (resultSet.isSucceeded()) {
                    return;
                }
                lastError = new IllegalStateException(resultSet.getErrorMessage());
            } catch (Exception error) {
                lastError = new RuntimeException(error);
            }
            sleepQuietly(1000L);
        }
        throw new IllegalStateException("NebulaGraph space `" + graphConfig.getSpace() + "` is not ready.", lastError);
    }

    private static void waitForSchemaReady(NebulaPool pool, NebulaGraphConfig graphConfig) {
        RuntimeException lastError = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try (Session session = pool.getSession(graphConfig.getUsername(), graphConfig.getPassword(), true)) {
                execute(session, "USE `" + graphConfig.getSpace() + "`;");
                for (NebulaImporterBundleWriter.CsvSchema schema : NebulaImporterBundleWriter.vertexSchemas()) {
                    execute(session, "DESCRIBE TAG `" + schema.getName() + "`;");
                }
                for (NebulaImporterBundleWriter.CsvSchema schema : NebulaImporterBundleWriter.edgeSchemas()) {
                    execute(session, "DESCRIBE EDGE `" + schema.getName() + "`;");
                }
                return;
            } catch (Exception error) {
                lastError = new RuntimeException(error);
            }
            sleepQuietly(1000L);
        }
        throw new IllegalStateException("NebulaGraph schema for `" + graphConfig.getSpace() + "` is not ready.", lastError);
    }

    private static void initPool(NebulaPool pool, NebulaGraphConfig graphConfig) {
        NebulaPoolConfig poolConfig = new NebulaPoolConfig()
                .setMinConnSize(graphConfig.getMinConnSize())
                .setMaxConnSize(graphConfig.getMaxConnSize())
                .setTimeout(graphConfig.getTimeoutMs())
                .setMinClusterHealthRate(0.0d);
        try {
            pool.init(Collections.singletonList(new HostAddress(graphConfig.getHost(), graphConfig.getPort())), poolConfig);
        } catch (Exception error) {
            throw new IllegalStateException(
                    "Failed to initialize NebulaGraph pool for " + graphConfig.getHost() + ":" + graphConfig.getPort(),
                    error
            );
        }
    }

    private static ResultSet execute(Session session, String statement) throws Exception {
        ResultSet resultSet = session.execute(statement);
        if (!resultSet.isSucceeded()) {
            throw new IllegalStateException("Failed nGQL: " + statement + ", error=" + resultSet.getErrorMessage());
        }
        return resultSet;
    }

    private static String renderProperty(NebulaImporterBundleWriter.PropertySpec property) {
        return "`" + property.getName() + "` " + property.getType();
    }

    private static String normalizeType(String type) {
        if (type == null) {
            return "";
        }
        String normalized = type.trim().toLowerCase();
        if ("int".equals(normalized) || "integer".equals(normalized) || "long".equals(normalized) || "int64".equals(normalized)) {
            return "int64";
        }
        if ("bool".equals(normalized) || "boolean".equals(normalized)) {
            return "bool";
        }
        return normalized;
    }

    private static boolean isCreateSpaceStatement(String statement) {
        return statement != null && statement.trim().toUpperCase().startsWith("CREATE SPACE");
    }

    private static boolean isUseStatement(String statement) {
        return statement != null && statement.trim().toUpperCase().startsWith("USE ");
    }

    private static String stringify(ValueWrapper valueWrapper) throws Exception {
        if (valueWrapper == null || valueWrapper.isEmpty() || valueWrapper.isNull()) {
            return "";
        }
        if (valueWrapper.isString()) {
            return valueWrapper.asString();
        }
        if (valueWrapper.isLong()) {
            return String.valueOf(valueWrapper.asLong());
        }
        if (valueWrapper.isDouble()) {
            return String.valueOf(valueWrapper.asDouble());
        }
        if (valueWrapper.isBoolean()) {
            return String.valueOf(valueWrapper.asBoolean());
        }
        return valueWrapper.toString();
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    private static Map<String, String> prefixedProperties(String prefix) {
        Properties properties = System.getProperties();
        java.util.LinkedHashMap<String, String> values = new java.util.LinkedHashMap<String, String>();
        for (String key : properties.stringPropertyNames()) {
            if (!key.startsWith(prefix)) {
                continue;
            }
            String value = properties.getProperty(key);
            if (value == null || value.trim().isEmpty()) {
                continue;
            }
            values.put(key.substring(prefix.length()), value.trim());
        }
        return values;
    }

    private void stopSparkSession(SparkSession sparkSession, Path warehouseDir) {
        if (sparkSession != null) {
            try {
                sparkSession.stop();
            } catch (Exception ignored) {
            }
        }
        if (warehouseDir == null || !Files.exists(warehouseDir)) {
            return;
        }
        try {
            Files.walk(warehouseDir)
                    .sorted(Collections.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException ignored) {
        }
    }

    private static String read(String propertyKey, String envKey, String defaultValue) {
        String property = System.getProperty(propertyKey);
        if (property != null && !property.trim().isEmpty()) {
            return property.trim();
        }
        String env = System.getenv(envKey);
        if (env != null && !env.trim().isEmpty()) {
            return env.trim();
        }
        return defaultValue;
    }

    private static int parseInt(String value, int defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException error) {
            return defaultValue;
        }
    }

    private static long parseLong(String value, long defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException error) {
            return defaultValue;
        }
    }

    private static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }

    private static final class BundleManifest {
        private final long totalVertices;
        private final long totalEdges;

        private BundleManifest(long totalVertices, long totalEdges) {
            this.totalVertices = totalVertices;
            this.totalEdges = totalEdges;
        }
    }

    public static final class ImportOptions {
        private final String sparkMaster;
        private final int repartition;
        private final int shufflePartitions;
        private final int batch;
        private final int connectionRetry;
        private final int executionRetry;
        private final boolean disableWriteLog;
        private final boolean overwrite;
        private final int parallelSchemas;
        private final boolean verticesOnly;

        public ImportOptions(
                String sparkMaster,
                int repartition,
                int shufflePartitions,
                int batch,
                int connectionRetry,
                int executionRetry,
                boolean disableWriteLog,
                boolean overwrite,
                int parallelSchemas,
                boolean verticesOnly
        ) {
            this.sparkMaster = sparkMaster == null || sparkMaster.trim().isEmpty() ? "local[*]" : sparkMaster.trim();
            this.repartition = Math.max(0, repartition);
            this.shufflePartitions = Math.max(1, shufflePartitions);
            this.batch = Math.max(1, batch);
            this.connectionRetry = Math.max(0, connectionRetry);
            this.executionRetry = Math.max(0, executionRetry);
            this.disableWriteLog = disableWriteLog;
            this.overwrite = overwrite;
            this.parallelSchemas = Math.max(1, parallelSchemas);
            this.verticesOnly = verticesOnly;
        }

        public static ImportOptions defaults() {
            return new ImportOptions("local[*]", 8, 8, 1024, 3, 3, true, true, 4, false);
        }

        public static ImportOptions fromSystem() {
            return new ImportOptions(
                    read(SPARK_MASTER_PROPERTY, SPARK_MASTER_ENV, "local[*]"),
                    parseInt(read(REPARTITION_PROPERTY, REPARTITION_ENV, "8"), 8),
                    parseInt(read(SHUFFLE_PARTITIONS_PROPERTY, SHUFFLE_PARTITIONS_ENV, "8"), 8),
                    parseInt(read(BATCH_PROPERTY, BATCH_ENV, "1024"), 1024),
                    parseInt(read(CONNECTION_RETRY_PROPERTY, CONNECTION_RETRY_ENV, "3"), 3),
                    parseInt(read(EXECUTION_RETRY_PROPERTY, EXECUTION_RETRY_ENV, "3"), 3),
                    parseBoolean(read(DISABLE_WRITE_LOG_PROPERTY, DISABLE_WRITE_LOG_ENV, "true"), true),
                    parseBoolean(read(OVERWRITE_PROPERTY, OVERWRITE_ENV, "true"), true),
                    parseInt(read(PARALLEL_SCHEMA_PROPERTY, PARALLEL_SCHEMA_ENV, "4"), 4),
                    parseBoolean(read(VERTICES_ONLY_PROPERTY, VERTICES_ONLY_ENV, "false"), false)
            );
        }
    }

    public static final class ImportSummary {
        private final Path bundleDir;
        private final int importedVertexFiles;
        private final int importedEdgeFiles;
        private final long totalVertices;
        private final long totalEdges;
        private final long durationMs;

        private ImportSummary(
                Path bundleDir,
                int importedVertexFiles,
                int importedEdgeFiles,
                long totalVertices,
                long totalEdges,
                long durationMs
        ) {
            this.bundleDir = bundleDir;
            this.importedVertexFiles = importedVertexFiles;
            this.importedEdgeFiles = importedEdgeFiles;
            this.totalVertices = totalVertices;
            this.totalEdges = totalEdges;
            this.durationMs = durationMs;
        }

        public Path getBundleDir() {
            return bundleDir;
        }

        public int getImportedVertexFiles() {
            return importedVertexFiles;
        }

        public int getImportedEdgeFiles() {
            return importedEdgeFiles;
        }

        public long getTotalVertices() {
            return totalVertices;
        }

        public long getTotalEdges() {
            return totalEdges;
        }

        public long getDurationMs() {
            return durationMs;
        }
    }
}
