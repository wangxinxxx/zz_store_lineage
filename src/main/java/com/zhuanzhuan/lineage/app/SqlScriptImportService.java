package com.zhuanzhuan.lineage.app;

import com.zhuanzhuan.lineage.common.HashUtils;
import com.zhuanzhuan.lineage.common.ScalaInterop;
import com.zhuanzhuan.lineage.metadata.DataMapMetadataClient;
import com.zhuanzhuan.lineage.metadata.DataMapTableMetadataFetcher;
import com.zhuanzhuan.lineage.metadata.LocalTableMetadataStore;
import com.zhuanzhuan.lineage.metadata.TableMetadataSnapshot;
import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExecutionStatus;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.RawPlanSnapshot;
import com.zhuanzhuan.lineage.model.SparkAppContext;
import com.zhuanzhuan.lineage.model.TableRef;
import com.zhuanzhuan.lineage.parser.DefaultSparkLineageParser;
import com.zhuanzhuan.lineage.storage.LineageStorage;
import com.zhuanzhuan.lineage.storage.LineageStorageFactory;
import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;
import com.zhuanzhuan.lineage.storage.nebula.NebulaImporterBundleWriter;
import com.zhuanzhuan.lineage.storage.nebula.SparkNebulaConnectorImporter;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.Tuple2;
import scala.collection.Seq;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SqlScriptImportService implements AutoCloseable {
    public static final String LOCAL_METADATA_CACHE_DIR_PROPERTY = "zz.lineage.local.metadata.cache.dir";
    public static final String LOCAL_METADATA_CACHE_DIR_ENV = "ZZ_LINEAGE_LOCAL_METADATA_CACHE_DIR";
    public static final String DATAMAP_BASE_URL_PROPERTY = "zz.lineage.datamap.base.url";
    public static final String DATAMAP_BASE_URL_ENV = "ZZ_LINEAGE_DATAMAP_BASE_URL";
    public static final String DATAMAP_COOKIE_PROPERTY = "zz.lineage.datamap.cookie";
    public static final String DATAMAP_COOKIE_ENV = "ZZ_LINEAGE_DATAMAP_COOKIE";
    public static final String DEFAULT_DATAMAP_BASE_URL = "https://dp.58corp.com";
    public static final String SPARK_CONF_PREFIX = "zz.lineage.spark.conf.";
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([^}]+)}");
    private static final DateTimeFormatter DATE_TOKEN = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final String STORAGE_TYPE_NEBULA = "nebula";

    private SparkSession spark;
    private final DefaultSparkLineageParser parser;
    private final LineageStorage storage;
    private final String owner;
    private final String bizDateToken;
    private final Set<String> hydratedTables;
    private final LocalTableMetadataStore metadataStore;
    private final DataMapTableMetadataFetcher metadataFetcher;
    private final NebulaImporterBundleWriter bundleWriter;
    private final SparkNebulaConnectorImporter connectorImporter;

    public SqlScriptImportService(
            SparkSession spark,
            DefaultSparkLineageParser parser,
            LineageStorage storage,
            String owner,
            String bizDateToken,
            LocalTableMetadataStore metadataStore,
            DataMapTableMetadataFetcher metadataFetcher,
            NebulaImporterBundleWriter bundleWriter,
            SparkNebulaConnectorImporter connectorImporter
    ) {
        this.spark = spark;
        this.parser = parser;
        this.storage = storage;
        this.owner = owner;
        this.bizDateToken = bizDateToken;
        this.hydratedTables = new HashSet<String>();
        this.metadataStore = metadataStore;
        this.metadataFetcher = metadataFetcher;
        this.bundleWriter = bundleWriter;
        this.connectorImporter = connectorImporter;
    }

    public static SqlScriptImportService createDefault() throws IOException {
        ensureNebulaMode();
        Path warehouseDir = Files.createTempDirectory("spark-script-import-warehouse-");
        SparkSession spark = createSparkSession(warehouseDir);
        spark.sparkContext().setLogLevel("WARN");
        LocalTableMetadataStore metadataStore = new LocalTableMetadataStore(resolveMetadataCacheDir());
        String dataMapBaseUrl = read(DATAMAP_BASE_URL_PROPERTY, DATAMAP_BASE_URL_ENV);
        if (dataMapBaseUrl == null || dataMapBaseUrl.trim().isEmpty()) {
            dataMapBaseUrl = DEFAULT_DATAMAP_BASE_URL;
        }
        String dataMapCookie = read(DATAMAP_COOKIE_PROPERTY, DATAMAP_COOKIE_ENV);
        DataMapTableMetadataFetcher metadataFetcher = new DataMapTableMetadataFetcher(
                new DataMapMetadataClient(dataMapBaseUrl, dataMapCookie),
                dataMapBaseUrl
        );
        return new SqlScriptImportService(
                spark,
                new DefaultSparkLineageParser(),
                LineageStorageFactory.createDefault(),
                "sql_script_importer",
                LocalDate.now().format(DATE_TOKEN),
                metadataStore,
                metadataFetcher,
                new NebulaImporterBundleWriter(
                        NebulaImporterBundleWriter.resolveBundleDirFromSystem(),
                        NebulaGraphConfig.fromSystem(),
                        NebulaImporterBundleWriter.ImporterOptions.fromSystem()
                ),
                new SparkNebulaConnectorImporter(
                        NebulaGraphConfig.fromSystem(),
                        SparkNebulaConnectorImporter.ImportOptions.fromSystem()
                )
        );
    }

    private static Path resolveMetadataCacheDir() throws IOException {
        String configured = read(LOCAL_METADATA_CACHE_DIR_PROPERTY, LOCAL_METADATA_CACHE_DIR_ENV);
        if (configured != null && !configured.trim().isEmpty()) {
            Path path = Paths.get(configured).toAbsolutePath().normalize();
            Files.createDirectories(path);
            return path;
        }
        Path path = Paths.get(System.getProperty("user.dir"), ".lineage-metadata-cache").toAbsolutePath().normalize();
        Files.createDirectories(path);
        return path;
    }

    private static SparkSession createSparkSession(Path warehouseDir) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("sql-script-import-service")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.legacy.createHiveTableByDefault", "false");
        applySparkOverrides(builder);
        builder.config("spark.sql.catalogImplementation", "in-memory");
        return builder.getOrCreate();
    }

    private void stopActiveSparkSession() {
        if (spark == null) {
            return;
        }
        Path warehousePath = null;
        try {
            warehousePath = Paths.get(spark.conf().get("spark.sql.warehouse.dir"));
        } catch (Exception ignored) {
        }
        try {
            spark.stop();
        } finally {
            spark = null;
            if (warehousePath != null) {
                deleteRecursively(warehousePath);
            }
        }
    }

    public void clearStorage() {
        storage.clear();
    }

    public MetadataSyncSummary syncMetadata(Path scriptPath) throws IOException {
        Path absolutePath = scriptPath.toAbsolutePath().normalize();
        List<Path> sqlFiles;
        if (Files.isDirectory(absolutePath)) {
            sqlFiles = listSqlFiles(absolutePath);
        } else {
            sqlFiles = Collections.singletonList(absolutePath);
        }

        LinkedHashMap<String, TableRef> referencedTables = new LinkedHashMap<String, TableRef>();
        for (Path sqlFile : sqlFiles) {
            String sqlText = new String(Files.readAllBytes(sqlFile), StandardCharsets.UTF_8);
            for (String statement : renderLineageStatements(splitStatements(sqlText), defaultVariables())) {
                try {
                    LogicalPlan logicalPlan = parsePlan(statement);
                    for (TableRef tableRef : collectReferencedTables(logicalPlan)) {
                        referencedTables.put(tableRef.normalizedName(), tableRef);
                    }
                } catch (Exception ignored) {
                }
            }
        }

        String batchName = buildBatchName(absolutePath);
        MetadataSyncSummary summary = new MetadataSyncSummary(absolutePath, sqlFiles.size(), referencedTables.size());
        summary.markStarted();
        summary.referencedTablesPath = metadataStore.saveBatchTableList(batchName, new ArrayList<TableRef>(referencedTables.values()));
        for (TableRef tableRef : referencedTables.values()) {
            if (metadataStore.exists(tableRef)) {
                summary.cachedBefore++;
            }
        }
        MetadataSyncResult syncResult = syncMetadataSnapshots(new ArrayList<TableRef>(referencedTables.values()));
        summary.failures.addAll(syncResult.failures);
        summary.failuresPath = metadataStore.saveBatchFailures(batchName, syncResult.failures);
        for (TableRef tableRef : referencedTables.values()) {
            if (metadataStore.exists(tableRef)) {
                summary.cachedAfter++;
            }
        }
        summary.markFinished();
        return summary;
    }

    public NebulaImporterBundleWriter.BundleSummary exportImporterBundle(Path scriptPath) throws IOException {
        Path absolutePath = scriptPath.toAbsolutePath().normalize();
        if (Files.isDirectory(absolutePath)) {
            return exportImporterBundleDirectory(absolutePath);
        }
        try (NebulaImporterBundleWriter.BundleSession bundleSession = bundleWriter.openSession(buildBatchName(absolutePath))) {
            PreparedImport prepared = prepareImport(absolutePath, false);
            bundleSession.appendScript(absolutePath, prepared.events, prepared.results);
            return bundleSession.finish();
        }
    }

    public SparkNebulaConnectorImporter.ImportSummary runSparkImportBundle(Path bundleDir) throws IOException {
        stopActiveSparkSession();
        return connectorImporter.importBundle(bundleDir);
    }

    private NebulaImporterBundleWriter.BundleSummary exportImporterBundleDirectory(Path directoryPath) throws IOException {
        if (!Files.isDirectory(directoryPath)) {
            throw new IllegalArgumentException("Path is not a directory: " + directoryPath);
        }
        List<Path> sqlFiles = listSqlFiles(directoryPath);
        try (NebulaImporterBundleWriter.BundleSession bundleSession = bundleWriter.openSession(buildBatchName(directoryPath))) {
            for (Path sqlFile : sqlFiles) {
                PreparedImport prepared = prepareImport(sqlFile, false);
                bundleSession.appendScript(sqlFile, prepared.events, prepared.results);
            }
            return bundleSession.finish();
        }
    }

    private PreparedImport prepareImport(Path scriptPath, boolean syncMetadataFirst) throws IOException {
        Path absolutePath = scriptPath.toAbsolutePath().normalize();
        String sqlText = new String(Files.readAllBytes(absolutePath), StandardCharsets.UTF_8);
        Map<String, String> variables = defaultVariables();
        List<String> statements = splitStatements(sqlText);
        if (syncMetadataFirst) {
            syncMetadataForStatements(renderLineageStatements(statements, variables), absolutePath);
        }

        List<ExecutionCaptureEvent> bufferedEvents = new ArrayList<ExecutionCaptureEvent>();
        List<NormalizedLineageResult> bufferedResults = new ArrayList<NormalizedLineageResult>();

        int lineageStatementIndex = 0;
        for (String rawStatement : statements) {
            String statement = rawStatement.trim();
            if (statement.isEmpty()) {
                continue;
            }

            if (isSetStatement(statement)) {
                registerSetVariable(variables, statement);
                continue;
            }

            String renderedStatement = replacePlaceholders(statement, variables);
            if (!isLineageStatement(renderedStatement)) {
                continue;
            }

            lineageStatementIndex++;
            try {
                AnalysisBundle analysisBundle = analyzeStatement(renderedStatement);
                ExecutionCaptureEvent event = buildEvent(absolutePath, lineageStatementIndex, renderedStatement, analysisBundle);
                NormalizedLineageResult result = parser.parse(
                        event,
                        analysisBundle.logicalPlan,
                        analysisBundle.analyzedPlan,
                        analysisBundle.optimizedPlan
                );
                bufferedEvents.add(event);
                bufferedResults.add(result);
            } catch (Exception error) {
            }
        }
        return new PreparedImport(bufferedEvents, bufferedResults);
    }

    @Override
    public void close() {
        try {
            stopActiveSparkSession();
        } finally {
            if (storage instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) storage).close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static void ensureNebulaMode() {
        if (System.getProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY) == null
                && System.getenv(LineageStorageFactory.STORAGE_TYPE_ENV) == null) {
            System.setProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY, STORAGE_TYPE_NEBULA);
        }
    }

    private LogicalPlan parsePlan(String sql) {
        Object sessionState = invokeNoArg(spark, "sessionState");
        Object sqlParser = invokeNoArg(sessionState, "sqlParser");
        Object logicalPlan = invokeOneArg(sqlParser, "parsePlan", String.class, sql);
        if (!(logicalPlan instanceof LogicalPlan)) {
            throw new IllegalStateException("Spark sqlParser did not return a LogicalPlan.");
        }
        return (LogicalPlan) logicalPlan;
    }

    private AnalysisBundle analyzeStatement(String sql) {
        LogicalPlan logicalPlan = parsePlan(sql);
        InsertIntoStatement insert = extractInsertStatement(logicalPlan);
        LogicalPlan analysisPlan = insert == null
                ? logicalPlan
                : parsePlan(stripInsertClause(sql));
        hydrateTablesForAnalysis(analysisPlan, sql);
        return executeAnalysis(logicalPlan, analysisPlan);
    }

    private void syncMetadataForStatements(List<String> statements, Path batchPath) throws IOException {
        if (statements == null || statements.isEmpty()) {
            return;
        }
        List<TableRef> referencedTables = collectReferencedTablesForStatements(statements);
        String batchName = buildBatchName(batchPath);
        metadataStore.saveBatchTableList(batchName, referencedTables);
        MetadataSyncResult result = syncMetadataSnapshots(referencedTables);
        metadataStore.saveBatchFailures(batchName, result.failures);
    }

    private List<TableRef> collectReferencedTablesForStatements(List<String> statements) {
        LinkedHashMap<String, TableRef> referencedTables = new LinkedHashMap<String, TableRef>();
        for (String statement : statements) {
            if (statement == null || statement.trim().isEmpty()) {
                continue;
            }
            try {
                LogicalPlan logicalPlan = parsePlan(statement);
                for (TableRef tableRef : collectReferencedTables(logicalPlan)) {
                    referencedTables.put(tableRef.normalizedName(), tableRef);
                }
            } catch (Exception ignored) {
            }
        }
        return new ArrayList<TableRef>(referencedTables.values());
    }

    private AnalysisBundle executeAnalysis(LogicalPlan logicalPlan, LogicalPlan planToExecute) {
        Object sessionState = invokeNoArg(spark, "sessionState");
        Object queryExecution = invokeCompatibleOneArg(sessionState, "executePlan", planToExecute);

        if (queryExecution != null) {
            LogicalPlan analyzedPlan = asLogicalPlan(invokeNoArg(queryExecution, "analyzed"));
            LogicalPlan optimizedPlan = asLogicalPlan(invokeNoArg(queryExecution, "optimizedPlan"));
            return new AnalysisBundle(
                    logicalPlan,
                    analyzedPlan == null ? planToExecute : analyzedPlan,
                    optimizedPlan == null ? (analyzedPlan == null ? planToExecute : analyzedPlan) : optimizedPlan
            );
        }

        Object analyzer = invokeNoArg(sessionState, "analyzer");
        Object analyzed = invokeCompatibleOneArg(analyzer, "execute", planToExecute);
        LogicalPlan analyzedPlan = asLogicalPlan(analyzed);
        if (analyzedPlan == null) {
            analyzedPlan = planToExecute;
        }

        Object optimizer = invokeNoArg(sessionState, "optimizer");
        Object optimized = invokeCompatibleOneArg(optimizer, "execute", analyzedPlan);
        LogicalPlan optimizedPlan = asLogicalPlan(optimized);
        if (optimizedPlan == null) {
            optimizedPlan = analyzedPlan;
        }
        return new AnalysisBundle(logicalPlan, analyzedPlan, optimizedPlan);
    }

    private LogicalPlan unwrapAnalysisPlan(LogicalPlan logicalPlan) {
        InsertIntoStatement insert = extractInsertStatement(logicalPlan);
        if (insert != null) {
            return insert.query();
        }
        return logicalPlan;
    }

    private String stripInsertClause(String sql) {
        return sql.replaceFirst(
                "(?is)insert\\s+(?:overwrite|into)\\s+table\\s+[a-zA-Z_][\\w]*(?:\\.[a-zA-Z_][\\w]*)+\\s*(?:partition\\s*\\([^)]*\\))?\\s*",
                ""
        );
    }

    private InsertIntoStatement extractInsertStatement(LogicalPlan logicalPlan) {
        if (logicalPlan == null) {
            return null;
        }
        if (logicalPlan instanceof InsertIntoStatement) {
            return (InsertIntoStatement) logicalPlan;
        }
        LogicalPlan transparentChild = extractTransparentChildPlan(logicalPlan);
        if (transparentChild != null && transparentChild != logicalPlan) {
            InsertIntoStatement nested = extractInsertStatement(transparentChild);
            if (nested != null) {
                return nested;
            }
        }
        for (LogicalPlan child : ScalaInterop.toJavaList(logicalPlan.children())) {
            InsertIntoStatement nested = extractInsertStatement(child);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    private void hydrateTablesForAnalysis(LogicalPlan logicalPlan, String sqlText) {
        if (logicalPlan == null) {
            return;
        }
        List<TableRef> referencedTables = collectReferencedTables(logicalPlan);
        for (TableRef tableRef : referencedTables) {
            hydrateTable(tableRef, sqlText);
        }
    }

    private MetadataSyncResult syncMetadataSnapshots(List<TableRef> tableRefs) {
        MetadataSyncResult result = new MetadataSyncResult();
        if (tableRefs == null || tableRefs.isEmpty()) {
            return result;
        }
        List<TableRef> missingTables = new ArrayList<TableRef>();
        for (TableRef tableRef : tableRefs) {
            if (tableRef == null || tableRef.getName() == null || tableRef.getName().trim().isEmpty()) {
                continue;
            }
            if (!shouldRefreshMetadata(tableRef)) {
                continue;
            }
            missingTables.add(tableRef);
        }
        batchSyncMetadata(missingTables, result);
        return result;
    }

    private void batchSyncMetadata(List<TableRef> tableRefs, MetadataSyncResult result) {
        if (tableRefs == null || tableRefs.isEmpty()) {
            return;
        }
        for (TableRef tableRef : tableRefs) {
            try {
                syncSingleTableMetadata(tableRef);
                result.syncedTables.add(tableRef.normalizedName());
            } catch (Exception error) {
                result.failedTables.add(tableRef.normalizedName());
                result.failures.add(tableRef.normalizedName() + ": " + formatError(error));
            }
        }
    }

    private boolean shouldRefreshMetadata(TableRef tableRef) {
        if (!metadataStore.exists(tableRef)) {
            return true;
        }
        if (metadataStore.needsReadableRewrite(tableRef)) {
            return true;
        }
        try {
            java.util.Optional<TableMetadataSnapshot> snapshot = metadataStore.load(tableRef);
            if (!snapshot.isPresent()) {
                return true;
            }
            if ("manual".equalsIgnoreCase(String.valueOf(snapshot.get().getMetastoreUri()))) {
                return false;
            }
            if (snapshot.get().getColumns().isEmpty()) {
                return true;
            }
            for (FieldSchema field : snapshot.get().getColumns()) {
                if (field != null && field.getComment() != null && !field.getComment().trim().isEmpty()) {
                    return false;
                }
            }
            return true;
        } catch (IOException ignored) {
            return true;
        }
    }

    private void hydrateTargetTableForAnalysis(InsertIntoStatement insert, LogicalPlan analyzedQueryPlan, String sqlText) {
        if (insert == null || analyzedQueryPlan == null) {
            return;
        }
        TableRef targetTable = extractInsertTarget(insert, sqlText);
        if (targetTable == null) {
            return;
        }
        List<FieldSchema> outputColumns = new ArrayList<FieldSchema>();
        for (Attribute attribute : ScalaInterop.toJavaList(analyzedQueryPlan.output())) {
            if (attribute == null || attribute.name() == null || attribute.name().trim().isEmpty()) {
                continue;
            }
            outputColumns.add(new FieldSchema(attribute.name(), attribute.dataType().simpleString(), "derived_target_placeholder"));
        }
        createPlaceholderTable(
                targetTable.getDatabase() == null || targetTable.getDatabase().trim().isEmpty() ? "default" : targetTable.getDatabase(),
                targetTable.getName(),
                outputColumns,
                extractInsertPartitionKeys(sqlText)
        );
    }

    private TableRef extractInsertTarget(InsertIntoStatement insert, String sqlText) {
        LogicalPlan tablePlan = insert.table();
        if (tablePlan instanceof UnresolvedRelation) {
            List<String> parts = ScalaInterop.toJavaList(((UnresolvedRelation) tablePlan).multipartIdentifier());
            if (parts.size() >= 2) {
                return TableRef.fromParts(parts);
            }
        }
        Pattern targetPattern = Pattern.compile(
                "(?is)insert\\s+(?:overwrite|into)\\s+table\\s+([a-zA-Z_][\\w]*(?:\\.[a-zA-Z_][\\w]*)+)"
        );
        Matcher matcher = targetPattern.matcher(sqlText);
        if (matcher.find()) {
            String[] parts = matcher.group(1).split("\\.");
            if (parts.length >= 2) {
                return TableRef.fromParts(Arrays.asList(parts));
            }
        }
        return null;
    }

    private List<FieldSchema> extractInsertPartitionKeys(String sqlText) {
        Pattern partitionPattern = Pattern.compile("(?is)partition\\s*\\((.*?)\\)");
        Matcher matcher = partitionPattern.matcher(sqlText);
        if (!matcher.find()) {
            return Collections.emptyList();
        }
        List<FieldSchema> partitionKeys = new ArrayList<FieldSchema>();
        for (String part : matcher.group(1).split(",")) {
            String[] keyValue = part.split("=", 2);
            String name = keyValue[0].trim().replace("`", "");
            if (name.isEmpty()) {
                continue;
            }
            partitionKeys.add(new FieldSchema(name, "string", "derived_partition_key"));
        }
        return partitionKeys;
    }

    private List<TableRef> collectReferencedTables(LogicalPlan logicalPlan) {
        LinkedHashMap<String, TableRef> tables = new LinkedHashMap<String, TableRef>();
        collectReferencedTables(logicalPlan, tables, new HashSet<String>());
        return new ArrayList<TableRef>(tables.values());
    }

    private void collectReferencedTables(LogicalPlan logicalPlan, Map<String, TableRef> tables, Set<String> visited) {
        if (logicalPlan == null) {
            return;
        }
        String visitKey = logicalPlan.nodeName() + "|" + System.identityHashCode(logicalPlan);
        if (!visited.add(visitKey)) {
            return;
        }
        try {
            if (logicalPlan instanceof UnresolvedRelation) {
                List<String> parts = ScalaInterop.toJavaList(((UnresolvedRelation) logicalPlan).multipartIdentifier());
                if (parts.size() >= 2) {
                    TableRef tableRef = TableRef.fromParts(parts);
                    tables.put(tableRef.normalizedName(), tableRef);
                }
            }
            for (LogicalPlan cteRelation : extractCteRelationPlans(logicalPlan)) {
                collectReferencedTables(cteRelation, tables, visited);
            }
            if (logicalPlan instanceof InsertIntoStatement) {
                collectReferencedTables(((InsertIntoStatement) logicalPlan).query(), tables, visited);
                return;
            }
            LogicalPlan reflectedChild = extractTransparentChildPlan(logicalPlan);
            if (reflectedChild != null && reflectedChild != logicalPlan) {
                collectReferencedTables(reflectedChild, tables, visited);
            }
            for (LogicalPlan child : ScalaInterop.toJavaList(logicalPlan.children())) {
                collectReferencedTables(child, tables, visited);
            }
        } finally {
            visited.remove(visitKey);
        }
    }

    private void hydrateTable(TableRef tableRef, String sqlText) {
        if (tableRef == null || tableRef.getName() == null || tableRef.getName().trim().isEmpty()) {
            return;
        }
        String database = tableRef.getDatabase() == null || tableRef.getDatabase().trim().isEmpty()
                ? "default"
                : tableRef.getDatabase();
        String normalizedName = database + "." + tableRef.getName();
        if (!hydratedTables.add(normalizedName)) {
            return;
        }

        try {
            java.util.Optional<TableMetadataSnapshot> snapshot = metadataStore.load(tableRef);
            if (snapshot.isPresent()) {
                TableMetadataSnapshot value = snapshot.get();
                createPlaceholderView(
                        database,
                        tableRef.getName(),
                        value.getColumns(),
                        value.getPartitionKeys()
                );
                return;
            }
        } catch (IOException ignored) {
        }

        List<FieldSchema> sqlDerivedColumns = deriveColumnsFromSql(sqlText, tableRef);
        if (!sqlDerivedColumns.isEmpty()) {
            createPlaceholderView(database, tableRef.getName(), sqlDerivedColumns, Collections.<FieldSchema>emptyList());
            return;
        }

        hydratedTables.remove(normalizedName);
        throw new IllegalStateException(
                "Failed to hydrate table metadata for " + normalizedName
                        + ". Metadata snapshot is missing from local cache and SQL fallback could not infer columns."
        );
    }

    private void syncSingleTableMetadata(TableRef tableRef) {
        if (tableRef == null) {
            return;
        }
        try {
            metadataStore.save(metadataFetcher.fetch(tableRef));
        } catch (Exception error) {
            throw new IllegalStateException("Failed to sync metadata snapshot for " + tableRef.normalizedName(), error);
        }
    }

    private void createPlaceholderTable(
            String database,
            String tableName,
            List<FieldSchema> columns,
            List<FieldSchema> partitionKeys
    ) {
        spark.sql("CREATE DATABASE IF NOT EXISTS " + quotedIdentifier(database));
        List<FieldSchema> effectiveColumns = deduplicateColumnsIgnoreCase(removePartitionColumns(columns, partitionKeys));
        List<FieldSchema> effectivePartitionKeys = deduplicateColumnsIgnoreCase(partitionKeys);

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ")
                .append(quotedIdentifier(database))
                .append(".")
                .append(quotedIdentifier(tableName))
                .append(" (")
                .append(joinColumns(effectiveColumns))
                .append(")")
                .append(" USING parquet");
        if (effectivePartitionKeys != null && !effectivePartitionKeys.isEmpty()) {
            ddl.append(" PARTITIONED BY (").append(joinColumns(effectivePartitionKeys)).append(")");
        }
        spark.sql(ddl.toString());
    }

    private void createPlaceholderView(
            String database,
            String tableName,
            List<FieldSchema> columns,
            List<FieldSchema> partitionKeys
    ) {
        spark.sql("CREATE DATABASE IF NOT EXISTS " + quotedIdentifier(database));
        List<FieldSchema> effectiveColumns = deduplicateColumnsIgnoreCase(mergeColumns(columns, partitionKeys));

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE OR REPLACE VIEW ")
                .append(quotedIdentifier(database))
                .append(".")
                .append(quotedIdentifier(tableName))
                .append(" AS SELECT ")
                .append(renderPlaceholderProjection(effectiveColumns))
                .append(" WHERE 1 = 0");
        spark.sql(ddl.toString());
    }

    private List<FieldSchema> removePartitionColumns(List<FieldSchema> columns, List<FieldSchema> partitionKeys) {
        if (columns == null || columns.isEmpty() || partitionKeys == null || partitionKeys.isEmpty()) {
            return columns;
        }
        Set<String> partitionNames = new HashSet<String>();
        for (FieldSchema partitionKey : partitionKeys) {
            if (partitionKey == null || partitionKey.getName() == null) {
                continue;
            }
            partitionNames.add(partitionKey.getName().toLowerCase(Locale.ROOT));
        }
        List<FieldSchema> filtered = new ArrayList<FieldSchema>(columns.size());
        for (FieldSchema column : columns) {
            if (column == null || column.getName() == null) {
                continue;
            }
            if (partitionNames.contains(column.getName().toLowerCase(Locale.ROOT))) {
                continue;
            }
            filtered.add(column);
        }
        return filtered;
    }

    private List<FieldSchema> mergeColumns(List<FieldSchema> primary, List<FieldSchema> supplement) {
        LinkedHashMap<String, FieldSchema> merged = new LinkedHashMap<String, FieldSchema>();
        if (primary != null) {
            for (FieldSchema field : primary) {
                if (field == null || field.getName() == null || field.getName().trim().isEmpty()) {
                    continue;
                }
                merged.put(field.getName(), field);
            }
        }
        if (supplement != null) {
            for (FieldSchema field : supplement) {
                if (field == null || field.getName() == null || field.getName().trim().isEmpty()) {
                    continue;
                }
                if (!merged.containsKey(field.getName())) {
                    merged.put(field.getName(), field);
                }
            }
        }
        return new ArrayList<FieldSchema>(merged.values());
    }

    private List<FieldSchema> deduplicateColumnsIgnoreCase(List<FieldSchema> columns) {
        if (columns == null || columns.isEmpty()) {
            return columns;
        }
        LinkedHashMap<String, FieldSchema> deduped = new LinkedHashMap<String, FieldSchema>();
        for (FieldSchema field : columns) {
            if (field == null || field.getName() == null || field.getName().trim().isEmpty()) {
                continue;
            }
            String key = field.getName().trim().toLowerCase(Locale.ROOT);
            if (!deduped.containsKey(key)) {
                deduped.put(key, field);
            }
        }
        return new ArrayList<FieldSchema>(deduped.values());
    }

    private String renderPlaceholderProjection(List<FieldSchema> columns) {
        List<String> rendered = new ArrayList<String>();
        if (columns != null) {
            for (FieldSchema column : columns) {
                if (column == null || column.getName() == null || column.getName().trim().isEmpty()) {
                    continue;
                }
                String type = column.getType() == null || column.getType().trim().isEmpty()
                        ? "string"
                        : column.getType().trim();
                rendered.add("CAST(NULL AS " + type + ") AS " + quotedIdentifier(column.getName()));
            }
        }
        if (rendered.isEmpty()) {
            rendered.add("CAST(NULL AS string) AS " + quotedIdentifier("_placeholder_col"));
        }
        return String.join(", ", rendered);
    }

    private List<FieldSchema> deriveColumnsFromSql(String sqlText, TableRef tableRef) {
        if (sqlText == null || sqlText.trim().isEmpty() || tableRef == null) {
            return Collections.emptyList();
        }
        String normalizedTableName = tableRef.normalizedName();
        LinkedHashMap<String, FieldSchema> columns = new LinkedHashMap<String, FieldSchema>();

        collectQualifiedColumns(sqlText, normalizedTableName, columns);

        Pattern aliasPattern = Pattern.compile(
                "(?i)(?:from|join)\\s+`?" + Pattern.quote(normalizedTableName) + "`?\\s+(?:as\\s+)?([a-zA-Z_][\\w]*)"
        );
        Matcher aliasMatcher = aliasPattern.matcher(sqlText);
        while (aliasMatcher.find()) {
            String alias = aliasMatcher.group(1);
            collectQualifiedColumns(sqlText, alias, columns);
            collectJoinedAliasSourceColumns(sqlText, aliasMatcher.start(), columns);
        }

        Pattern selectBlockPattern = Pattern.compile(
                "(?is)select\\s+(.*?)\\s+from\\s+`?" + Pattern.quote(normalizedTableName) + "`?(?:\\s|$)"
        );
        Matcher selectBlockMatcher = selectBlockPattern.matcher(sqlText);
        while (selectBlockMatcher.find()) {
            collectSelectClauseColumns(selectBlockMatcher.group(1), columns);
        }

        collectDirectTableWhereColumns(sqlText, normalizedTableName, columns);

        if (columns.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<FieldSchema>(columns.values());
    }

    private void collectQualifiedColumns(String sqlText, String qualifier, Map<String, FieldSchema> columns) {
        if (qualifier == null || qualifier.trim().isEmpty()) {
            return;
        }
        Pattern columnPattern = Pattern.compile(
                "(?i)(?:`?" + Pattern.quote(qualifier) + "`?)\\.(`?[a-zA-Z_][\\w]*`?)"
        );
        Matcher columnMatcher = columnPattern.matcher(sqlText);
        while (columnMatcher.find()) {
            String rawColumn = columnMatcher.group(1);
            String columnName = rawColumn == null ? null : rawColumn.replace("`", "");
            if (columnName == null || columnName.trim().isEmpty()) {
                continue;
            }
            if (!columns.containsKey(columnName)) {
                columns.put(columnName, new FieldSchema(columnName, "string", "derived_from_sql_fallback"));
            }
        }
    }

    private void collectSelectClauseColumns(String selectClause, Map<String, FieldSchema> columns) {
        if (selectClause == null || selectClause.trim().isEmpty()) {
            return;
        }
        String normalized = selectClause.replaceAll("(?m)--.*$", " ");
        Pattern tokenPattern = Pattern.compile("\\b([a-zA-Z_][\\w]*)\\b");
        Matcher tokenMatcher = tokenPattern.matcher(normalized);
        while (tokenMatcher.find()) {
            String token = tokenMatcher.group(1);
            if (token == null) {
                continue;
            }
            String normalizedToken = token.toLowerCase(Locale.ROOT);
            if (isSqlNoiseToken(normalizedToken)) {
                continue;
            }
            if (!columns.containsKey(token)) {
                columns.put(token, new FieldSchema(token, "string", "derived_from_select_clause"));
            }
        }
    }

    private void collectDirectTableWhereColumns(String sqlText, String normalizedTableName, Map<String, FieldSchema> columns) {
        if (sqlText == null || sqlText.trim().isEmpty() || normalizedTableName == null || normalizedTableName.trim().isEmpty()) {
            return;
        }
        Pattern wherePattern = Pattern.compile(
                "(?is)from\\s+`?" + Pattern.quote(normalizedTableName)
                        + "`?\\s+where\\s+(.*?)(?=(?:group\\s+by|having|union\\s+all|union|order\\s+by|limit|\\)\\s*(?:,|select|insert|$)|$))"
        );
        Matcher whereMatcher = wherePattern.matcher(sqlText);
        while (whereMatcher.find()) {
            collectSelectClauseColumns(whereMatcher.group(1), columns);
        }
    }

    private void collectJoinedAliasSourceColumns(String sqlText, int tableReferenceStart, Map<String, FieldSchema> columns) {
        if (sqlText == null || sqlText.trim().isEmpty() || tableReferenceStart <= 0) {
            return;
        }
        String lower = sqlText.toLowerCase(Locale.ROOT);
        int selectIndex = lower.lastIndexOf("select", tableReferenceStart);
        if (selectIndex < 0) {
            return;
        }
        int fromIndex = lower.indexOf("from", selectIndex);
        if (fromIndex < 0 || fromIndex > tableReferenceStart) {
            return;
        }
        String selectClause = sqlText.substring(selectIndex + "select".length(), fromIndex);
        collectArithmeticLeadingIdentifiers(selectClause, columns);
    }

    private void collectArithmeticLeadingIdentifiers(String selectClause, Map<String, FieldSchema> columns) {
        if (selectClause == null || selectClause.trim().isEmpty()) {
            return;
        }
        for (String expression : splitTopLevelExpressions(selectClause)) {
            String candidate = extractLeadingArithmeticIdentifier(expression);
            if (candidate == null || candidate.trim().isEmpty()) {
                continue;
            }
            if (!columns.containsKey(candidate)) {
                columns.put(candidate, new FieldSchema(candidate, "string", "derived_from_join_expression"));
            }
        }
    }

    private List<String> splitTopLevelExpressions(String selectClause) {
        List<String> expressions = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean singleQuote = false;
        boolean doubleQuote = false;
        boolean backtickQuote = false;
        for (int i = 0; i < selectClause.length(); i++) {
            char currentChar = selectClause.charAt(i);
            if (currentChar == '\'' && !doubleQuote && !backtickQuote) {
                singleQuote = !singleQuote;
            } else if (currentChar == '"' && !singleQuote && !backtickQuote) {
                doubleQuote = !doubleQuote;
            } else if (currentChar == '`' && !singleQuote && !doubleQuote) {
                backtickQuote = !backtickQuote;
            } else if (!singleQuote && !doubleQuote && !backtickQuote) {
                if (currentChar == '(') {
                    depth++;
                } else if (currentChar == ')' && depth > 0) {
                    depth--;
                } else if (currentChar == ',' && depth == 0) {
                    expressions.add(current.toString());
                    current.setLength(0);
                    continue;
                }
            }
            current.append(currentChar);
        }
        if (current.length() > 0) {
            expressions.add(current.toString());
        }
        return expressions;
    }

    private String extractLeadingArithmeticIdentifier(String expression) {
        if (expression == null || expression.trim().isEmpty()) {
            return null;
        }
        int operatorIndex = -1;
        boolean singleQuote = false;
        boolean doubleQuote = false;
        boolean backtickQuote = false;
        for (int i = 0; i < expression.length(); i++) {
            char currentChar = expression.charAt(i);
            if (currentChar == '\'' && !doubleQuote && !backtickQuote) {
                singleQuote = !singleQuote;
                continue;
            }
            if (currentChar == '"' && !singleQuote && !backtickQuote) {
                doubleQuote = !doubleQuote;
                continue;
            }
            if (currentChar == '`' && !singleQuote && !doubleQuote) {
                backtickQuote = !backtickQuote;
                continue;
            }
            if (singleQuote || doubleQuote || backtickQuote) {
                continue;
            }
            if (currentChar == '*' || currentChar == '/' || currentChar == '+' || currentChar == '-') {
                operatorIndex = i;
                break;
            }
        }
        if (operatorIndex < 0) {
            return null;
        }
        String prefix = expression.substring(0, operatorIndex);
        Matcher matcher = Pattern.compile("\\b([a-zA-Z_][\\w]*)\\b").matcher(prefix);
        String candidate = null;
        while (matcher.find()) {
            String token = matcher.group(1);
            int start = matcher.start(1);
            if (start > 0 && prefix.charAt(start - 1) == '.') {
                continue;
            }
            String normalizedToken = token.toLowerCase(Locale.ROOT);
            if (isSqlNoiseToken(normalizedToken)) {
                continue;
            }
            candidate = token;
        }
        return candidate;
    }

    private boolean isSqlNoiseToken(String token) {
        if (token == null || token.isEmpty()) {
            return true;
        }
        return "select".equals(token)
                || "distinct".equals(token)
                || "as".equals(token)
                || "over".equals(token)
                || "partition".equals(token)
                || "by".equals(token)
                || "from".equals(token)
                || "where".equals(token)
                || "and".equals(token)
                || "or".equals(token)
                || "not".equals(token)
                || "case".equals(token)
                || "when".equals(token)
                || "then".equals(token)
                || "else".equals(token)
                || "end".equals(token)
                || "null".equals(token)
                || "if".equals(token)
                || "in".equals(token)
                || "sum".equals(token)
                || "max".equals(token)
                || "min".equals(token)
                || "avg".equals(token)
                || "count".equals(token)
                || "nvl".equals(token)
                || "coalesce".equals(token)
                || "max_by".equals(token)
                || "date_format".equals(token)
                || "to_date".equals(token)
                || "last_day".equals(token)
                || "day".equals(token)
                || "yyyy".equals(token)
                || "mm".equals(token);
    }

    private String joinColumns(List<FieldSchema> columns) {
        List<String> rendered = new ArrayList<String>();
        for (FieldSchema column : columns) {
            if (column == null || column.getName() == null || column.getName().trim().isEmpty()) {
                continue;
            }
            String type = column.getType() == null || column.getType().trim().isEmpty()
                    ? "string"
                    : column.getType();
            rendered.add(quotedIdentifier(column.getName()) + " " + type);
        }
        if (rendered.isEmpty()) {
            rendered.add(quotedIdentifier("_placeholder_col") + " string");
        }
        return String.join(", ", rendered);
    }

    private String quotedIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private LogicalPlan asLogicalPlan(Object value) {
        if (value instanceof LogicalPlan) {
            return (LogicalPlan) value;
        }
        return null;
    }

    private ExecutionCaptureEvent buildEvent(Path scriptPath, int statementIndex, String statement, AnalysisBundle analysisBundle) {
        long captureTimeMs = System.currentTimeMillis();
        String taskId = sanitizeTaskId(scriptPath.getFileName().toString()) + "_stmt_" + statementIndex;
        String runId = sanitizeTaskId(scriptPath.getFileName().toString()) + "_import_run";
        String eventId = "offline:" + HashUtils.sha1(scriptPath + "#" + statementIndex + "#" + statement);

        return new ExecutionCaptureEvent(
                eventId,
                ExecutionStatus.SUCCESS,
                new LineageTaskContext(
                        taskId,
                        scriptPath.getFileName().toString(),
                        runId,
                        bizDateToken,
                        owner,
                        scriptPath.toString()
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
                        planText(analysisBundle.logicalPlan),
                        planText(analysisBundle.analyzedPlan),
                        planText(analysisBundle.optimizedPlan),
                        ""
                )
        );
    }

    private String planText(LogicalPlan logicalPlan) {
        if (logicalPlan == null) {
            return "";
        }
        try {
            return logicalPlan.toString();
        } catch (Exception ignored) {
            return "";
        }
    }

    private Map<String, String> defaultVariables() {
        LinkedHashMap<String, String> variables = new LinkedHashMap<>();
        LocalDate current = LocalDate.now();
        LocalDate previousDay = current.minusDays(1);
        variables.put("biz_date", bizDateToken);
        variables.put("outfilesuffix", previousDay.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        variables.put("datesuffix", bizDateToken);
        variables.put("today", bizDateToken);
        variables.put("end_date", bizDateToken);
        variables.put("sevendaysbeforesuffix", current.minusDays(7).format(DATE_TOKEN));
        variables.put("start_date", "20230101");
        return variables;
    }

    private List<String> renderLineageStatements(List<String> statements, Map<String, String> variables) {
        List<String> renderedStatements = new ArrayList<String>();
        if (statements == null) {
            return renderedStatements;
        }
        LinkedHashMap<String, String> scopedVariables = new LinkedHashMap<String, String>(variables);
        for (String rawStatement : statements) {
            String statement = rawStatement == null ? "" : rawStatement.trim();
            if (statement.isEmpty()) {
                continue;
            }
            if (isSetStatement(statement)) {
                registerSetVariable(scopedVariables, statement);
                continue;
            }
            String renderedStatement = replacePlaceholders(statement, scopedVariables);
            if (isLineageStatement(renderedStatement)) {
                renderedStatements.add(renderedStatement);
            }
        }
        return renderedStatements;
    }

    private List<String> collectDirectoryLineageStatements(List<Path> sqlFiles) throws IOException {
        List<String> statements = new ArrayList<String>();
        if (sqlFiles == null) {
            return statements;
        }
        for (Path sqlFile : sqlFiles) {
            String sqlText = new String(Files.readAllBytes(sqlFile.toAbsolutePath().normalize()), StandardCharsets.UTF_8);
            statements.addAll(renderLineageStatements(splitStatements(sqlText), defaultVariables()));
        }
        return statements;
    }

    private boolean isSetStatement(String statement) {
        return statement.toLowerCase(Locale.ROOT).startsWith("set ");
    }

    private void registerSetVariable(Map<String, String> variables, String statement) {
        String content = statement.trim().substring(4).trim();
        int index = content.indexOf('=');
        if (index <= 0) {
            return;
        }
        String key = content.substring(0, index).trim().toLowerCase(Locale.ROOT);
        String value = content.substring(index + 1).trim();
        if (value.endsWith(";")) {
            value = value.substring(0, value.length() - 1).trim();
        }
        value = stripQuotes(value);
        if (!key.isEmpty() && !value.isEmpty()) {
            variables.put(key, value.replace("-", ""));
        }
    }

    private String replacePlaceholders(String statement, Map<String, String> variables) {
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(statement);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String key = matcher.group(1).trim().toLowerCase(Locale.ROOT);
            String replacement = variables.get(key);
            if (replacement == null) {
                replacement = defaultPlaceholderValue(key);
            }
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String defaultPlaceholderValue(String key) {
        if (key.contains("date") || key.contains("suffix")) {
            return bizDateToken;
        }
        return "0";
    }

    private boolean isLineageStatement(String statement) {
        String normalized = statement.trim().toLowerCase(Locale.ROOT);
        String collapsed = normalized.replaceAll("\\s+", " ");
        if (collapsed.startsWith("add jar")
                || collapsed.startsWith("create temporary function")
                || collapsed.startsWith("create temp function")
                || collapsed.startsWith("drop table")
                || collapsed.startsWith("drop view")
                || collapsed.startsWith("msck")
                || collapsed.startsWith("analyze")) {
            return false;
        }
        if (collapsed.startsWith("insert ")) {
            return true;
        }
        if (collapsed.startsWith("create table") && collapsed.contains(" as ")) {
            return true;
        }
        if (collapsed.startsWith("with ")) {
            return collapsed.contains(" insert ") || collapsed.contains("create table");
        }
        return false;
    }

    private List<String> splitStatements(String sqlText) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean singleQuote = false;
        boolean doubleQuote = false;
        boolean backtickQuote = false;
        boolean lineComment = false;
        boolean blockComment = false;

        for (int i = 0; i < sqlText.length(); i++) {
            char currentChar = sqlText.charAt(i);
            char nextChar = i + 1 < sqlText.length() ? sqlText.charAt(i + 1) : '\0';

            if (lineComment) {
                if (currentChar == '\n') {
                    lineComment = false;
                    current.append(currentChar);
                }
                continue;
            }
            if (blockComment) {
                if (currentChar == '*' && nextChar == '/') {
                    blockComment = false;
                    i++;
                }
                continue;
            }
            if (!singleQuote && !doubleQuote && !backtickQuote) {
                if (currentChar == '-' && nextChar == '-') {
                    lineComment = true;
                    i++;
                    continue;
                }
                if (currentChar == '/' && nextChar == '*') {
                    blockComment = true;
                    i++;
                    continue;
                }
            }

            if (currentChar == '\'' && !doubleQuote && !backtickQuote) {
                singleQuote = !singleQuote;
            } else if (currentChar == '"' && !singleQuote && !backtickQuote) {
                doubleQuote = !doubleQuote;
            } else if (currentChar == '`' && !singleQuote && !doubleQuote) {
                backtickQuote = !backtickQuote;
            }

            if (currentChar == ';' && !singleQuote && !doubleQuote && !backtickQuote) {
                statements.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(currentChar);
        }

        if (current.length() > 0) {
            statements.add(current.toString());
        }
        return statements;
    }

    private String sanitizeTaskId(String fileName) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        StringBuilder builder = new StringBuilder(lower.length());
        for (int i = 0; i < lower.length(); i++) {
            char ch = lower.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                builder.append(ch);
            } else {
                builder.append('_');
            }
        }
        return builder.toString().replaceAll("_+", "_");
    }

    private String stripQuotes(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    private String buildBatchName(Path path) {
        String normalized = path.toAbsolutePath().normalize().toString();
        return normalized.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private Object invokeNoArg(Object target, String methodName) {
        if (target == null) {
            throw new IllegalStateException("Target is null for method " + methodName);
        }
        try {
            Method method = target.getClass().getMethod(methodName);
            method.setAccessible(true);
            return method.invoke(target);
        } catch (Exception ignored) {
        }
        try {
            Method method = target.getClass().getDeclaredMethod(methodName);
            method.setAccessible(true);
            return method.invoke(target);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to invoke method `" + methodName + "` on " + target.getClass().getName(), error);
        }
    }

    private Object invokeOneArg(Object target, String methodName, Class<?> argumentType, Object argumentValue) {
        if (target == null) {
            throw new IllegalStateException("Target is null for method " + methodName);
        }
        try {
            Method method = target.getClass().getMethod(methodName, argumentType);
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        } catch (Exception ignored) {
        }
        try {
            Method method = target.getClass().getDeclaredMethod(methodName, argumentType);
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to invoke method `" + methodName + "` on " + target.getClass().getName(), error);
        }
    }

    private Object invokeCompatibleOneArg(Object target, String methodName, Object argumentValue) {
        if (target == null) {
            throw new IllegalStateException("Target is null for method " + methodName);
        }
        Method[] methods = target.getClass().getMethods();
        Object result = invokeCompatibleOneArg(methods, target, methodName, argumentValue);
        if (result != null) {
            return result;
        }
        methods = target.getClass().getDeclaredMethods();
        result = invokeCompatibleOneArg(methods, target, methodName, argumentValue);
        if (result != null) {
            return result;
        }
        throw new IllegalStateException("Failed to invoke compatible method `" + methodName + "` on " + target.getClass().getName());
    }

    private List<LogicalPlan> extractCteRelationPlans(LogicalPlan plan) {
        Object relations;
        try {
            relations = invokeNoArg(plan, "cteRelations");
        } catch (Exception ignored) {
            return Collections.emptyList();
        }
        if (!(relations instanceof Seq)) {
            return Collections.emptyList();
        }
        List<LogicalPlan> plans = new ArrayList<LogicalPlan>();
        for (Object value : ScalaInterop.toJavaList((Seq<?>) relations)) {
            if (!(value instanceof Tuple2)) {
                continue;
            }
            Object relationPlan = ((Tuple2<?, ?>) value)._2();
            if (relationPlan instanceof LogicalPlan) {
                plans.add((LogicalPlan) relationPlan);
            }
        }
        return plans;
    }

    private LogicalPlan extractTransparentChildPlan(Object target) {
        for (String methodName : new String[]{"child", "plan", "queryPlan", "inputPlan"}) {
            try {
                Object value = invokeNoArg(target, methodName);
                if (value instanceof LogicalPlan) {
                    return (LogicalPlan) value;
                }
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    private Object invokeCompatibleOneArg(Method[] methods, Object target, String methodName, Object argumentValue) {
        for (Method method : methods) {
            if (!methodName.equals(method.getName()) || method.getParameterTypes().length != 1) {
                continue;
            }
            Class<?> parameterType = method.getParameterTypes()[0];
            if (argumentValue != null && !parameterType.isInstance(argumentValue) && !parameterType.isAssignableFrom(argumentValue.getClass())) {
                continue;
            }
            try {
                method.setAccessible(true);
                return method.invoke(target, argumentValue);
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    private static String read(String propertyKey, String envKey) {
        String property = System.getProperty(propertyKey);
        if (property != null && !property.trim().isEmpty()) {
            return property.trim();
        }
        String env = System.getenv(envKey);
        if (env != null && !env.trim().isEmpty()) {
            return env.trim();
        }
        return null;
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

    private static void applySparkOverrides(SparkSession.Builder builder) {
        for (Map.Entry<String, String> entry : prefixedProperties(SPARK_CONF_PREFIX).entrySet()) {
            builder.config(entry.getKey(), entry.getValue());
        }
    }

    private static Map<String, String> prefixedProperties(String prefix) {
        LinkedHashMap<String, String> values = new LinkedHashMap<String, String>();
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (!key.startsWith(prefix)) {
                continue;
            }
            String value = entry.getValue() == null ? null : String.valueOf(entry.getValue()).trim();
            if (value == null || value.isEmpty()) {
                continue;
            }
            values.put(key.substring(prefix.length()), value);
        }
        return values;
    }

    private void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try {
            Files.walk(root)
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

    private String formatError(Throwable error) {
        List<String> parts = new ArrayList<>();
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && !message.trim().isEmpty()) {
                parts.add(message.trim());
            } else {
                parts.add(current.getClass().getSimpleName());
            }
            current = current.getCause();
        }
        return String.join(" | caused by: ", parts);
    }

    private List<Path> listSqlFiles(Path directoryPath) throws IOException {
        try (Stream<Path> stream = Files.walk(directoryPath)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".sql"))
                    .sorted(Comparator.comparing(Path::toString))
                    .collect(Collectors.toList());
        }
    }

    private static final class AnalysisBundle {
        private final LogicalPlan logicalPlan;
        private final LogicalPlan analyzedPlan;
        private final LogicalPlan optimizedPlan;

        private AnalysisBundle(LogicalPlan logicalPlan, LogicalPlan analyzedPlan, LogicalPlan optimizedPlan) {
            this.logicalPlan = logicalPlan;
            this.analyzedPlan = analyzedPlan;
            this.optimizedPlan = optimizedPlan;
        }
    }

    private static final class PreparedImport {
        private final List<ExecutionCaptureEvent> events;
        private final List<NormalizedLineageResult> results;

        private PreparedImport(
                List<ExecutionCaptureEvent> events,
                List<NormalizedLineageResult> results
        ) {
            this.events = events;
            this.results = results;
        }
    }

    private static final class MetadataSyncResult {
        private final List<String> syncedTables = new ArrayList<String>();
        private final List<String> failedTables = new ArrayList<String>();
        private final List<String> failures = new ArrayList<String>();
    }

    public static final class MetadataSyncSummary {
        private final Path targetPath;
        private final int totalScripts;
        private final int totalReferencedTables;
        private long startedAtEpochMs;
        private long finishedAtEpochMs;
        private int cachedBefore;
        private int cachedAfter;
        private Path referencedTablesPath;
        private Path failuresPath;
        private final List<String> failures = new ArrayList<String>();

        private MetadataSyncSummary(Path targetPath, int totalScripts, int totalReferencedTables) {
            this.targetPath = targetPath;
            this.totalScripts = totalScripts;
            this.totalReferencedTables = totalReferencedTables;
        }

        public Path getTargetPath() {
            return targetPath;
        }

        public int getTotalScripts() {
            return totalScripts;
        }

        public int getTotalReferencedTables() {
            return totalReferencedTables;
        }

        public int getCachedBefore() {
            return cachedBefore;
        }

        public int getCachedAfter() {
            return cachedAfter;
        }

        public int getNewlyCached() {
            return Math.max(0, cachedAfter - cachedBefore);
        }

        public Path getReferencedTablesPath() {
            return referencedTablesPath;
        }

        public Path getFailuresPath() {
            return failuresPath;
        }

        public List<String> getFailures() {
            return new ArrayList<String>(failures);
        }

        public long getDurationMs() {
            if (startedAtEpochMs <= 0 || finishedAtEpochMs <= 0) {
                return -1L;
            }
            return finishedAtEpochMs - startedAtEpochMs;
        }

        private void markStarted() {
            startedAtEpochMs = System.currentTimeMillis();
        }

        private void markFinished() {
            finishedAtEpochMs = System.currentTimeMillis();
        }
    }
}
