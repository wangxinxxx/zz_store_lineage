package com.zhuanzhuan.lineage.storage.nebula;

import com.zhuanzhuan.lineage.common.HashUtils;
import com.zhuanzhuan.lineage.model.ColumnInstanceNode;
import com.zhuanzhuan.lineage.model.ColumnNode;
import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExpressionNode;
import com.zhuanzhuan.lineage.model.LineageGraphEdge;
import com.zhuanzhuan.lineage.model.LiteralNode;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.OperatorInstanceNode;
import com.zhuanzhuan.lineage.model.PredicateNode;
import com.zhuanzhuan.lineage.model.RelationInstanceNode;
import com.zhuanzhuan.lineage.model.ScopeNode;
import com.zhuanzhuan.lineage.model.TableLineageEdge;
import com.zhuanzhuan.lineage.model.TableRef;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public final class NebulaImporterBundleWriter {
    public static final String BUNDLE_DIR_PROPERTY = "zz.lineage.import.bundle.dir";
    public static final String BUNDLE_DIR_ENV = "ZZ_LINEAGE_IMPORT_BUNDLE_DIR";
    public static final String IMPORTER_BATCH_PROPERTY = "zz.lineage.importer.batch";
    public static final String IMPORTER_BATCH_ENV = "ZZ_LINEAGE_IMPORTER_BATCH";
    public static final String IMPORTER_READER_CONCURRENCY_PROPERTY = "zz.lineage.importer.readerConcurrency";
    public static final String IMPORTER_READER_CONCURRENCY_ENV = "ZZ_LINEAGE_IMPORTER_READER_CONCURRENCY";
    public static final String IMPORTER_CONCURRENCY_PROPERTY = "zz.lineage.importer.importerConcurrency";
    public static final String IMPORTER_CONCURRENCY_ENV = "ZZ_LINEAGE_IMPORTER_CONCURRENCY";
    public static final String IMPORTER_CLIENT_CONCURRENCY_PROPERTY = "zz.lineage.importer.clientConcurrencyPerAddress";
    public static final String IMPORTER_CLIENT_CONCURRENCY_ENV = "ZZ_LINEAGE_IMPORTER_CLIENT_CONCURRENCY";
    public static final String IMPORTER_RETRY_PROPERTY = "zz.lineage.importer.retry";
    public static final String IMPORTER_RETRY_ENV = "ZZ_LINEAGE_IMPORTER_RETRY";
    public static final String IMPORTER_STATS_INTERVAL_PROPERTY = "zz.lineage.importer.statsInterval";
    public static final String IMPORTER_STATS_INTERVAL_ENV = "ZZ_LINEAGE_IMPORTER_STATS_INTERVAL";

    public static final String VERTICES_DIR_NAME = "vertices";
    public static final String EDGES_DIR_NAME = "edges";
    public static final String LOGS_DIR_NAME = "logs";
    public static final String IMPORTER_CONFIG_FILE_NAME = "nebula-importer.yaml";
    public static final String SCHEMA_FILE_NAME = "schema.ngql";
    public static final String MANIFEST_FILE_NAME = "manifest.properties";
    public static final String SCRIPTS_FILE_NAME = "scripts.tsv";
    public static final String RUN_PS1_FILE_NAME = "run-import.ps1";
    public static final String RUN_SH_FILE_NAME = "run-import.sh";
    public static final String NULL_TOKEN = "__ZZ_LINEAGE_NULL__";

    private static final Map<String, VertexSchema> VERTEX_SCHEMAS = new LinkedHashMap<String, VertexSchema>();
    private static final Map<String, EdgeSchema> EDGE_SCHEMAS = new LinkedHashMap<String, EdgeSchema>();

    private static final String[][] VERTEX_DEFINITIONS = new String[][]{
            {"table_node", "normalized_name:string", "catalog_name:string", "database_name:string", "table_name:string", "source_type:string"},
            {"column_node", "column_name:string", "owner_id:string", "owner_type:string", "data_type:string", "qualifier:string"},
            {"expression_node", "expression_type:string", "expression_sql:string", "normalized_expression:string",
                    "expression_category:string", "display_sql:string", "is_black_box:bool", "query_block_id:string", "plan_node_id:string"},
            {"literal_node", "literal_type:string", "literal_value:string", "normalized_value:string"},
            {"column_instance_node", "column_id:string", "column_name:string", "scope_id:string", "relation_instance_id:string", "instance_type:string", "data_type:string", "ordinal:int",
                    "instance_role:string", "is_output:bool", "query_block_id:string", "plan_node_id:string"},
            {"relation_instance_node", "instance_name:string", "scope_id:string", "source_table_id:string", "source_type:string", "alias_name:string", "plan_node_name:string",
                    "query_block_id:string", "plan_node_id:string", "join_type:string", "null_supply_side:string", "is_subquery_source:bool"},
            {"predicate_node", "predicate_type:string", "predicate_sql:string", "normalized_predicate:string", "scope_id:string", "plan_node_name:string",
                    "predicate_role:string", "display_sql:string", "query_block_id:string", "plan_node_id:string"}
    };

    private static final String[][] EDGE_DEFINITIONS = new String[][]{
            {"table_has_column"},
            {"column_has_instance", "event_id:string", "capture_time:timestamp", "role:string"},
            {"latest_relation_instance_joins_relation_instance", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_literal_flows_to_expression", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_column_instance_flows_to_expression", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_expression_flows_to_expression", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_expression_flows_to_column_instance", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_column_instance_flows_to_column_instance", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_column_instance_flows_to_predicate", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_relation_instance_flows_to_predicate", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_relation_instance_flows_to_column_instance", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_predicate_flows_to_column_instance", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"},
            {"latest_literal_flows_to_predicate", "last_event_id:string", "last_task_id:string", "last_run_id:string", "last_seen_time:timestamp", "role:string"}
    };

    static {
        registerSchemas(VERTEX_DEFINITIONS, true);
        registerSchemas(EDGE_DEFINITIONS, false);
    }

    private final Path rootDir;
    private final NebulaGraphConfig graphConfig;
    private final ImporterOptions importerOptions;

    public NebulaImporterBundleWriter(Path rootDir, NebulaGraphConfig graphConfig, ImporterOptions importerOptions) {
        this.rootDir = rootDir;
        this.graphConfig = graphConfig;
        this.importerOptions = importerOptions == null ? ImporterOptions.defaults() : importerOptions;
    }

    public static Path resolveBundleDirFromSystem() throws IOException {
        String configured = read(BUNDLE_DIR_PROPERTY, BUNDLE_DIR_ENV);
        Path path;
        if (configured != null && !configured.trim().isEmpty()) {
            path = Paths.get(configured).toAbsolutePath().normalize();
        } else {
            path = Paths.get(System.getProperty("user.dir"), ".nebula-importer-bundles").toAbsolutePath().normalize();
        }
        Files.createDirectories(path);
        return path;
    }

    public BundleSession openSession(String batchName) throws IOException {
        Files.createDirectories(rootDir);
        String sessionName = System.currentTimeMillis() + "-" + sanitize(batchName);
        Path bundleDir = rootDir.resolve(sessionName).toAbsolutePath().normalize();
        Files.createDirectories(bundleDir.resolve(VERTICES_DIR_NAME));
        Files.createDirectories(bundleDir.resolve(EDGES_DIR_NAME));
        Files.createDirectories(bundleDir.resolve(LOGS_DIR_NAME));
        return new BundleSession(bundleDir);
    }

    public static List<CsvSchema> vertexSchemas() {
        return schemaCopies(VERTEX_SCHEMAS.values(), true);
    }

    public static List<CsvSchema> edgeSchemas() {
        return schemaCopies(EDGE_SCHEMAS.values(), false);
    }

    public static CsvSchema vertexSchema(String name) {
        return copySchema(vertex(name), true);
    }

    public static CsvSchema edgeSchema(String name) {
        return copySchema(edge(name), false);
    }

    public static final class ImporterOptions {
        private final int batch;
        private final int readerConcurrency;
        private final int importerConcurrency;
        private final int clientConcurrencyPerAddress;
        private final int retry;
        private final String statsInterval;

        public ImporterOptions(
                int batch,
                int readerConcurrency,
                int importerConcurrency,
                int clientConcurrencyPerAddress,
                int retry,
                String statsInterval
        ) {
            this.batch = Math.max(1, batch);
            this.readerConcurrency = Math.max(1, readerConcurrency);
            this.importerConcurrency = Math.max(1, importerConcurrency);
            this.clientConcurrencyPerAddress = Math.max(1, clientConcurrencyPerAddress);
            this.retry = Math.max(0, retry);
            this.statsInterval = statsInterval == null || statsInterval.trim().isEmpty() ? "10s" : statsInterval.trim();
        }

        public static ImporterOptions defaults() {
            return new ImporterOptions(1024, 8, 64, 10, 3, "10s");
        }

        public static ImporterOptions fromSystem() {
            return new ImporterOptions(
                    parseInt(read(IMPORTER_BATCH_PROPERTY, IMPORTER_BATCH_ENV), 1024),
                    parseInt(read(IMPORTER_READER_CONCURRENCY_PROPERTY, IMPORTER_READER_CONCURRENCY_ENV), 8),
                    parseInt(read(IMPORTER_CONCURRENCY_PROPERTY, IMPORTER_CONCURRENCY_ENV), 64),
                    parseInt(read(IMPORTER_CLIENT_CONCURRENCY_PROPERTY, IMPORTER_CLIENT_CONCURRENCY_ENV), 10),
                    parseInt(read(IMPORTER_RETRY_PROPERTY, IMPORTER_RETRY_ENV), 3),
                    read(IMPORTER_STATS_INTERVAL_PROPERTY, IMPORTER_STATS_INTERVAL_ENV, "10s")
            );
        }
    }

    public static final class BundleSummary {
        private final Path bundleDir;
        private final Path importerConfigPath;
        private final Path schemaPath;
        private final Path runPs1Path;
        private final Path runShPath;
        private final int totalScripts;
        private final int totalEvents;
        private final int totalResults;
        private final int vertexFiles;
        private final int edgeFiles;
        private final int totalVertices;
        private final int totalEdges;
        private final long durationMs;

        private BundleSummary(
                Path bundleDir,
                Path importerConfigPath,
                Path schemaPath,
                Path runPs1Path,
                Path runShPath,
                int totalScripts,
                int totalEvents,
                int totalResults,
                int vertexFiles,
                int edgeFiles,
                int totalVertices,
                int totalEdges,
                long durationMs
        ) {
            this.bundleDir = bundleDir;
            this.importerConfigPath = importerConfigPath;
            this.schemaPath = schemaPath;
            this.runPs1Path = runPs1Path;
            this.runShPath = runShPath;
            this.totalScripts = totalScripts;
            this.totalEvents = totalEvents;
            this.totalResults = totalResults;
            this.vertexFiles = vertexFiles;
            this.edgeFiles = edgeFiles;
            this.totalVertices = totalVertices;
            this.totalEdges = totalEdges;
            this.durationMs = durationMs;
        }

        public Path getBundleDir() {
            return bundleDir;
        }

        public Path getImporterConfigPath() {
            return importerConfigPath;
        }

        public Path getSchemaPath() {
            return schemaPath;
        }

        public Path getRunPs1Path() {
            return runPs1Path;
        }

        public Path getRunShPath() {
            return runShPath;
        }

        public int getTotalScripts() {
            return totalScripts;
        }

        public int getTotalEvents() {
            return totalEvents;
        }

        public int getTotalResults() {
            return totalResults;
        }

        public int getVertexFiles() {
            return vertexFiles;
        }

        public int getEdgeFiles() {
            return edgeFiles;
        }

        public int getTotalVertices() {
            return totalVertices;
        }

        public int getTotalEdges() {
            return totalEdges;
        }

        public long getDurationMs() {
            return durationMs;
        }
    }

    public static final class CsvSchema implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final boolean vertex;
        private final List<PropertySpec> properties;

        private CsvSchema(String name, boolean vertex, List<PropertySpec> properties) {
            this.name = name;
            this.vertex = vertex;
            this.properties = properties;
        }

        public String getName() {
            return name;
        }

        public boolean isVertex() {
            return vertex;
        }

        public List<PropertySpec> getProperties() {
            return new ArrayList<PropertySpec>(properties);
        }
    }

    public static final class PropertySpec implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String name;
        private final String type;

        private PropertySpec(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }
    }

    public final class BundleSession implements AutoCloseable {
        private final Path bundleDir;
        private final Path verticesDir;
        private final Path edgesDir;
        private final long startedAtMs = System.currentTimeMillis();
        private final Map<String, RecordStore> vertexStores = new LinkedHashMap<String, RecordStore>();
        private final Map<String, RecordStore> edgeStores = new LinkedHashMap<String, RecordStore>();
        private boolean closed;
        private int totalScripts;
        private int totalEvents;
        private int totalResults;
        private int totalVertices;
        private int totalEdges;
        private BundleSummary finalSummary;

        private BundleSession(Path bundleDir) {
            this.bundleDir = bundleDir;
            this.verticesDir = bundleDir.resolve(VERTICES_DIR_NAME);
            this.edgesDir = bundleDir.resolve(EDGES_DIR_NAME);
        }

        public void appendScript(Path scriptPath, List<ExecutionCaptureEvent> events, List<NormalizedLineageResult> results) {
            ensureOpen();
            totalScripts++;
            totalEvents += sizeOf(events);
            totalResults += sizeOf(results);

            if (events != null) {
                for (ExecutionCaptureEvent event : events) {
                    if (event != null) {
                        appendEvent(event);
                    }
                }
            }
            if (results != null) {
                for (NormalizedLineageResult result : results) {
                    if (result != null) {
                        appendResult(result);
                    }
                }
            }
        }

        public BundleSummary finish() throws IOException {
            if (finalSummary != null) {
                return finalSummary;
            }
            writeCsvFiles();
            Path schemaPath = writeSchemaFile();
            writeManifest();
            closed = true;
            finalSummary = new BundleSummary(
                    bundleDir,
                    bundleDir.resolve(IMPORTER_CONFIG_FILE_NAME),
                    schemaPath,
                    bundleDir.resolve(RUN_PS1_FILE_NAME),
                    bundleDir.resolve(RUN_SH_FILE_NAME),
                    totalScripts,
                    totalEvents,
                    totalResults,
                    nonEmptyStores(vertexStores),
                    nonEmptyStores(edgeStores),
                    totalVertices,
                    totalEdges,
                    System.currentTimeMillis() - startedAtMs
            );
            return finalSummary;
        }

        @Override
        public void close() throws IOException {
            finish();
        }

        private void appendEvent(ExecutionCaptureEvent event) {
            // Phase 1 convergence: execution/task/run metadata is no longer written into the main caliber graph.
        }

        private void appendResult(NormalizedLineageResult result) {
            Set<String> knownTableOwners = new LinkedHashSet<String>();
            for (TableRef tableRef : result.getInputTables()) {
                String inputVid = tableVid(tableRef);
                knownTableOwners.add(inputVid);
                putVertex(vertex("table_node"), inputVid,
                        tableRef.normalizedName(),
                        tableRef.getCatalog(),
                        tableRef.getDatabase(),
                        tableRef.getName(),
                        tableRef.getSourceType()
                );
            }

            for (TableRef tableRef : result.getOutputTables()) {
                String outputVid = tableVid(tableRef);
                knownTableOwners.add(outputVid);
                putVertex(vertex("table_node"), outputVid,
                        tableRef.normalizedName(),
                        tableRef.getCatalog(),
                        tableRef.getDatabase(),
                        tableRef.getName(),
                        tableRef.getSourceType()
                );
            }

            for (ColumnNode columnNode : result.getColumnNodes()) {
                putVertex(vertex("column_node"), columnNode.getNodeId(),
                        columnNode.getName(),
                        columnNode.getOwnerId(),
                        columnNode.getOwnerType(),
                        columnNode.getDataType(),
                        join(columnNode.getQualifier(), ".")
                );
                String ownerVid = tableOwnerVid(columnNode);
                if (ownerVid != null && knownTableOwners.contains(ownerVid)) {
                    putEdge(edge("table_has_column"), ownerVid, columnNode.getNodeId(), 0L);
                }
            }

            for (ExpressionNode expressionNode : result.getExpressionNodes()) {
                putVertex(vertex("expression_node"), expressionNode.getNodeId(),
                        expressionNode.getExpressionType(),
                        expressionNode.getExpressionSql(),
                        expressionNode.getNormalizedExpression(),
                        expressionNode.getExpressionCategory(),
                        expressionNode.getDisplaySql(),
                        bool(expressionNode.isBlackBox()),
                        expressionNode.getQueryBlockId(),
                        expressionNode.getPlanNodeId()
                );
            }

            for (LiteralNode literalNode : result.getLiteralNodes()) {
                putVertex(vertex("literal_node"), literalNode.getNodeId(),
                        literalNode.getLiteralType(),
                        literalNode.getLiteralValue(),
                        literalNode.getNormalizedValue()
                );
            }

            for (ColumnInstanceNode columnInstanceNode : result.getColumnInstanceNodes()) {
                putVertex(vertex("column_instance_node"), columnInstanceNode.getNodeId(),
                        columnInstanceNode.getColumnId(),
                        columnInstanceNode.getColumnName(),
                        columnInstanceNode.getScopeId(),
                        columnInstanceNode.getRelationInstanceId(),
                        columnInstanceNode.getInstanceType(),
                        columnInstanceNode.getDataType(),
                        integer(columnInstanceNode.getOrdinal()),
                        columnInstanceNode.getInstanceRole(),
                        bool(columnInstanceNode.isOutput()),
                        columnInstanceNode.getQueryBlockId(),
                        columnInstanceNode.getPlanNodeId()
                );
            }

            for (RelationInstanceNode relationInstanceNode : result.getRelationInstanceNodes()) {
                putVertex(vertex("relation_instance_node"), relationInstanceNode.getNodeId(),
                        relationInstanceNode.getInstanceName(),
                        relationInstanceNode.getScopeId(),
                        relationInstanceNode.getSourceTableId(),
                        relationInstanceNode.getSourceType(),
                        relationInstanceNode.getAliasName(),
                        relationInstanceNode.getPlanNodeName(),
                        relationInstanceNode.getQueryBlockId(),
                        relationInstanceNode.getPlanNodeId(),
                        relationInstanceNode.getJoinType(),
                        relationInstanceNode.getNullSupplySide(),
                        bool(relationInstanceNode.isSubquerySource())
                );
            }

            for (PredicateNode predicateNode : result.getPredicateNodes()) {
                putVertex(vertex("predicate_node"), predicateNode.getNodeId(),
                        predicateNode.getPredicateType(),
                        predicateNode.getPredicateSql(),
                        predicateNode.getNormalizedPredicate(),
                        predicateNode.getScopeId(),
                        predicateNode.getPlanNodeName(),
                        predicateNode.getPredicateRole(),
                        predicateNode.getDisplaySql(),
                        predicateNode.getQueryBlockId(),
                        predicateNode.getPlanNodeId()
                );
            }
            for (LineageGraphEdge graphEdge : result.getGraphEdges()) {
                appendGraphEdge(result, graphEdge);
            }
        }

        private void appendGraphEdge(NormalizedLineageResult result, LineageGraphEdge graphEdge) {
            String eventId = graphEdge.getEventId() == null ? result.getEventId() : graphEdge.getEventId();
            String taskId = inferTaskId(result);
            String runId = inferRunId(result);
            String captureTime = timestamp(result.getCaptureTimeEpochMs());
            String role = graphEdge.getRole();
            String rankRole = nullableRankToken(role);

            String edgeType = graphEdge.getEdgeType();
            if ("COLUMN_TO_COLUMN_INSTANCE".equals(edgeType)) {
                putEdge(edge("column_has_instance"),
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        edgeRank("column_has_instance", graphEdge.getSourceNodeId(), graphEdge.getTargetNodeId(), rankRole),
                        eventId,
                        captureTime,
                        role
                );
                return;
            }
            if ("RELATION_INSTANCE_TO_RELATION_INSTANCE".equals(edgeType)) {
                putEdge(edge("latest_relation_instance_joins_relation_instance"),
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        latestSemanticRank("latest_relation_instance_joins_relation_instance", role),
                        eventId,
                        taskId,
                        runId,
                        captureTime,
                        role
                );
                return;
            }
            appendCurrentFlowEdge(edgeType, graphEdge, eventId, taskId, runId, captureTime, role);
        }

        private void appendCurrentFlowEdge(
                String edgeType,
                LineageGraphEdge graphEdge,
                String eventId,
                String taskId,
                String runId,
                String captureTime,
                String role
        ) {
            if ("COLUMN_INSTANCE_TO_PREDICATE".equals(edgeType)) {
                putLatestFlowEdge("latest_column_instance_flows_to_predicate", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("RELATION_INSTANCE_TO_PREDICATE".equals(edgeType)) {
                putLatestFlowEdge("latest_relation_instance_flows_to_predicate", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("RELATION_INSTANCE_TO_DERIVED_COLUMN_INSTANCE".equals(edgeType)) {
                putLatestFlowEdge("latest_relation_instance_flows_to_column_instance", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("PREDICATE_TO_DERIVED_COLUMN_INSTANCE".equals(edgeType)) {
                putLatestFlowEdge("latest_predicate_flows_to_column_instance", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("EXPRESSION_TO_COLUMN_INSTANCE".equals(edgeType)) {
                putLatestFlowEdge("latest_expression_flows_to_column_instance", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("COLUMN_INSTANCE_TO_EXPRESSION".equals(edgeType)) {
                putLatestFlowEdge("latest_column_instance_flows_to_expression", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("EXPRESSION_TO_EXPRESSION".equals(edgeType)) {
                putLatestFlowEdge("latest_expression_flows_to_expression", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("COLUMN_INSTANCE_TO_COLUMN_INSTANCE".equals(edgeType)) {
                putLatestFlowEdge("latest_column_instance_flows_to_column_instance", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("LITERAL_TO_EXPRESSION".equals(edgeType)) {
                putLatestFlowEdge("latest_literal_flows_to_expression", graphEdge, eventId, taskId, runId, captureTime, role);
                return;
            }
            if ("LITERAL_TO_PREDICATE".equals(edgeType)) {
                putLatestFlowEdge("latest_literal_flows_to_predicate", graphEdge, eventId, taskId, runId, captureTime, role);
            }
        }

        private void putLatestFlowEdge(
                String edgeName,
                LineageGraphEdge graphEdge,
                String eventId,
                String taskId,
                String runId,
                String captureTime,
                String role
        ) {
            putEdge(edge(edgeName),
                    graphEdge.getSourceNodeId(),
                    graphEdge.getTargetNodeId(),
                    latestSemanticRank(edgeName, role),
                    eventId,
                    taskId,
                    runId,
                    captureTime,
                    role
            );
        }

        private RecordStore vertexStore(VertexSchema schema) {
            RecordStore store = vertexStores.get(schema.name);
            if (store == null) {
                store = new RecordStore(schema);
                vertexStores.put(schema.name, store);
            }
            return store;
        }

        private RecordStore edgeStore(EdgeSchema schema) {
            RecordStore store = edgeStores.get(schema.name);
            if (store == null) {
                store = new RecordStore(schema);
                edgeStores.put(schema.name, store);
            }
            return store;
        }

        private void putVertex(VertexSchema schema, String vid, String... propertyValues) {
            RecordStore store = vertexStore(schema);
            if (store.rows.put(vid, schema.buildVertexRow(vid, propertyValues)) == null) {
                totalVertices++;
            }
        }

        private void putEdge(EdgeSchema schema, String srcVid, String dstVid, long rank, String... propertyValues) {
            RecordStore store = edgeStore(schema);
            String key = srcVid + '\u0001' + dstVid + '\u0001' + rank;
            if (store.rows.put(key, schema.buildEdgeRow(srcVid, dstVid, rank, propertyValues)) == null) {
                totalEdges++;
            }
        }

        private void writeCsvFiles() throws IOException {
            for (RecordStore store : vertexStores.values()) {
                if (!store.rows.isEmpty()) {
                    writeCsv(store, verticesDir.resolve(store.schema.name + ".csv"));
                }
            }
            for (RecordStore store : edgeStores.values()) {
                if (!store.rows.isEmpty()) {
                    writeCsv(store, edgesDir.resolve(store.schema.name + ".csv"));
                }
            }
        }

        private void writeCsv(RecordStore store, Path filePath) throws IOException {
            try (BufferedWriter writer = Files.newBufferedWriter(
                    filePath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            )) {
                for (String[] row : store.rows.values()) {
                    writer.write(toCsvRow(row));
                    writer.newLine();
                }
            }
        }

        private void writeManifest() throws IOException {
            Properties properties = new Properties();
            properties.setProperty("total_scripts", String.valueOf(totalScripts));
            properties.setProperty("total_events", String.valueOf(totalEvents));
            properties.setProperty("total_results", String.valueOf(totalResults));
            properties.setProperty("vertex_files", String.valueOf(nonEmptyStores(vertexStores)));
            properties.setProperty("edge_files", String.valueOf(nonEmptyStores(edgeStores)));
            properties.setProperty("total_vertices", String.valueOf(totalVertices));
            properties.setProperty("total_edges", String.valueOf(totalEdges));
            try (OutputStream outputStream = Files.newOutputStream(
                    bundleDir.resolve(MANIFEST_FILE_NAME),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            )) {
                properties.store(outputStream, "lineage data bundle manifest");
            }
        }

        private Path writeSchemaFile() throws IOException {
            Path schemaPath = bundleDir.resolve(SCHEMA_FILE_NAME);
            Files.write(
                    schemaPath,
                    schemaStatements(graphConfig.getSpace()),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
            return schemaPath;
        }

        private void ensureOpen() {
            if (closed) {
                throw new IllegalStateException("Bundle session is already closed: " + bundleDir);
            }
        }
    }

    private static final class RecordStore {
        private final Schema schema;
        private final LinkedHashMap<String, String[]> rows = new LinkedHashMap<String, String[]>();

        private RecordStore(Schema schema) {
            this.schema = schema;
        }
    }

    private abstract static class Schema {
        protected final String name;
        protected final List<Property> properties;

        private Schema(String name, List<Property> properties) {
            this.name = name;
            this.properties = properties;
        }

        protected String renderCreateStatement(String prefix) {
            StringBuilder builder = new StringBuilder(prefix).append(" `").append(name).append("`(");
            for (int i = 0; i < properties.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                Property property = properties.get(i);
                builder.append("`").append(property.name).append("` ").append(property.type);
            }
            builder.append(");");
            return builder.toString();
        }
    }

    private static final class VertexSchema extends Schema {
        private VertexSchema(String name, List<Property> properties) {
            super(name, properties);
        }

        private String[] buildVertexRow(String vid, String... propertyValues) {
            if (propertyValues.length != properties.size()) {
                throw new IllegalArgumentException("Unexpected property count for tag `" + name + "`.");
            }
            String[] row = new String[properties.size() + 1];
            row[0] = vid;
            System.arraycopy(propertyValues, 0, row, 1, propertyValues.length);
            return row;
        }
    }

    private static final class EdgeSchema extends Schema {
        private EdgeSchema(String name, List<Property> properties) {
            super(name, properties);
        }

        private String[] buildEdgeRow(String srcVid, String dstVid, long rank, String... propertyValues) {
            if (propertyValues.length != properties.size()) {
                throw new IllegalArgumentException("Unexpected property count for edge `" + name + "`.");
            }
            String[] row = new String[properties.size() + 3];
            row[0] = srcVid;
            row[1] = dstVid;
            row[2] = String.valueOf(rank);
            System.arraycopy(propertyValues, 0, row, 3, propertyValues.length);
            return row;
        }
    }

    private static final class Property {
        private final String name;
        private final String type;

        private Property(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }

    private static void registerSchemas(String[][] definitions, boolean vertex) {
        for (String[] definition : definitions) {
            List<Property> properties = new ArrayList<Property>();
            for (int i = 1; i < definition.length; i++) {
                String[] parts = definition[i].split(":", 2);
                properties.add(new Property(parts[0], parts[1]));
            }
            if (vertex) {
                VERTEX_SCHEMAS.put(definition[0], new VertexSchema(definition[0], properties));
            } else {
                EDGE_SCHEMAS.put(definition[0], new EdgeSchema(definition[0], properties));
            }
        }
    }

    private static List<CsvSchema> schemaCopies(Collection<? extends Schema> schemas, boolean vertex) {
        List<CsvSchema> copies = new ArrayList<CsvSchema>();
        for (Schema schema : schemas) {
            copies.add(copySchema(schema, vertex));
        }
        return copies;
    }

    private static CsvSchema copySchema(Schema schema, boolean vertex) {
        List<PropertySpec> properties = new ArrayList<PropertySpec>();
        for (Property property : schema.properties) {
            properties.add(new PropertySpec(property.name, property.type));
        }
        return new CsvSchema(schema.name, vertex, properties);
    }

    private static VertexSchema vertex(String name) {
        VertexSchema schema = VERTEX_SCHEMAS.get(name);
        if (schema == null) {
            throw new IllegalArgumentException("Unknown vertex schema: " + name);
        }
        return schema;
    }

    private static EdgeSchema edge(String name) {
        EdgeSchema schema = EDGE_SCHEMAS.get(name);
        if (schema == null) {
            throw new IllegalArgumentException("Unknown edge schema: " + name);
        }
        return schema;
    }

    public static List<String> schemaStatements(String space) {
        List<String> statements = new ArrayList<String>();
        statements.add(createSpaceStatement(space));
        statements.add("USE `" + space + "`;");
        for (VertexSchema schema : VERTEX_SCHEMAS.values()) {
            statements.add(schema.renderCreateStatement("CREATE TAG IF NOT EXISTS"));
        }
        for (EdgeSchema schema : EDGE_SCHEMAS.values()) {
            statements.add(schema.renderCreateStatement("CREATE EDGE IF NOT EXISTS"));
        }
        return statements;
    }

    private static String createSpaceStatement(String space) {
        return "CREATE SPACE IF NOT EXISTS `" + space + "` (partition_num=10, replica_factor=1, vid_type=FIXED_STRING(256));";
    }

    private static int sizeOf(List<?> values) {
        return values == null ? 0 : values.size();
    }

    private static int nonEmptyStores(Map<String, RecordStore> stores) {
        int count = 0;
        for (RecordStore store : stores.values()) {
            if (!store.rows.isEmpty()) {
                count++;
            }
        }
        return count;
    }

    private static String toCsvRow(String[] values) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(csvEscape(values[i]));
        }
        return builder.toString();
    }

    private static String csvEscape(String value) {
        String normalized = value == null ? NULL_TOKEN : value;
        boolean needsQuote = normalized.contains(",")
                || normalized.contains("\"")
                || normalized.contains("\n")
                || normalized.contains("\r");
        if (!needsQuote) {
            return normalized;
        }
        return "\"" + normalized.replace("\"", "\"\"") + "\"";
    }

    private static String yamlQuote(String value) {
        String normalized = value == null ? "" : value;
        return "\"" + normalized
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                + "\"";
    }

    private static String timestamp(long captureTimeEpochMs) {
        return String.valueOf(captureTimeEpochMs / 1000L);
    }

    private static String integer(Integer value) {
        return value == null ? null : String.valueOf(value);
    }

    private static String bool(boolean value) {
        return String.valueOf(value);
    }

    private static String join(List<String> values, String delimiter) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return String.join(delimiter, values);
    }

    private static String inferTaskId(NormalizedLineageResult result) {
        for (TableLineageEdge tableEdge : result.getTableEdges()) {
            if (tableEdge.getTaskId() != null && !tableEdge.getTaskId().isEmpty()) {
                return tableEdge.getTaskId();
            }
        }
        return null;
    }

    private static String inferRunId(NormalizedLineageResult result) {
        for (TableLineageEdge tableEdge : result.getTableEdges()) {
            if (tableEdge.getRunId() != null && !tableEdge.getRunId().isEmpty()) {
                return tableEdge.getRunId();
            }
        }
        return null;
    }

    private static String tableOwnerVid(ColumnNode columnNode) {
        if (columnNode.getOwnerId() == null || columnNode.getOwnerId().isEmpty()) {
            return null;
        }
        if ("TABLE".equals(columnNode.getOwnerType()) || "TABLE_OR_SUBQUERY".equals(columnNode.getOwnerType())) {
            return "table:" + columnNode.getOwnerId();
        }
        return null;
    }

    private static String taskVid(String taskId) {
        return "task:" + taskId;
    }

    private static String runVid(String runId) {
        return "run:" + runId;
    }

    private static String captureVid(String eventId) {
        return "capture:" + eventId;
    }

    private static String tableVid(TableRef tableRef) {
        return "table:" + tableRef.normalizedName();
    }

    private static String nullableRankToken(String value) {
        return value == null ? "NULL" : "\"" + escape(value) + "\"";
    }

    private static String escape(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    private static long latestSemanticRank(String edgeName, String role) {
        return edgeRank(edgeName, role == null ? "" : role);
    }

    private static long edgeRank(String... values) {
        String digest = HashUtils.sha1(String.join("|", values));
        String hex = digest.substring(0, 15);
        return Long.parseLong(hex, 16);
    }

    private static String sanitize(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "bundle";
        }
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException error) {
            return defaultValue;
        }
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

    private static String read(String propertyKey, String envKey, String defaultValue) {
        String value = read(propertyKey, envKey);
        return value == null ? defaultValue : value;
    }
}
