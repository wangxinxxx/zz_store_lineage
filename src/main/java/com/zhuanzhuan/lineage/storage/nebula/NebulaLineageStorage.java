package com.zhuanzhuan.lineage.storage.nebula;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.zhuanzhuan.lineage.common.HashUtils;
import com.zhuanzhuan.lineage.model.ColumnInstanceNode;
import com.zhuanzhuan.lineage.model.ColumnNode;
import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExpressionNode;
import com.zhuanzhuan.lineage.model.LiteralNode;
import com.zhuanzhuan.lineage.model.LineageGraphEdge;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.OperatorInstanceNode;
import com.zhuanzhuan.lineage.model.PredicateNode;
import com.zhuanzhuan.lineage.model.RelationInstanceNode;
import com.zhuanzhuan.lineage.model.ScopeNode;
import com.zhuanzhuan.lineage.model.TableLineageEdge;
import com.zhuanzhuan.lineage.model.TableRef;
import com.zhuanzhuan.lineage.storage.LineageStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NebulaLineageStorage implements LineageStorage, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NebulaLineageStorage.class);
    private static final int MAX_WRITE_RETRIES = 20;
    private static final int STATEMENT_FLUSH_THRESHOLD = 2000;

    private final NebulaGraphConfig config;
    private final NebulaPool pool;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public NebulaLineageStorage(NebulaGraphConfig config) {
        this.config = config;
        this.pool = new NebulaPool();
        initPool();
    }

    @Override
    public void saveCapture(ExecutionCaptureEvent event) {
        ensureInitialized();
        executeAll(buildCaptureStatements(event));
    }

    @Override
    public void saveLineage(NormalizedLineageResult result) {
        ensureInitialized();
        executeAll(buildLineageStatements(result));
    }

    @Override
    public void saveBatch(List<ExecutionCaptureEvent> events, List<NormalizedLineageResult> results) {
        ensureInitialized();
        List<String> statements = new ArrayList<String>();
        for (ExecutionCaptureEvent event : events) {
            statements.addAll(buildCaptureStatements(event));
            flushIfNeeded(statements);
        }
        for (NormalizedLineageResult result : results) {
            statements.addAll(buildLineageStatements(result));
            flushIfNeeded(statements);
        }
        flushStatements(statements);
    }

    public void executeStatements(List<String> statements) {
        ensureInitialized();
        executeAll(statements == null ? new ArrayList<String>() : statements);
    }

    public static List<String> buildBatchStatements(List<ExecutionCaptureEvent> events, List<NormalizedLineageResult> results) {
        List<String> statements = new ArrayList<String>();
        if (events != null) {
            for (ExecutionCaptureEvent event : events) {
                if (event == null) {
                    continue;
                }
                statements.addAll(buildCaptureStatements(event));
            }
        }
        if (results != null) {
            for (NormalizedLineageResult result : results) {
                if (result == null) {
                    continue;
                }
                statements.addAll(buildLineageStatements(result));
            }
        }
        return statements;
    }

    public static List<String> buildCaptureStatements(ExecutionCaptureEvent event) {
        List<String> statements = new ArrayList<String>();
        String captureVid = captureVid(event.getEventId());
        statements.add(upsertCaptureEventVertex(event));

        if (event.getTaskContext().getTaskId() != null) {
            statements.add(upsertTaskVertex(event));
            statements.add(insertEdge(
                    "task_has_execution",
                    taskVid(event.getTaskContext().getTaskId()),
                    captureVid,
                    edgeRank(event.getEventId(), "task_has_execution"),
                    quote(event.getEventId()),
                    quoteLong(event.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        String runId = event.getTaskContext().getRunId();
        if (runId != null) {
            statements.add(upsertRunVertex(event));
            if (event.getTaskContext().getTaskId() != null) {
                statements.add(insertEdge(
                        "task_has_run",
                        taskVid(event.getTaskContext().getTaskId()),
                        runVid(runId),
                        edgeRank(event.getTaskContext().getTaskId(), runId, "task_has_run"),
                        quote(runId),
                        quoteLong(event.getCaptureTimeEpochMs() / 1000L)
                ));
            }
            statements.add(insertEdge(
                    "run_has_execution",
                    runVid(runId),
                    captureVid,
                    edgeRank(event.getEventId(), "run_has_execution"),
                    quote(event.getEventId()),
                    quoteLong(event.getCaptureTimeEpochMs() / 1000L)
            ));
        }
        return statements;
    }

    public static List<String> buildLineageStatements(NormalizedLineageResult result) {
        List<String> statements = new ArrayList<String>();
        Set<String> writtenNodeIds = new LinkedHashSet<>();
        String captureVid = captureVid(result.getEventId());
        Set<String> knownTableOwners = new LinkedHashSet<>();

        statements.add(upsertCaptureEventVertex(result));
        statements.add(updateCaptureEventVertex(result));

        for (TableRef tableRef : result.getInputTables()) {
            statements.add(upsertTableVertex(tableRef));
            String vid = tableVid(tableRef);
            writtenNodeIds.add(vid);
            knownTableOwners.add(vid);
            statements.add(insertEdge(
                    "execution_reads_table",
                    captureVid,
                    vid,
                    edgeRank(result.getEventId(), captureVid, vid, "execution_reads_table"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }
        for (TableRef tableRef : result.getOutputTables()) {
            String vid = tableVid(tableRef);
            if (writtenNodeIds.add(vid)) {
                statements.add(upsertTableVertex(tableRef));
            }
            knownTableOwners.add(vid);
            statements.add(insertEdge(
                    "execution_writes_table",
                    captureVid,
                    vid,
                    edgeRank(result.getEventId(), captureVid, vid, "execution_writes_table"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        for (ColumnNode columnNode : result.getColumnNodes()) {
            if (writtenNodeIds.add(columnNode.getNodeId())) {
                statements.add(upsertColumnVertex(columnNode));
            }
            statements.add(insertEdge(
                    "execution_emits_column",
                    captureVid,
                    columnNode.getNodeId(),
                    edgeRank(result.getEventId(), captureVid, columnNode.getNodeId(), "execution_emits_column"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
            String ownerVid = tableOwnerVid(columnNode);
            if (ownerVid != null && knownTableOwners.contains(ownerVid)) {
                statements.add(insertEdgeIfNotExists(
                        "table_has_column",
                        ownerVid,
                        columnNode.getNodeId(),
                        0L
                ));
            }
        }

        for (ExpressionNode expressionNode : result.getExpressionNodes()) {
            if (writtenNodeIds.add(expressionNode.getNodeId())) {
                statements.add(upsertExpressionVertex(expressionNode));
            }
            statements.add(insertEdge(
                    "execution_observes_expression",
                    captureVid,
                    expressionNode.getNodeId(),
                    edgeRank(result.getEventId(), captureVid, expressionNode.getNodeId(), "execution_observes_expression"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        for (ScopeNode scopeNode : result.getScopeNodes()) {
            if (writtenNodeIds.add(scopeNode.getNodeId())) {
                statements.add(upsertScopeVertex(scopeNode));
            }
            statements.add(insertEdge(
                    "execution_observes_scope",
                    captureVid,
                    scopeNode.getNodeId(),
                    edgeRank(result.getEventId(), captureVid, scopeNode.getNodeId(), "execution_observes_scope"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        for (LiteralNode literalNode : result.getLiteralNodes()) {
            if (writtenNodeIds.add(literalNode.getNodeId())) {
                statements.add(upsertLiteralVertex(literalNode));
            }
            statements.add(insertEdge(
                    "execution_observes_literal",
                    captureVid,
                    literalNode.getNodeId(),
                    edgeRank(result.getEventId(), captureVid, literalNode.getNodeId(), "execution_observes_literal"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        for (OperatorInstanceNode operatorInstanceNode : result.getOperatorInstanceNodes()) {
            if (writtenNodeIds.add(operatorInstanceNode.getNodeId())) {
                statements.add(upsertOperatorInstanceVertex(operatorInstanceNode));
            }
            statements.add(insertEdge(
                    "execution_observes_operator_instance",
                    captureVid,
                    operatorInstanceNode.getNodeId(),
                    edgeRank(result.getEventId(), captureVid, operatorInstanceNode.getNodeId(), "execution_observes_operator_instance"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        for (ColumnInstanceNode columnInstanceNode : result.getColumnInstanceNodes()) {
            if (writtenNodeIds.add(columnInstanceNode.getNodeId())) {
                statements.add(upsertColumnInstanceVertex(columnInstanceNode));
            }
            statements.add(insertEdge(
                    "execution_observes_column_instance",
                    captureVid,
                    columnInstanceNode.getNodeId(),
                    edgeRank(result.getEventId(), captureVid, columnInstanceNode.getNodeId(), "execution_observes_column_instance"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        for (RelationInstanceNode relationInstanceNode : result.getRelationInstanceNodes()) {
            if (writtenNodeIds.add(relationInstanceNode.getNodeId())) {
                statements.add(upsertRelationInstanceVertex(relationInstanceNode));
            }
            statements.add(insertEdge(
                    "execution_observes_relation_instance",
                    captureVid,
                    relationInstanceNode.getNodeId(),
                    edgeRank(result.getEventId(), captureVid, relationInstanceNode.getNodeId(), "execution_observes_relation_instance"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        for (PredicateNode predicateNode : result.getPredicateNodes()) {
            if (writtenNodeIds.add(predicateNode.getNodeId())) {
                statements.add(upsertPredicateVertex(predicateNode));
            }
            statements.add(insertEdge(
                    "execution_observes_predicate",
                    captureVid,
                    predicateNode.getNodeId(),
                    edgeRank(result.getEventId(), captureVid, predicateNode.getNodeId(), "execution_observes_predicate"),
                    quote(result.getEventId()),
                    quoteLong(result.getCaptureTimeEpochMs() / 1000L)
            ));
        }

        for (TableLineageEdge tableEdge : result.getTableEdges()) {
            String sourceVid = tableVid(tableEdge.getSourceTable());
            String targetVid = tableVid(tableEdge.getTargetTable());
            statements.add(insertEdge(
                    "table_flows_to_table",
                    sourceVid,
                    targetVid,
                    edgeRank(tableEdge.getEventId(), sourceVid, targetVid, "table_flows_to_table"),
                    quote(tableEdge.getEventId()),
                    nullableQuote(tableEdge.getTaskId()),
                    nullableQuote(tableEdge.getRunId()),
                    nullableQuote(tableEdge.getWriteMode()),
                    quoteLong(tableEdge.getCaptureTimeEpochMs() / 1000L),
                    nullableQuote(tableEdge.getConfidence())
            ));
            statements.addAll(replaceLatestEdge(
                    "latest_table_flows_to_table",
                    sourceVid,
                    targetVid,
                    nullableQuote(tableEdge.getEventId()),
                    nullableQuote(tableEdge.getTaskId()),
                    nullableQuote(tableEdge.getRunId()),
                    nullableQuote(tableEdge.getWriteMode()),
                    quoteLong(tableEdge.getCaptureTimeEpochMs() / 1000L),
                    nullableQuote(tableEdge.getConfidence())
            ));

            if (tableEdge.getTaskId() != null) {
                statements.add(insertEdge(
                        "task_reads_table",
                        taskVid(tableEdge.getTaskId()),
                        sourceVid,
                        edgeRank(tableEdge.getEventId(), tableEdge.getTaskId(), tableEdge.getSourceTable().normalizedName(), "task_reads_table"),
                        quote(tableEdge.getEventId()),
                        nullableQuote(tableEdge.getRunId()),
                        quoteLong(tableEdge.getCaptureTimeEpochMs() / 1000L)
                ));
                statements.add(insertEdge(
                        "task_writes_table",
                        taskVid(tableEdge.getTaskId()),
                        targetVid,
                        edgeRank(tableEdge.getEventId(), tableEdge.getTaskId(), tableEdge.getTargetTable().normalizedName(), "task_writes_table"),
                        quote(tableEdge.getEventId()),
                        nullableQuote(tableEdge.getRunId()),
                        quoteLong(tableEdge.getCaptureTimeEpochMs() / 1000L)
                ));
            }
        }

        for (LineageGraphEdge graphEdge : result.getGraphEdges()) {
            statements.addAll(buildSemanticLineageEdges(result, graphEdge));
        }

        return statements;
    }

    @Override
    public void close() {
        pool.close();
    }

    @Override
    public void clear() {
        try (Session session = pool.getSession(config.getUsername(), config.getPassword(), true)) {
            ResultSet resultSet = session.execute("DROP SPACE IF EXISTS `" + config.getSpace() + "`;");
            if (!resultSet.isSucceeded()) {
                throw new IllegalStateException(resultSet.getErrorMessage());
            }
            initialized.set(false);
        } catch (Exception error) {
            throw new IllegalStateException("Failed to clear NebulaGraph space `" + config.getSpace() + "`.", error);
        }
    }

    private void initPool() {
        NebulaPoolConfig poolConfig = new NebulaPoolConfig()
                .setMinConnSize(config.getMinConnSize())
                .setMaxConnSize(config.getMaxConnSize())
                .setTimeout(config.getTimeoutMs())
                .setMinClusterHealthRate(0.0d);

        try {
            pool.init(Arrays.asList(new HostAddress(config.getHost(), config.getPort())), poolConfig);
        } catch (Exception error) {
            throw new IllegalStateException(
                    "Failed to initialize NebulaGraph pool for " + config.getHost() + ":" + config.getPort(),
                    error
            );
        }
    }

    private void ensureInitialized() {
        if (initialized.get()) {
            return;
        }

        synchronized (initialized) {
            if (initialized.get()) {
                return;
            }
            bootstrapSchema();
            initialized.set(true);
        }
    }

    private void bootstrapSchema() {
        executeSchemaStatement("CREATE SPACE IF NOT EXISTS `" + config.getSpace() + "` (partition_num=10, replica_factor=1, vid_type=FIXED_STRING(256));");
        waitForSpaceReady();

        List<String> statements = Arrays.asList(
                "USE `" + config.getSpace() + "`;",
                "CREATE TAG IF NOT EXISTS `task_node`(`task_name` string, `owner` string, `script_path` string, `biz_date` string);",
                "CREATE TAG IF NOT EXISTS `run_node`(`run_id` string, `biz_date` string);",
                "CREATE TAG IF NOT EXISTS `capture_event`(`func_name` string, `status` string, `capture_time` timestamp, `statement_type` string, `error_message` string);",
                "CREATE TAG IF NOT EXISTS `table_node`(`normalized_name` string, `catalog_name` string, `database_name` string, `table_name` string, `source_type` string);",
                "CREATE TAG IF NOT EXISTS `column_node`(`column_name` string, `owner_id` string, `owner_type` string, `data_type` string, `qualifier` string);",
                "CREATE TAG IF NOT EXISTS `expression_node`(`expression_type` string, `expression_sql` string, `normalized_expression` string);",
                "CREATE TAG IF NOT EXISTS `scope_node`(`scope_name` string, `scope_type` string, `parent_scope_id` string, `plan_node_name` string);",
                "CREATE TAG IF NOT EXISTS `literal_node`(`literal_type` string, `literal_value` string, `normalized_value` string);",
                "CREATE TAG IF NOT EXISTS `operator_instance_node`(`scope_id` string, `operator_type` string, `operator_sub_type` string, `operator_path` string, `parent_operator_id` string, `plan_node_name` string);",
                "CREATE TAG IF NOT EXISTS `column_instance_node`(`column_id` string, `column_name` string, `scope_id` string, `relation_instance_id` string, `instance_type` string, `data_type` string, `ordinal` int);",
                "CREATE TAG IF NOT EXISTS `relation_instance_node`(`instance_name` string, `scope_id` string, `source_table_id` string, `source_type` string, `alias_name` string, `plan_node_name` string);",
                "CREATE TAG IF NOT EXISTS `predicate_node`(`predicate_type` string, `predicate_sql` string, `normalized_predicate` string, `scope_id` string, `plan_node_name` string);",
                "CREATE EDGE IF NOT EXISTS `task_has_run`(`run_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `task_has_execution`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `run_has_execution`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `task_reads_table`(`event_id` string, `run_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `task_writes_table`(`event_id` string, `run_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_reads_table`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_writes_table`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_emits_column`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_observes_expression`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_observes_scope`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_observes_literal`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_observes_operator_instance`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_observes_column_instance`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_observes_relation_instance`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `execution_observes_predicate`(`event_id` string, `capture_time` timestamp);",
                "CREATE EDGE IF NOT EXISTS `table_has_column`();",
                "CREATE EDGE IF NOT EXISTS `scope_contains_scope`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `scope_outputs_column`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `scope_uses_expression`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `scope_contains_operator_instance`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `operator_precedes_operator_instance`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `operator_outputs_column_instance`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `operator_uses_expression`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `operator_uses_predicate`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `operator_reads_relation_instance`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `scope_contains_column_instance`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `scope_reads_relation_instance`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `relation_instance_exposes_column_instance`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `relation_instance_of_table`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `relation_instance_joins_relation_instance`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_relation_instance_joins_relation_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `column_has_instance`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `scope_uses_predicate`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `relation_instance_filtered_by_predicate`(`event_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `table_depends_on_table`(`event_id` string, `task_id` string, `run_id` string, `write_mode` string, `capture_time` timestamp, `confidence` string);",
                "CREATE EDGE IF NOT EXISTS `latest_table_depends_on_table`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_write_mode` string, `last_seen_time` timestamp, `confidence` string);",
                "CREATE EDGE IF NOT EXISTS `table_flows_to_table`(`event_id` string, `task_id` string, `run_id` string, `write_mode` string, `capture_time` timestamp, `confidence` string);",
                "CREATE EDGE IF NOT EXISTS `latest_table_flows_to_table`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_write_mode` string, `last_seen_time` timestamp, `confidence` string);",
                "CREATE EDGE IF NOT EXISTS `column_depends_on_column`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_depends_on_column`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_flows_to_column`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `column_uses_expression`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_uses_expression`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_flows_to_expression`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `expression_depends_on_column`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_expression_depends_on_column`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_expression_flows_to_column`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `expression_depends_on_expression`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_expression_depends_on_expression`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_expression_flows_to_expression`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `expression_depends_on_literal`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_expression_depends_on_literal`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_literal_flows_to_expression`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `expression_depends_on_column_instance`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_expression_depends_on_column_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_instance_flows_to_expression`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `column_instance_uses_expression`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_instance_uses_expression`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_expression_flows_to_column_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `column_instance_depends_on_column_instance`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_instance_depends_on_column_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_instance_flows_to_column_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `predicate_depends_on_column`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_predicate_depends_on_column`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_flows_to_predicate`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `predicate_depends_on_column_instance`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_predicate_depends_on_column_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_instance_flows_to_predicate`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `column_instance_depends_on_relation_instance`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_instance_depends_on_relation_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_relation_instance_flows_to_column_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `column_instance_filtered_by_predicate`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_column_instance_filtered_by_predicate`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_predicate_flows_to_column_instance`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `predicate_depends_on_literal`(`event_id` string, `task_id` string, `run_id` string, `capture_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_predicate_depends_on_literal`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);",
                "CREATE EDGE IF NOT EXISTS `latest_literal_flows_to_predicate`(`last_event_id` string, `last_task_id` string, `last_run_id` string, `last_seen_time` timestamp, `role` string);"
        );

        executeAll(statements);
        waitForSchemaReady();
    }

    private void waitForSpaceReady() {
        RuntimeException lastError = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try (Session session = pool.getSession(config.getUsername(), config.getPassword(), true)) {
                ResultSet resultSet = session.execute("USE `" + config.getSpace() + "`;");
                if (resultSet.isSucceeded()) {
                    return;
                }
                lastError = new IllegalStateException(resultSet.getErrorMessage());
            } catch (Exception error) {
                lastError = new RuntimeException(error);
            }

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        throw new IllegalStateException("NebulaGraph space `" + config.getSpace() + "` is not ready.", lastError);
    }

    private void executeSchemaStatement(String statement) {
        try (Session session = pool.getSession(config.getUsername(), config.getPassword(), true)) {
            ResultSet resultSet = session.execute(statement);
            if (!resultSet.isSucceeded()) {
                throw new IllegalStateException(resultSet.getErrorMessage());
            }
        } catch (Exception error) {
            throw new IllegalStateException("Failed to execute schema statement: " + statement, error);
        }
    }

    private void executeAll(List<String> statements) {
        if (statements.isEmpty()) {
            return;
        }

        RuntimeException lastError = null;
        for (int attempt = 0; attempt < MAX_WRITE_RETRIES; attempt++) {
            try (Session session = pool.getSession(config.getUsername(), config.getPassword(), true)) {
                ResultSet useResult = session.execute("USE `" + config.getSpace() + "`;");
                if (!useResult.isSucceeded()) {
                    throw new IllegalStateException(useResult.getErrorMessage());
                }

                for (String statement : statements) {
                    ResultSet resultSet = session.execute(statement);
                    if (!resultSet.isSucceeded()) {
                        throw new IllegalStateException("Failed nGQL: " + statement + ", error=" + resultSet.getErrorMessage());
                    }
                }
                return;
            } catch (Exception error) {
                lastError = new IllegalStateException("Failed to persist lineage into NebulaGraph.", error);
                if (!isRetryableNebulaError(error) || attempt == MAX_WRITE_RETRIES - 1) {
                    throw lastError;
                }
                sleepQuietly(1500L);
            }
        }
    }

    private void flushIfNeeded(List<String> statements) {
        if (statements.size() >= STATEMENT_FLUSH_THRESHOLD) {
            flushStatements(statements);
        }
    }

    private void flushStatements(List<String> statements) {
        if (statements.isEmpty()) {
            return;
        }
        List<String> batch = new ArrayList<String>(statements);
        statements.clear();
        executeAll(batch);
    }

    private void waitForSchemaReady() {
        RuntimeException lastError = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            try (Session session = pool.getSession(config.getUsername(), config.getPassword(), true)) {
                ResultSet useResult = session.execute("USE `" + config.getSpace() + "`;");
                if (!useResult.isSucceeded()) {
                    lastError = new IllegalStateException(useResult.getErrorMessage());
                } else {
                    ResultSet captureTag = session.execute("DESCRIBE TAG `capture_event`;");
                    ResultSet scopeTag = session.execute("DESCRIBE TAG `scope_node`;");
                    ResultSet literalTag = session.execute("DESCRIBE TAG `literal_node`;");
                    ResultSet operatorInstanceTag = session.execute("DESCRIBE TAG `operator_instance_node`;");
                    ResultSet columnInstanceTag = session.execute("DESCRIBE TAG `column_instance_node`;");
                    ResultSet relationInstanceTag = session.execute("DESCRIBE TAG `relation_instance_node`;");
                    ResultSet predicateTag = session.execute("DESCRIBE TAG `predicate_node`;");
                    ResultSet taskExecutionEdge = session.execute("DESCRIBE EDGE `task_has_execution`;");
                    ResultSet tableFlowEdge = session.execute("DESCRIBE EDGE `table_flows_to_table`;");
                    ResultSet latestTableFlowEdge = session.execute("DESCRIBE EDGE `latest_table_flows_to_table`;");
                    ResultSet latestColumnFlowEdge = session.execute("DESCRIBE EDGE `latest_column_flows_to_column`;");
                    ResultSet latestColumnExpressionFlowEdge = session.execute("DESCRIBE EDGE `latest_column_flows_to_expression`;");
                    ResultSet latestExpressionColumnFlowEdge = session.execute("DESCRIBE EDGE `latest_expression_flows_to_column`;");
                    ResultSet latestExpressionFlowEdge = session.execute("DESCRIBE EDGE `latest_expression_flows_to_expression`;");
                    ResultSet latestExpressionEdge = session.execute("DESCRIBE EDGE `latest_expression_depends_on_expression`;");
                    ResultSet scopeContainsEdge = session.execute("DESCRIBE EDGE `scope_contains_scope`;");
                    ResultSet scopeOutputEdge = session.execute("DESCRIBE EDGE `scope_outputs_column`;");
                    ResultSet scopeExpressionEdge = session.execute("DESCRIBE EDGE `scope_uses_expression`;");
                    ResultSet scopeOperatorInstanceEdge = session.execute("DESCRIBE EDGE `scope_contains_operator_instance`;");
                    ResultSet operatorOperatorEdge = session.execute("DESCRIBE EDGE `operator_precedes_operator_instance`;");
                    ResultSet operatorColumnInstanceEdge = session.execute("DESCRIBE EDGE `operator_outputs_column_instance`;");
                    ResultSet operatorExpressionEdge = session.execute("DESCRIBE EDGE `operator_uses_expression`;");
                    ResultSet operatorPredicateEdge = session.execute("DESCRIBE EDGE `operator_uses_predicate`;");
                    ResultSet operatorRelationEdge = session.execute("DESCRIBE EDGE `operator_reads_relation_instance`;");
                    ResultSet scopeColumnInstanceEdge = session.execute("DESCRIBE EDGE `scope_contains_column_instance`;");
                    ResultSet scopeReadsRelationEdge = session.execute("DESCRIBE EDGE `scope_reads_relation_instance`;");
                    ResultSet relationColumnInstanceEdge = session.execute("DESCRIBE EDGE `relation_instance_exposes_column_instance`;");
                    ResultSet relationJoinEdge = session.execute("DESCRIBE EDGE `relation_instance_joins_relation_instance`;");
                    ResultSet latestRelationJoinEdge = session.execute("DESCRIBE EDGE `latest_relation_instance_joins_relation_instance`;");
                    ResultSet columnHasInstanceEdge = session.execute("DESCRIBE EDGE `column_has_instance`;");
                    ResultSet scopeUsesPredicateEdge = session.execute("DESCRIBE EDGE `scope_uses_predicate`;");
                    ResultSet relationPredicateEdge = session.execute("DESCRIBE EDGE `relation_instance_filtered_by_predicate`;");
                    ResultSet latestLiteralFlowEdge = session.execute("DESCRIBE EDGE `latest_literal_flows_to_expression`;");
                    ResultSet latestColumnInstanceExpressionFlowEdge = session.execute("DESCRIBE EDGE `latest_column_instance_flows_to_expression`;");
                    ResultSet latestExpressionColumnInstanceFlowEdge = session.execute("DESCRIBE EDGE `latest_expression_flows_to_column_instance`;");
                    ResultSet latestColumnInstanceColumnInstanceFlowEdge = session.execute("DESCRIBE EDGE `latest_column_instance_flows_to_column_instance`;");
                    ResultSet latestColumnInstancePredicateFlowEdge = session.execute("DESCRIBE EDGE `latest_column_instance_flows_to_predicate`;");
                    ResultSet latestRelationInstanceColumnInstanceFlowEdge = session.execute("DESCRIBE EDGE `latest_relation_instance_flows_to_column_instance`;");
                    ResultSet latestPredicateColumnInstanceFlowEdge = session.execute("DESCRIBE EDGE `latest_predicate_flows_to_column_instance`;");
                    ResultSet latestPredicateLiteralFlowEdge = session.execute("DESCRIBE EDGE `latest_literal_flows_to_predicate`;");
                    if (captureTag.isSucceeded()
                            && scopeTag.isSucceeded()
                            && literalTag.isSucceeded()
                            && operatorInstanceTag.isSucceeded()
                            && columnInstanceTag.isSucceeded()
                            && relationInstanceTag.isSucceeded()
                            && predicateTag.isSucceeded()
                            && taskExecutionEdge.isSucceeded()
                            && tableFlowEdge.isSucceeded()
                            && latestTableFlowEdge.isSucceeded()
                            && latestColumnFlowEdge.isSucceeded()
                            && latestColumnExpressionFlowEdge.isSucceeded()
                            && latestExpressionColumnFlowEdge.isSucceeded()
                            && latestExpressionFlowEdge.isSucceeded()
                            && latestExpressionEdge.isSucceeded()
                            && scopeContainsEdge.isSucceeded()
                            && scopeOutputEdge.isSucceeded()
                            && scopeExpressionEdge.isSucceeded()
                            && scopeOperatorInstanceEdge.isSucceeded()
                            && operatorOperatorEdge.isSucceeded()
                            && operatorColumnInstanceEdge.isSucceeded()
                            && operatorExpressionEdge.isSucceeded()
                            && operatorPredicateEdge.isSucceeded()
                            && operatorRelationEdge.isSucceeded()
                            && scopeColumnInstanceEdge.isSucceeded()
                            && scopeReadsRelationEdge.isSucceeded()
                            && relationColumnInstanceEdge.isSucceeded()
                            && relationJoinEdge.isSucceeded()
                            && latestRelationJoinEdge.isSucceeded()
                            && columnHasInstanceEdge.isSucceeded()
                            && scopeUsesPredicateEdge.isSucceeded()
                            && relationPredicateEdge.isSucceeded()
                            && latestLiteralFlowEdge.isSucceeded()
                            && latestColumnInstanceExpressionFlowEdge.isSucceeded()
                            && latestExpressionColumnInstanceFlowEdge.isSucceeded()
                            && latestColumnInstanceColumnInstanceFlowEdge.isSucceeded()
                            && latestColumnInstancePredicateFlowEdge.isSucceeded()
                            && latestRelationInstanceColumnInstanceFlowEdge.isSucceeded()
                            && latestPredicateColumnInstanceFlowEdge.isSucceeded()
                            && latestPredicateLiteralFlowEdge.isSucceeded()) {
                        return;
                    }
                    lastError = new IllegalStateException(
                            "capture_event=" + captureTag.getErrorMessage()
                                    + ", scope_node=" + scopeTag.getErrorMessage()
                                    + ", literal_node=" + literalTag.getErrorMessage()
                                    + ", operator_instance_node=" + operatorInstanceTag.getErrorMessage()
                                    + ", column_instance_node=" + columnInstanceTag.getErrorMessage()
                                    + ", relation_instance_node=" + relationInstanceTag.getErrorMessage()
                                    + ", predicate_node=" + predicateTag.getErrorMessage()
                                    + ", task_has_execution=" + taskExecutionEdge.getErrorMessage()
                                    + ", table_flows_to_table=" + tableFlowEdge.getErrorMessage()
                                    + ", latest_table_flows_to_table=" + latestTableFlowEdge.getErrorMessage()
                                    + ", latest_column_flows_to_column=" + latestColumnFlowEdge.getErrorMessage()
                                    + ", latest_column_flows_to_expression=" + latestColumnExpressionFlowEdge.getErrorMessage()
                                    + ", latest_expression_flows_to_column=" + latestExpressionColumnFlowEdge.getErrorMessage()
                                    + ", latest_expression_flows_to_expression=" + latestExpressionFlowEdge.getErrorMessage()
                                    + ", latest_expression_depends_on_expression=" + latestExpressionEdge.getErrorMessage()
                                    + ", scope_contains_scope=" + scopeContainsEdge.getErrorMessage()
                                    + ", scope_outputs_column=" + scopeOutputEdge.getErrorMessage()
                                    + ", scope_uses_expression=" + scopeExpressionEdge.getErrorMessage()
                                    + ", scope_contains_operator_instance=" + scopeOperatorInstanceEdge.getErrorMessage()
                                    + ", operator_precedes_operator_instance=" + operatorOperatorEdge.getErrorMessage()
                                    + ", operator_outputs_column_instance=" + operatorColumnInstanceEdge.getErrorMessage()
                                    + ", operator_uses_expression=" + operatorExpressionEdge.getErrorMessage()
                                    + ", operator_uses_predicate=" + operatorPredicateEdge.getErrorMessage()
                                    + ", operator_reads_relation_instance=" + operatorRelationEdge.getErrorMessage()
                                    + ", scope_contains_column_instance=" + scopeColumnInstanceEdge.getErrorMessage()
                                    + ", scope_reads_relation_instance=" + scopeReadsRelationEdge.getErrorMessage()
                                    + ", relation_instance_exposes_column_instance=" + relationColumnInstanceEdge.getErrorMessage()
                                    + ", relation_instance_joins_relation_instance=" + relationJoinEdge.getErrorMessage()
                                    + ", latest_relation_instance_joins_relation_instance=" + latestRelationJoinEdge.getErrorMessage()
                                    + ", column_has_instance=" + columnHasInstanceEdge.getErrorMessage()
                                    + ", scope_uses_predicate=" + scopeUsesPredicateEdge.getErrorMessage()
                                    + ", relation_instance_filtered_by_predicate=" + relationPredicateEdge.getErrorMessage()
                                    + ", latest_literal_flows_to_expression=" + latestLiteralFlowEdge.getErrorMessage()
                                    + ", latest_column_instance_flows_to_expression=" + latestColumnInstanceExpressionFlowEdge.getErrorMessage()
                                    + ", latest_expression_flows_to_column_instance=" + latestExpressionColumnInstanceFlowEdge.getErrorMessage()
                                    + ", latest_column_instance_flows_to_column_instance=" + latestColumnInstanceColumnInstanceFlowEdge.getErrorMessage()
                                    + ", latest_column_instance_flows_to_predicate=" + latestColumnInstancePredicateFlowEdge.getErrorMessage()
                                    + ", latest_relation_instance_flows_to_column_instance=" + latestRelationInstanceColumnInstanceFlowEdge.getErrorMessage()
                                    + ", latest_predicate_flows_to_column_instance=" + latestPredicateColumnInstanceFlowEdge.getErrorMessage()
                                    + ", latest_literal_flows_to_predicate=" + latestPredicateLiteralFlowEdge.getErrorMessage()
                    );
                }
            } catch (Exception error) {
                lastError = new RuntimeException(error);
            }
            sleepQuietly(1000L);
        }

        throw new IllegalStateException("NebulaGraph schema was not ready after bootstrap.", lastError);
    }

    private boolean isRetryableNebulaError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && (message.contains("No schema found")
                    || message.contains("not existed")
                    || message.contains("EdgeNotFound")
                    || message.contains("EdgeName")
                    || message.contains("Edge not found")
                    || message.contains("Tag not found")
                    || message.contains("Not the leader")
                    || message.contains("Leader changed")
                    || message.contains("please retry later")
                    || message.contains("Please retry later"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    private static String upsertTaskVertex(ExecutionCaptureEvent event) {
        String taskId = event.getTaskContext().getTaskId();
        return "INSERT VERTEX IF NOT EXISTS `task_node`(`task_name`, `owner`, `script_path`, `biz_date`) VALUES "
                + quoteVid(taskVid(taskId))
                + ":("
                + nullableQuote(event.getTaskContext().getTaskName()) + ","
                + nullableQuote(event.getTaskContext().getOwner()) + ","
                + nullableQuote(event.getTaskContext().getScriptPath()) + ","
                + nullableQuote(event.getTaskContext().getBizDate())
                + ");";
    }

    private static String upsertRunVertex(ExecutionCaptureEvent event) {
        String runId = event.getTaskContext().getRunId();
        return "INSERT VERTEX IF NOT EXISTS `run_node`(`run_id`, `biz_date`) VALUES "
                + quoteVid(runVid(runId))
                + ":("
                + nullableQuote(runId) + ","
                + nullableQuote(event.getTaskContext().getBizDate())
                + ");";
    }

    private static String upsertCaptureEventVertex(ExecutionCaptureEvent event) {
        return "INSERT VERTEX IF NOT EXISTS `capture_event`(`func_name`, `status`, `capture_time`, `statement_type`, `error_message`) VALUES "
                + quoteVid(captureVid(event.getEventId()))
                + ":("
                + nullableQuote(event.getFuncName()) + ","
                + quote(event.getStatus().name()) + ","
                + quoteLong(event.getCaptureTimeEpochMs() / 1000L) + ","
                + quote("UNKNOWN") + ","
                + nullableQuote(event.getErrorMessage())
                + ");";
    }

    private static String upsertCaptureEventVertex(NormalizedLineageResult result) {
        return "INSERT VERTEX IF NOT EXISTS `capture_event`(`func_name`, `status`, `capture_time`, `statement_type`, `error_message`) VALUES "
                + quoteVid(captureVid(result.getEventId()))
                + ":("
                + quote("unknown") + ","
                + quote("SUCCESS") + ","
                + quoteLong(result.getCaptureTimeEpochMs() / 1000L) + ","
                + nullableQuote(result.getStatementType()) + ","
                + "NULL"
                + ");";
    }

    private static String updateCaptureEventVertex(NormalizedLineageResult result) {
        return "UPDATE VERTEX ON `capture_event` "
                + quoteVid(captureVid(result.getEventId()))
                + " SET "
                + "`capture_event`.`statement_type` = " + nullableQuote(result.getStatementType()) + ", "
                + "`capture_event`.`capture_time` = " + quoteLong(result.getCaptureTimeEpochMs() / 1000L) + ", "
                + "`capture_event`.`status` = " + quote("SUCCESS")
                + ";";
    }

    private static String upsertTableVertex(TableRef tableRef) {
        return "INSERT VERTEX IF NOT EXISTS `table_node`(`normalized_name`, `catalog_name`, `database_name`, `table_name`, `source_type`) VALUES "
                + quoteVid(tableVid(tableRef))
                + ":("
                + quote(tableRef.normalizedName()) + ","
                + nullableQuote(tableRef.getCatalog()) + ","
                + nullableQuote(tableRef.getDatabase()) + ","
                + nullableQuote(tableRef.getName()) + ","
                + nullableQuote(tableRef.getSourceType())
                + ");";
    }

    private static String upsertColumnVertex(ColumnNode columnNode) {
        return "INSERT VERTEX IF NOT EXISTS `column_node`(`column_name`, `owner_id`, `owner_type`, `data_type`, `qualifier`) VALUES "
                + quoteVid(columnNode.getNodeId())
                + ":("
                + nullableQuote(columnNode.getName()) + ","
                + nullableQuote(columnNode.getOwnerId()) + ","
                + nullableQuote(columnNode.getOwnerType()) + ","
                + nullableQuote(columnNode.getDataType()) + ","
                + nullableQuote(String.join(".", columnNode.getQualifier()))
                + ");";
    }

    private static String upsertExpressionVertex(ExpressionNode expressionNode) {
        return "INSERT VERTEX IF NOT EXISTS `expression_node`(`expression_type`, `expression_sql`, `normalized_expression`) VALUES "
                + quoteVid(expressionNode.getNodeId())
                + ":("
                + nullableQuote(expressionNode.getExpressionType()) + ","
                + nullableQuote(expressionNode.getExpressionSql()) + ","
                + nullableQuote(expressionNode.getNormalizedExpression())
                + ");";
    }

    private static String upsertScopeVertex(ScopeNode scopeNode) {
        return "INSERT VERTEX IF NOT EXISTS `scope_node`(`scope_name`, `scope_type`, `parent_scope_id`, `plan_node_name`) VALUES "
                + quoteVid(scopeNode.getNodeId())
                + ":("
                + nullableQuote(scopeNode.getScopeName()) + ","
                + nullableQuote(scopeNode.getScopeType()) + ","
                + nullableQuote(scopeNode.getParentScopeId()) + ","
                + nullableQuote(scopeNode.getPlanNodeName())
                + ");";
    }

    private static String upsertLiteralVertex(LiteralNode literalNode) {
        return "INSERT VERTEX IF NOT EXISTS `literal_node`(`literal_type`, `literal_value`, `normalized_value`) VALUES "
                + quoteVid(literalNode.getNodeId())
                + ":("
                + nullableQuote(literalNode.getLiteralType()) + ","
                + nullableQuote(literalNode.getLiteralValue()) + ","
                + nullableQuote(literalNode.getNormalizedValue())
                + ");";
    }

    private static String upsertOperatorInstanceVertex(OperatorInstanceNode operatorInstanceNode) {
        return "INSERT VERTEX IF NOT EXISTS `operator_instance_node`(`scope_id`, `operator_type`, `operator_sub_type`, `operator_path`, `parent_operator_id`, `plan_node_name`) VALUES "
                + quoteVid(operatorInstanceNode.getNodeId())
                + ":("
                + nullableQuote(operatorInstanceNode.getScopeId()) + ","
                + nullableQuote(operatorInstanceNode.getOperatorType()) + ","
                + nullableQuote(operatorInstanceNode.getOperatorSubType()) + ","
                + nullableQuote(operatorInstanceNode.getOperatorPath()) + ","
                + nullableQuote(operatorInstanceNode.getParentOperatorId()) + ","
                + nullableQuote(operatorInstanceNode.getPlanNodeName())
                + ");";
    }

    private static String upsertColumnInstanceVertex(ColumnInstanceNode columnInstanceNode) {
        return "INSERT VERTEX IF NOT EXISTS `column_instance_node`(`column_id`, `column_name`, `scope_id`, `relation_instance_id`, `instance_type`, `data_type`, `ordinal`) VALUES "
                + quoteVid(columnInstanceNode.getNodeId())
                + ":("
                + nullableQuote(columnInstanceNode.getColumnId()) + ","
                + nullableQuote(columnInstanceNode.getColumnName()) + ","
                + nullableQuote(columnInstanceNode.getScopeId()) + ","
                + nullableQuote(columnInstanceNode.getRelationInstanceId()) + ","
                + nullableQuote(columnInstanceNode.getInstanceType()) + ","
                + nullableQuote(columnInstanceNode.getDataType()) + ","
                + (columnInstanceNode.getOrdinal() == null ? "NULL" : String.valueOf(columnInstanceNode.getOrdinal()))
                + ");";
    }

    private static String upsertRelationInstanceVertex(RelationInstanceNode relationInstanceNode) {
        return "INSERT VERTEX IF NOT EXISTS `relation_instance_node`(`instance_name`, `scope_id`, `source_table_id`, `source_type`, `alias_name`, `plan_node_name`) VALUES "
                + quoteVid(relationInstanceNode.getNodeId())
                + ":("
                + nullableQuote(relationInstanceNode.getInstanceName()) + ","
                + nullableQuote(relationInstanceNode.getScopeId()) + ","
                + nullableQuote(relationInstanceNode.getSourceTableId()) + ","
                + nullableQuote(relationInstanceNode.getSourceType()) + ","
                + nullableQuote(relationInstanceNode.getAliasName()) + ","
                + nullableQuote(relationInstanceNode.getPlanNodeName())
                + ");";
    }

    private static String upsertPredicateVertex(PredicateNode predicateNode) {
        return "INSERT VERTEX IF NOT EXISTS `predicate_node`(`predicate_type`, `predicate_sql`, `normalized_predicate`, `scope_id`, `plan_node_name`) VALUES "
                + quoteVid(predicateNode.getNodeId())
                + ":("
                + nullableQuote(predicateNode.getPredicateType()) + ","
                + nullableQuote(predicateNode.getPredicateSql()) + ","
                + nullableQuote(predicateNode.getNormalizedPredicate()) + ","
                + nullableQuote(predicateNode.getScopeId()) + ","
                + nullableQuote(predicateNode.getPlanNodeName())
                + ");";
    }

    private static String insertEdge(String edgeName, String srcVid, String dstVid, long rank, String... values) {
        return "INSERT EDGE IF NOT EXISTS `" + edgeName + "` VALUES "
                + quoteVid(srcVid)
                + "->"
                + quoteVid(dstVid)
                + "@"
                + rank
                + ":("
                + String.join(",", values)
                + ");";
    }

    private static String upsertEdge(String edgeName, String srcVid, String dstVid, long rank, String... values) {
        return "INSERT EDGE `" + edgeName + "` VALUES "
                + quoteVid(srcVid)
                + "->"
                + quoteVid(dstVid)
                + "@"
                + rank
                + ":("
                + String.join(",", values)
                + ");";
    }

    private static String insertEdgeIfNotExists(String edgeName, String srcVid, String dstVid, long rank, String... values) {
        return insertEdge(edgeName, srcVid, dstVid, rank, values);
    }

    private static List<String> buildSemanticLineageEdges(NormalizedLineageResult result, LineageGraphEdge graphEdge) {
        String eventId = graphEdge.getEventId() == null ? result.getEventId() : graphEdge.getEventId();
        String taskId = inferTaskId(result);
        String runId = inferRunId(result);
        long captureTime = result.getCaptureTimeEpochMs() / 1000L;
        String role = nullableQuote(graphEdge.getRole());

        switch (graphEdge.getEdgeType()) {
            case "SCOPE_TO_SCOPE":
                return structuralEdges(
                        "scope_contains_scope",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "SCOPE_TO_COLUMN":
                return structuralEdges(
                        "scope_outputs_column",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "SCOPE_TO_EXPRESSION":
                return structuralEdges(
                        "scope_uses_expression",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "SCOPE_TO_OPERATOR_INSTANCE":
                return structuralEdges(
                        "scope_contains_operator_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "OPERATOR_TO_OPERATOR_INSTANCE":
                return structuralEdges(
                        "operator_precedes_operator_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "OPERATOR_TO_COLUMN_INSTANCE":
                return structuralEdges(
                        "operator_outputs_column_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "OPERATOR_TO_EXPRESSION":
                return structuralEdges(
                        "operator_uses_expression",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "OPERATOR_TO_PREDICATE":
                return structuralEdges(
                        "operator_uses_predicate",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "OPERATOR_TO_RELATION_INSTANCE":
                return structuralEdges(
                        "operator_reads_relation_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "SCOPE_TO_COLUMN_INSTANCE":
                return structuralEdges(
                        "scope_contains_column_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "SCOPE_TO_RELATION_INSTANCE":
                return structuralEdges(
                        "scope_reads_relation_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "RELATION_INSTANCE_TO_COLUMN_INSTANCE":
                return structuralEdges(
                        "relation_instance_exposes_column_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "RELATION_INSTANCE_TO_TABLE":
                return structuralEdges(
                        "relation_instance_of_table",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "COLUMN_TO_COLUMN_INSTANCE":
                return structuralEdges(
                        "column_has_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "SCOPE_TO_PREDICATE":
                return structuralEdges(
                        "scope_uses_predicate",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "RELATION_INSTANCE_TO_PREDICATE":
                return structuralEdges(
                        "relation_instance_filtered_by_predicate",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        captureTime,
                        role
                );
            case "RELATION_INSTANCE_TO_RELATION_INSTANCE":
                return buildLatestSemanticEdge(
                        "relation_instance_joins_relation_instance",
                        "latest_relation_instance_joins_relation_instance",
                        graphEdge.getSourceNodeId(),
                        graphEdge.getTargetNodeId(),
                        eventId,
                        taskId,
                        runId,
                        captureTime,
                        role,
                        latestSemanticRank("latest_relation_instance_joins_relation_instance", graphEdge.getRole())
                );
            case "COLUMN_TO_COLUMN":
                return mergeStatements(
                        buildDependencyEdges(
                                "column_depends_on_column",
                                "latest_column_depends_on_column",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_depends_on_column", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_column_flows_to_column",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_flows_to_column", graphEdge.getRole())
                        )
                );
            case "COLUMN_TO_PREDICATE":
                return mergeStatements(
                        buildDependencyEdges(
                                "predicate_depends_on_column",
                                "latest_predicate_depends_on_column",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_predicate_depends_on_column", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_column_flows_to_predicate",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_flows_to_predicate", graphEdge.getRole())
                        )
                );
            case "COLUMN_INSTANCE_TO_PREDICATE":
                return mergeStatements(
                        buildDependencyEdges(
                                "predicate_depends_on_column_instance",
                                "latest_predicate_depends_on_column_instance",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_predicate_depends_on_column_instance", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_column_instance_flows_to_predicate",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_instance_flows_to_predicate", graphEdge.getRole())
                        )
                );
            case "RELATION_INSTANCE_TO_DERIVED_COLUMN_INSTANCE":
                return mergeStatements(
                        buildDependencyEdges(
                                "column_instance_depends_on_relation_instance",
                                "latest_column_instance_depends_on_relation_instance",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_instance_depends_on_relation_instance", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_relation_instance_flows_to_column_instance",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_relation_instance_flows_to_column_instance", graphEdge.getRole())
                        )
                );
            case "PREDICATE_TO_DERIVED_COLUMN_INSTANCE":
                return mergeStatements(
                        buildDependencyEdges(
                                "column_instance_filtered_by_predicate",
                                "latest_column_instance_filtered_by_predicate",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_instance_filtered_by_predicate", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_predicate_flows_to_column_instance",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_predicate_flows_to_column_instance", graphEdge.getRole())
                        )
                );
            case "EXPRESSION_TO_COLUMN_INSTANCE":
                return mergeStatements(
                        buildDependencyEdges(
                                "column_instance_uses_expression",
                                "latest_column_instance_uses_expression",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_instance_uses_expression", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_expression_flows_to_column_instance",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_expression_flows_to_column_instance", graphEdge.getRole())
                        )
                );
            case "EXPRESSION_TO_COLUMN":
                return mergeStatements(
                        buildDependencyEdges(
                                "column_uses_expression",
                                "latest_column_uses_expression",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_uses_expression", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_expression_flows_to_column",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_expression_flows_to_column", graphEdge.getRole())
                        )
                );
            case "COLUMN_TO_EXPRESSION":
                return mergeStatements(
                        buildDependencyEdges(
                                "expression_depends_on_column",
                                "latest_expression_depends_on_column",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_expression_depends_on_column", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_column_flows_to_expression",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_flows_to_expression", graphEdge.getRole())
                        )
                );
            case "COLUMN_INSTANCE_TO_EXPRESSION":
                return mergeStatements(
                        buildDependencyEdges(
                                "expression_depends_on_column_instance",
                                "latest_expression_depends_on_column_instance",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_expression_depends_on_column_instance", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_column_instance_flows_to_expression",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_instance_flows_to_expression", graphEdge.getRole())
                        )
                );
            case "COLUMN_INSTANCE_TO_COLUMN_INSTANCE":
                return mergeStatements(
                        buildDependencyEdges(
                                "column_instance_depends_on_column_instance",
                                "latest_column_instance_depends_on_column_instance",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_instance_depends_on_column_instance", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_column_instance_flows_to_column_instance",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_column_instance_flows_to_column_instance", graphEdge.getRole())
                        )
                );
            case "EXPRESSION_TO_EXPRESSION":
                return mergeStatements(
                        buildDependencyEdges(
                                "expression_depends_on_expression",
                                "latest_expression_depends_on_expression",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_expression_depends_on_expression", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_expression_flows_to_expression",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_expression_flows_to_expression", graphEdge.getRole())
                        )
                );
            case "LITERAL_TO_EXPRESSION":
                return mergeStatements(
                        buildDependencyEdges(
                                "expression_depends_on_literal",
                                "latest_expression_depends_on_literal",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_expression_depends_on_literal", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_literal_flows_to_expression",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_literal_flows_to_expression", graphEdge.getRole())
                        )
                );
            case "LITERAL_TO_PREDICATE":
                return mergeStatements(
                        buildDependencyEdges(
                                "predicate_depends_on_literal",
                                "latest_predicate_depends_on_literal",
                                graphEdge.getTargetNodeId(),
                                graphEdge.getSourceNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_predicate_depends_on_literal", graphEdge.getRole())
                        ),
                        buildLatestFlowEdge(
                                "latest_literal_flows_to_predicate",
                                graphEdge.getSourceNodeId(),
                                graphEdge.getTargetNodeId(),
                                eventId,
                                taskId,
                                runId,
                                captureTime,
                                role,
                                latestSemanticRank("latest_literal_flows_to_predicate", graphEdge.getRole())
                        )
                );
            default:
                LOGGER.debug("Skip unsupported lineage graph edge type: {}", graphEdge.getEdgeType());
                return new ArrayList<>();
        }
    }

    private static List<String> structuralEdges(
            String edgeName,
            String srcVid,
            String dstVid,
            String eventId,
            long captureTimeSeconds,
            String role
    ) {
        List<String> statements = new ArrayList<>(1);
        statements.add(insertEdgeIfNotExists(
                edgeName,
                srcVid,
                dstVid,
                edgeRank(edgeName, srcVid, dstVid, role),
                nullableQuote(eventId),
                quoteLong(captureTimeSeconds),
                role
        ));
        return statements;
    }

    private static List<String> buildDependencyEdges(
            String factEdgeName,
            String latestEdgeName,
            String srcVid,
            String dstVid,
            String eventId,
            String taskId,
            String runId,
            long captureTimeSeconds,
            String role,
            long latestRank
    ) {
        List<String> statements = new ArrayList<>();
        statements.add(insertEdge(
                factEdgeName,
                srcVid,
                dstVid,
                edgeRank(eventId, factEdgeName, srcVid, dstVid, role),
                quote(eventId),
                nullableQuote(taskId),
                nullableQuote(runId),
                quoteLong(captureTimeSeconds),
                role
        ));
        statements.addAll(replaceLatestEdge(
                latestEdgeName,
                srcVid,
                dstVid,
                latestRank,
                nullableQuote(eventId),
                nullableQuote(taskId),
                nullableQuote(runId),
                quoteLong(captureTimeSeconds),
                role
        ));
        return statements;
    }

    private static List<String> buildLatestFlowEdge(
            String latestEdgeName,
            String srcVid,
            String dstVid,
            String eventId,
            String taskId,
            String runId,
            long captureTimeSeconds,
            String role,
            long latestRank
    ) {
        return replaceLatestEdge(
                latestEdgeName,
                srcVid,
                dstVid,
                latestRank,
                nullableQuote(eventId),
                nullableQuote(taskId),
                nullableQuote(runId),
                quoteLong(captureTimeSeconds),
                role
        );
    }

    private static List<String> buildLatestSemanticEdge(
            String factEdgeName,
            String latestEdgeName,
            String srcVid,
            String dstVid,
            String eventId,
            String taskId,
            String runId,
            long captureTimeSeconds,
            String role,
            long latestRank
    ) {
        List<String> statements = new ArrayList<>();
        statements.add(insertEdge(
                factEdgeName,
                srcVid,
                dstVid,
                edgeRank(eventId, factEdgeName, srcVid, dstVid, role),
                quote(eventId),
                nullableQuote(taskId),
                nullableQuote(runId),
                quoteLong(captureTimeSeconds),
                role
        ));
        statements.addAll(replaceLatestEdge(
                latestEdgeName,
                srcVid,
                dstVid,
                latestRank,
                nullableQuote(eventId),
                nullableQuote(taskId),
                nullableQuote(runId),
                quoteLong(captureTimeSeconds),
                role
        ));
        return statements;
    }

    private static List<String> mergeStatements(List<String>... statementGroups) {
        List<String> merged = new ArrayList<>();
        for (List<String> statements : statementGroups) {
            merged.addAll(statements);
        }
        return merged;
    }

    private static List<String> replaceLatestEdge(String edgeName, String srcVid, String dstVid, String... values) {
        return replaceLatestEdge(edgeName, srcVid, dstVid, 0L, values);
    }

    private static List<String> replaceLatestEdge(String edgeName, String srcVid, String dstVid, long rank, String... values) {
        List<String> statements = new ArrayList<>(1);
        statements.add(upsertEdge(edgeName, srcVid, dstVid, rank, values));
        return statements;
    }

    private static long latestSemanticRank(String edgeName, String role) {
        return edgeRank(edgeName, role == null ? "" : role);
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

    private static String quoteVid(String value) {
        return quote(value);
    }

    private static String quote(String value) {
        return "\"" + escape(value == null ? "" : value) + "\"";
    }

    private static String nullableQuote(String value) {
        return value == null ? "NULL" : quote(value);
    }

    private static String quoteLong(long value) {
        return String.valueOf(value);
    }

    private static String escape(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    private static long edgeRank(String... values) {
        String digest = HashUtils.sha1(String.join("|", values));
        String hex = digest.substring(0, 15);
        return Long.parseLong(hex, 16);
    }
}
