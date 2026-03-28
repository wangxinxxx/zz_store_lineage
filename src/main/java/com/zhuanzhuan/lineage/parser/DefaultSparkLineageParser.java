package com.zhuanzhuan.lineage.parser;

import com.zhuanzhuan.lineage.common.HashUtils;
import com.zhuanzhuan.lineage.common.ScalaInterop;
import com.zhuanzhuan.lineage.model.ColumnInstanceNode;
import com.zhuanzhuan.lineage.model.ColumnNode;
import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExpressionNode;
import com.zhuanzhuan.lineage.model.LiteralNode;
import com.zhuanzhuan.lineage.model.LineageGraphEdge;
import com.zhuanzhuan.lineage.model.LineageWarning;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.OperatorInstanceNode;
import com.zhuanzhuan.lineage.model.PredicateNode;
import com.zhuanzhuan.lineage.model.RelationInstanceNode;
import com.zhuanzhuan.lineage.model.ScopeNode;
import com.zhuanzhuan.lineage.model.TableLineageEdge;
import com.zhuanzhuan.lineage.model.TableRef;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.WindowExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.catalyst.plans.logical.Union;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public final class DefaultSparkLineageParser implements SparkLineageParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSparkLineageParser.class);

    @Override
    public NormalizedLineageResult parse(ExecutionCaptureEvent event, QueryExecution qe) {
        LogicalPlan logicalPlan = safeGet(qe::logical);
        LogicalPlan analyzedPlan = safeGet(qe::analyzed);
        LogicalPlan optimizedPlan = safeGet(qe::optimizedPlan);

        return parse(event, logicalPlan, analyzedPlan, optimizedPlan);
    }

    public NormalizedLineageResult parse(ExecutionCaptureEvent event, LogicalPlan logicalPlan) {
        return parse(event, logicalPlan, logicalPlan, logicalPlan);
    }

    public NormalizedLineageResult parse(
            ExecutionCaptureEvent event,
            LogicalPlan logicalPlan,
            LogicalPlan analyzedPlan,
            LogicalPlan optimizedPlan
    ) {
        LogicalPlan inputPlan = analyzedPlan != null ? analyzedPlan : logicalPlan;

        List<TableRef> inputTables = deduplicateTables(collectInputTables(inputPlan));
        List<TableRef> outputTables = deduplicateTables(collectOutputTables(logicalPlan));
        if (outputTables.isEmpty()) {
            outputTables = deduplicateTables(collectOutputTables(inputPlan));
        }

        String statementType = detectStatementType(logicalPlan, outputTables);
        GraphBuilder graphBuilder = new GraphBuilder(event.getEventId());
        ResolutionContext resolutionContext = buildResolutionContext(logicalPlan, inputPlan, optimizedPlan);
        String rootScopeId = collectScopeGraph(
                event,
                logicalPlan != null ? logicalPlan : inputPlan,
                statementType,
                outputTables,
                graphBuilder,
                resolutionContext
        );
        String rootOutputOperatorId = null;
        if (rootScopeId != null) {
            LogicalPlan outputPlan = unwrapOutputPlan(logicalPlan != null ? logicalPlan : inputPlan);
            if (outputPlan != null) {
                rootOutputOperatorId = registerOperatorInstance(
                        event,
                        rootScopeId,
                        "root.output",
                        outputPlan,
                        null,
                        graphBuilder
                );
            }
        }

        List<TargetExpression> targets = selectBestTargetExpressions(logicalPlan, analyzedPlan, optimizedPlan, resolutionContext);
        for (int i = 0; i < targets.size(); i++) {
            TargetExpression targetExpression = targets.get(i);
            ColumnNode targetColumn = buildTargetColumn(event, outputTables, targetExpression, i);
            graphBuilder.addColumn(targetColumn);
            List<ColumnInstanceNode> outputColumnInstances = Collections.emptyList();
            if (rootScopeId != null) {
                graphBuilder.addEdge(new LineageGraphEdge(
                        rootScopeId,
                        targetColumn.getNodeId(),
                        "SCOPE_TO_COLUMN",
                        "final_output",
                        event.getEventId()
                ));
                outputColumnInstances = createColumnInstances(
                        event,
                        rootScopeId,
                        targetColumn,
                        "OUTPUT",
                        "final_output",
                        i,
                        graphBuilder
                );
                connectOperatorToOutputInstances(event, rootOutputOperatorId, outputColumnInstances, graphBuilder);
            }
            attachExpression(
                    event,
                    targetExpression,
                    targetColumn.getNodeId(),
                    SinkKind.COLUMN,
                    "value",
                    "out." + i,
                    rootScopeId,
                    rootOutputOperatorId,
                    outputColumnInstances,
                    graphBuilder,
                    resolutionContext
            );
        }
        materializeScopeOutputReferenceEdges(event, graphBuilder);
        materializeDerivedOutputAttributionEdges(event, graphBuilder);

        List<LineageWarning> warnings = new ArrayList<>();
        if (inputTables.isEmpty()) {
            warnings.add(new LineageWarning("NO_INPUT_TABLE", "No physical input tables were resolved from the analyzed plan."));
        }
        if (outputTables.isEmpty()) {
            warnings.add(new LineageWarning("NO_OUTPUT_TABLE", "No target tables were resolved from the logical or analyzed plan."));
        }

        List<TableLineageEdge> tableEdges = new ArrayList<>();
        for (TableRef source : inputTables) {
            for (TableRef target : outputTables) {
                if (!source.normalizedName().equals(target.normalizedName())) {
                    tableEdges.add(new TableLineageEdge(
                            source,
                            target,
                            event.getTaskContext().getTaskId(),
                            event.getTaskContext().getRunId(),
                            event.getEventId(),
                            writeModeFromStatement(statementType),
                            event.getCaptureTimeEpochMs(),
                            "HIGH"
                    ));
                }
            }
        }

        return new NormalizedLineageResult(
                event.getEventId(),
                event.getCaptureTimeEpochMs(),
                statementType,
                inputTables,
                outputTables,
                tableEdges,
                graphBuilder.columns(),
                graphBuilder.expressions(),
                graphBuilder.scopes(),
                graphBuilder.literals(),
                graphBuilder.operatorInstances(),
                graphBuilder.columnInstances(),
                graphBuilder.relationInstances(),
                graphBuilder.predicates(),
                graphBuilder.edges(),
                warnings
        );
    }

    private void materializeScopeOutputReferenceEdges(
            ExecutionCaptureEvent event,
            GraphBuilder builder
    ) {
        for (ColumnInstanceNode consumerInstance : builder.columnInstances()) {
            if ("OUTPUT".equals(consumerInstance.getInstanceType())) {
                continue;
            }
            if (consumerInstance.getRelationInstanceId() != null) {
                continue;
            }
            ColumnNode referencedColumn = builder.findColumn(consumerInstance.getColumnId());
            if (referencedColumn == null || !"SCOPE".equals(referencedColumn.getOwnerType())) {
                continue;
            }
            String ownerScopeId = referencedColumn.getOwnerId();
            if (ownerScopeId == null || ownerScopeId.isEmpty()) {
                continue;
            }
            if (!builder.isScopeSameOrDescendant(consumerInstance.getScopeId(), ownerScopeId)) {
                continue;
            }
            for (ColumnInstanceNode producerInstance : builder.findColumnInstancesByColumnId(referencedColumn.getNodeId())) {
                if (!"OUTPUT".equals(producerInstance.getInstanceType())) {
                    continue;
                }
                if (!ownerScopeId.equals(producerInstance.getScopeId())) {
                    continue;
                }
                if (producerInstance.getNodeId().equals(consumerInstance.getNodeId())) {
                    continue;
                }
                builder.addEdge(new LineageGraphEdge(
                        producerInstance.getNodeId(),
                        consumerInstance.getNodeId(),
                        "COLUMN_INSTANCE_TO_COLUMN_INSTANCE",
                        "scope_output_reference",
                        event.getEventId()
                ));
            }
        }
    }

    private void materializeDerivedOutputAttributionEdges(
            ExecutionCaptureEvent event,
            GraphBuilder builder
    ) {
        for (ColumnInstanceNode outputInstance : builder.columnInstances()) {
            if (!"OUTPUT".equals(outputInstance.getInstanceType())) {
                continue;
            }
            DerivedAttribution attribution = collectDerivedAttribution(outputInstance, builder);
            for (Map.Entry<String, String> relationAttribution : attribution.relationInstanceRoles.entrySet()) {
                builder.addEdge(new LineageGraphEdge(
                        relationAttribution.getKey(),
                        outputInstance.getNodeId(),
                        "RELATION_INSTANCE_TO_DERIVED_COLUMN_INSTANCE",
                        relationAttribution.getValue(),
                        event.getEventId()
                ));
            }
            for (Map.Entry<String, String> predicateAttribution : attribution.predicateRoles.entrySet()) {
                builder.addEdge(new LineageGraphEdge(
                        predicateAttribution.getKey(),
                        outputInstance.getNodeId(),
                        "PREDICATE_TO_DERIVED_COLUMN_INSTANCE",
                        predicateAttribution.getValue(),
                        event.getEventId()
                ));
            }
        }
    }

    private DerivedAttribution collectDerivedAttribution(ColumnInstanceNode outputInstance, GraphBuilder builder) {
        LinkedHashMap<String, String> relationInstanceRoles = new LinkedHashMap<>();
        LinkedHashMap<String, String> predicateRoles = new LinkedHashMap<>();
        collectUpstreamColumnInstanceAttribution(
                outputInstance.getNodeId(),
                builder,
                new HashSet<String>(),
                new HashSet<String>(),
                relationInstanceRoles,
                predicateRoles
        );
        return new DerivedAttribution(relationInstanceRoles, predicateRoles);
    }

    private void collectUpstreamColumnInstanceAttribution(
            String columnInstanceId,
            GraphBuilder builder,
            Set<String> visitedColumnInstances,
            Set<String> visitedExpressions,
            Map<String, String> relationInstanceRoles,
            Map<String, String> predicateRoles
    ) {
        if (!visitedColumnInstances.add(columnInstanceId)) {
            return;
        }
        ColumnInstanceNode columnInstanceNode = builder.findColumnInstance(columnInstanceId);
        if (columnInstanceNode == null) {
            return;
        }
        collectRelationAttribution(columnInstanceNode, builder, relationInstanceRoles, predicateRoles);

        for (LineageGraphEdge edge : builder.incomingEdges(columnInstanceId)) {
            if ("COLUMN_INSTANCE_TO_COLUMN_INSTANCE".equals(edge.getEdgeType())) {
                collectUpstreamColumnInstanceAttribution(
                        edge.getSourceNodeId(),
                        builder,
                        visitedColumnInstances,
                        visitedExpressions,
                        relationInstanceRoles,
                        predicateRoles
                );
            } else if ("EXPRESSION_TO_COLUMN_INSTANCE".equals(edge.getEdgeType())) {
                collectUpstreamExpressionAttribution(
                        edge.getSourceNodeId(),
                        builder,
                        visitedColumnInstances,
                        visitedExpressions,
                        relationInstanceRoles,
                        predicateRoles
                );
            }
        }
    }

    private void collectUpstreamExpressionAttribution(
            String expressionNodeId,
            GraphBuilder builder,
            Set<String> visitedColumnInstances,
            Set<String> visitedExpressions,
            Map<String, String> relationInstanceRoles,
            Map<String, String> predicateRoles
    ) {
        if (!visitedExpressions.add(expressionNodeId)) {
            return;
        }
        for (LineageGraphEdge edge : builder.incomingEdges(expressionNodeId)) {
            if ("COLUMN_INSTANCE_TO_EXPRESSION".equals(edge.getEdgeType())) {
                collectUpstreamColumnInstanceAttribution(
                        edge.getSourceNodeId(),
                        builder,
                        visitedColumnInstances,
                        visitedExpressions,
                        relationInstanceRoles,
                        predicateRoles
                );
            } else if ("EXPRESSION_TO_EXPRESSION".equals(edge.getEdgeType())) {
                collectUpstreamExpressionAttribution(
                        edge.getSourceNodeId(),
                        builder,
                        visitedColumnInstances,
                        visitedExpressions,
                        relationInstanceRoles,
                        predicateRoles
                );
            }
        }
    }

    private void collectRelationAttribution(
            ColumnInstanceNode columnInstanceNode,
            GraphBuilder builder,
            Map<String, String> relationInstanceRoles,
            Map<String, String> predicateRoles
    ) {
        String relationInstanceId = columnInstanceNode.getRelationInstanceId();
        if (relationInstanceId == null || relationInstanceId.isEmpty()) {
            return;
        }
        relationInstanceRoles.put(relationInstanceId, "source_relation_instance");
        for (LineageGraphEdge edge : builder.outgoingEdges(relationInstanceId)) {
            if ("RELATION_INSTANCE_TO_PREDICATE".equals(edge.getEdgeType())) {
                predicateRoles.put(edge.getTargetNodeId(), normalizePredicateRole(edge.getRole()));
            }
        }
    }

    private String collectScopeGraph(
            ExecutionCaptureEvent event,
            LogicalPlan plan,
            String statementType,
            List<TableRef> outputTables,
            GraphBuilder builder,
            ResolutionContext resolutionContext
    ) {
        if (plan == null) {
            return null;
        }
        String scopeName = outputTables.isEmpty()
                ? normalizeIdentifierToken(statementType == null ? "query" : statementType)
                : outputTables.get(0).normalizedName();
        String rootScopeId = scopeNodeId(event.getEventId(), "ROOT_QUERY", "root", scopeName);
        builder.addScope(new ScopeNode(
                rootScopeId,
                scopeName,
                "ROOT_QUERY",
                null,
                plan.nodeName()
        ));
        collectScopeContext(event, plan, rootScopeId, "root", null, builder, resolutionContext);
        collectNestedScopes(event, plan, rootScopeId, "root", builder, resolutionContext, new HashSet<String>());
        return rootScopeId;
    }

    private void collectNestedScopes(
            ExecutionCaptureEvent event,
            LogicalPlan plan,
            String parentScopeId,
            String path,
            GraphBuilder builder,
            ResolutionContext resolutionContext,
            Set<String> visited
    ) {
        if (plan == null) {
            return;
        }
        String visitKey = path + "|" + plan.nodeName() + "|" + System.identityHashCode(plan);
        if (!visited.add(visitKey)) {
            return;
        }
        try {
            if (isWithWrapper(plan)) {
                int cteIndex = 0;
                for (Tuple2<String, LogicalPlan> cteRelation : extractCteRelations(plan)) {
                    String cteName = normalizeIdentifierToken(cteRelation._1());
                    String scopePath = path + ".cte." + cteIndex + "." + cteName;
                    String scopeId = scopeNodeId(event.getEventId(), "CTE", scopePath, cteName);
                    builder.addScope(new ScopeNode(scopeId, cteName, "CTE", parentScopeId, cteRelation._2().nodeName()));
                    builder.addEdge(new LineageGraphEdge(parentScopeId, scopeId, "SCOPE_TO_SCOPE", "contains", event.getEventId()));
                    collectScopeContext(event, cteRelation._2(), scopeId, scopePath, null, builder, resolutionContext);
                    attachScopeOutputs(event, cteRelation._2(), cteName, scopeId, scopePath, builder, resolutionContext);
                    collectNestedScopes(event, cteRelation._2(), scopeId, scopePath, builder, resolutionContext, visited);
                    cteIndex++;
                }
                List<LogicalPlan> children = ScalaInterop.toJavaList(plan.children());
                if (!children.isEmpty()) {
                    collectNestedScopes(event, children.get(0), parentScopeId, path + ".query", builder, resolutionContext, visited);
                }
                return;
            }
            if (plan instanceof SubqueryAlias) {
                SubqueryAlias alias = (SubqueryAlias) plan;
                String aliasName = normalizeIdentifierToken(alias.alias());
                String scopePath = path + ".alias." + aliasName;
                String scopeId = scopeNodeId(event.getEventId(), "SUBQUERY_ALIAS", scopePath, aliasName);
                builder.addScope(new ScopeNode(scopeId, aliasName, "SUBQUERY_ALIAS", parentScopeId, alias.child().nodeName()));
                builder.addEdge(new LineageGraphEdge(parentScopeId, scopeId, "SCOPE_TO_SCOPE", "contains", event.getEventId()));
                collectScopeContext(event, alias.child(), scopeId, scopePath, null, builder, resolutionContext);
                attachScopeOutputs(event, alias.child(), aliasName, scopeId, scopePath, builder, resolutionContext);
                collectNestedScopes(event, alias.child(), scopeId, scopePath, builder, resolutionContext, visited);
                return;
            }
            if (plan instanceof Union) {
                List<LogicalPlan> unionChildren = ScalaInterop.toJavaList(plan.children());
                for (int i = 0; i < unionChildren.size(); i++) {
                    LogicalPlan child = unionChildren.get(i);
                    String branchName = "branch_" + i;
                    String scopePath = path + ".union." + i;
                    String scopeId = scopeNodeId(event.getEventId(), "UNION_BRANCH", scopePath, branchName);
                    builder.addScope(new ScopeNode(scopeId, branchName, "UNION_BRANCH", parentScopeId, child.nodeName()));
                    builder.addEdge(new LineageGraphEdge(parentScopeId, scopeId, "SCOPE_TO_SCOPE", "contains", event.getEventId()));
                    collectScopeContext(event, child, scopeId, scopePath, null, builder, resolutionContext);
                    attachScopeOutputs(event, child, scopeId, scopeId, scopePath, builder, resolutionContext);
                    collectNestedScopes(event, child, scopeId, scopePath, builder, resolutionContext, visited);
                }
                return;
            }
            for (LogicalPlan child : ScalaInterop.toJavaList(plan.children())) {
                collectNestedScopes(event, child, parentScopeId, path + "." + normalizeIdentifierToken(child.nodeName()), builder, resolutionContext, visited);
            }
        } finally {
            visited.remove(visitKey);
        }
    }

    private void attachScopeOutputs(
            ExecutionCaptureEvent event,
            LogicalPlan scopePlan,
            String ownerId,
            String scopeNodeId,
            String path,
            GraphBuilder builder,
            ResolutionContext resolutionContext
    ) {
        String operatorInstanceId = registerOperatorInstance(event, scopeNodeId, path, scopePlan, null, builder);
        List<TargetExpression> outputs = tryCollectTargetExpressions(scopePlan, resolutionContext);
        for (int i = 0; i < outputs.size(); i++) {
            TargetExpression output = outputs.get(i);
            ColumnNode scopedOutput = buildScopeOutputColumn(ownerId, output, i);
            builder.addColumn(scopedOutput);
            builder.addEdge(new LineageGraphEdge(scopeNodeId, scopedOutput.getNodeId(), "SCOPE_TO_COLUMN", "scope_output", event.getEventId()));
            List<ColumnInstanceNode> outputColumnInstances = createColumnInstances(
                    event,
                    scopeNodeId,
                    scopedOutput,
                    "OUTPUT",
                    "scope_output",
                    i,
                    builder
            );
            connectOperatorToOutputInstances(event, operatorInstanceId, outputColumnInstances, builder);
            attachExpression(
                    event,
                    output,
                    scopedOutput.getNodeId(),
                    SinkKind.COLUMN,
                    "scope_output",
                    path + ".out." + i,
                    scopeNodeId,
                    operatorInstanceId,
                    outputColumnInstances,
                    builder,
                    resolutionContext
            );
        }
    }

    private ColumnNode buildScopeOutputColumn(String ownerId, TargetExpression targetExpression, int index) {
        return new ColumnNode(
                ownerId + "." + targetExpression.getName(),
                ownerId,
                "SCOPE",
                targetExpression.getName(),
                targetExpression.getDataType(),
                Collections.singletonList(String.valueOf(index))
        );
    }

    private void collectScopeContext(
            ExecutionCaptureEvent event,
            LogicalPlan scopePlan,
            String scopeNodeId,
            String path,
            String parentOperatorId,
            GraphBuilder builder,
            ResolutionContext resolutionContext
    ) {
        collectScopeContext(event, scopePlan, scopeNodeId, path, parentOperatorId, builder, resolutionContext, new HashSet<String>());
    }

    private List<RelationInstanceNode> collectScopeContext(
            ExecutionCaptureEvent event,
            LogicalPlan scopePlan,
            String scopeNodeId,
            String path,
            String parentOperatorId,
            GraphBuilder builder,
            ResolutionContext resolutionContext,
            Set<String> visited
    ) {
        if (scopePlan == null) {
            return Collections.emptyList();
        }

        String visitKey = scopeNodeId + "|" + path + "|" + System.identityHashCode(scopePlan);
        if (!visited.add(visitKey)) {
            return Collections.emptyList();
        }
        try {
            if (isWithWrapper(scopePlan)) {
                List<LogicalPlan> children = ScalaInterop.toJavaList(scopePlan.children());
                if (children.isEmpty()) {
                    return Collections.emptyList();
                }
                return collectScopeContext(event, children.get(0), scopeNodeId, path + ".query", parentOperatorId, builder, resolutionContext, visited);
            }
            if (scopePlan instanceof SubqueryAlias) {
                SubqueryAlias alias = (SubqueryAlias) scopePlan;
                Optional<TableRef> aliasedTable = extractLocalPhysicalTableRef(alias.child());
                if (aliasedTable.isPresent()) {
                    return Collections.singletonList(registerRelationInstance(
                            event,
                            scopeNodeId,
                            path,
                            aliasedTable.get(),
                            normalizeIdentifierToken(alias.alias()),
                            scopePlan.nodeName(),
                            builder
                    ));
                }
                return collectScopeContext(
                        event,
                        alias.child(),
                        scopeNodeId,
                        path + ".alias." + normalizeIdentifierToken(alias.alias()),
                        parentOperatorId,
                        builder,
                        resolutionContext,
                        visited
                );
            }
            if (scopePlan instanceof Union) {
                return Collections.emptyList();
            }
            if (scopePlan instanceof InsertIntoStatement) {
                return collectScopeContext(event, ((InsertIntoStatement) scopePlan).query(), scopeNodeId, path + ".query", parentOperatorId, builder, resolutionContext, visited);
            }
            String operatorInstanceId = registerOperatorInstance(event, scopeNodeId, path, scopePlan, parentOperatorId, builder);
            if (scopePlan instanceof UnresolvedRelation) {
                UnresolvedRelation unresolvedRelation = (UnresolvedRelation) scopePlan;
                Optional<LogicalPlan> namedPlan = resolveNamedRelationPlan(unresolvedRelation, resolutionContext);
                if (namedPlan.isPresent()) {
                    List<String> identifierParts = toStringList(unresolvedRelation.multipartIdentifier());
                    String relationKey = resolutionContext.normalizePlanKey(identifierParts);
                    if (!resolutionContext.enterNamedPlan(relationKey)) {
                        return Collections.emptyList();
                    }
                    try {
                        RelationInstanceNode relationInstanceNode = registerNamedRelationInstance(
                                event,
                                scopeNodeId,
                                path,
                                namedRelationName(identifierParts),
                                namedPlan.get().nodeName(),
                                builder
                        );
                        connectOperatorToRelations(
                                event,
                                operatorInstanceId,
                                Collections.singletonList(relationInstanceNode),
                                "reads_named_relation",
                                builder
                        );
                        collectScopeContext(
                                event,
                                namedPlan.get(),
                                scopeNodeId,
                                path + ".named." + relationKey,
                                operatorInstanceId,
                                builder,
                                resolutionContext,
                                visited
                        );
                        return Collections.singletonList(relationInstanceNode);
                    } finally {
                        resolutionContext.exitNamedPlan(relationKey);
                    }
                }
            }
            if (scopePlan instanceof Filter) {
                Filter filter = (Filter) scopePlan;
                List<RelationInstanceNode> childRelations = collectScopeContext(
                        event,
                        filter.child(),
                        scopeNodeId,
                        path + ".filter",
                        operatorInstanceId,
                        builder,
                        resolutionContext,
                        visited
                );
                connectOperatorToRelations(event, operatorInstanceId, childRelations, "filter_input", builder);
                attachPredicate(
                        event,
                        filter.condition(),
                        "FILTER",
                        scopePlan.nodeName(),
                        scopeNodeId,
                        operatorInstanceId,
                        childRelations,
                        path + ".filter.predicate",
                        builder,
                        resolutionContext
                );
                return childRelations;
            }
            if (scopePlan instanceof Join) {
                Join join = (Join) scopePlan;
                List<LogicalPlan> children = ScalaInterop.toJavaList(join.children());
                List<List<RelationInstanceNode>> relationGroups = new ArrayList<List<RelationInstanceNode>>();
                LinkedHashMap<String, RelationInstanceNode> relations = new LinkedHashMap<String, RelationInstanceNode>();
                for (int i = 0; i < children.size(); i++) {
                    List<RelationInstanceNode> childRelations = collectScopeContext(
                            event,
                            children.get(i),
                            scopeNodeId,
                            path + ".join." + i,
                            operatorInstanceId,
                            builder,
                            resolutionContext,
                            visited
                    );
                    relationGroups.add(childRelations);
                    connectOperatorToRelations(event, operatorInstanceId, childRelations, joinInputRole(i, children.size()), builder);
                    for (RelationInstanceNode relationInstanceNode : childRelations) {
                        relations.put(relationInstanceNode.getNodeId(), relationInstanceNode);
                    }
                }
                connectJoinRelations(event, join, relationGroups, builder);
                if (join.condition().isDefined()) {
                    attachPredicate(
                            event,
                            join.condition().get(),
                            "JOIN",
                            scopePlan.nodeName(),
                            scopeNodeId,
                            operatorInstanceId,
                            new ArrayList<>(relations.values()),
                            path + ".join.condition",
                            builder,
                            resolutionContext
                    );
                }
                return new ArrayList<>(relations.values());
            }
            if (scopePlan instanceof Aggregate) {
                Aggregate aggregate = (Aggregate) scopePlan;
                List<RelationInstanceNode> childRelations = collectScopeContext(
                        event,
                        aggregate.child(),
                        scopeNodeId,
                        path + ".aggregate",
                        operatorInstanceId,
                        builder,
                        resolutionContext,
                        visited
                );
                connectOperatorToRelations(event, operatorInstanceId, childRelations, "aggregate_input", builder);
                int groupingIndex = 0;
                for (Expression groupingExpression : ScalaInterop.toJavaList(aggregate.groupingExpressions())) {
                    attachOperatorContextExpression(
                            event,
                            groupingExpression,
                            scopeNodeId,
                            operatorInstanceId,
                            "group_key",
                            path + ".aggregate.group." + groupingIndex,
                            builder,
                            resolutionContext
                    );
                    groupingIndex++;
                }
                int aggregateIndex = 0;
                for (NamedExpression aggregateExpression : ScalaInterop.toJavaList(aggregate.aggregateExpressions())) {
                    attachOperatorContextExpression(
                            event,
                            asExpression(aggregateExpression),
                            scopeNodeId,
                            operatorInstanceId,
                            "aggregate_expression",
                            path + ".aggregate.expr." + aggregateIndex,
                            builder,
                            resolutionContext
                    );
                    aggregateIndex++;
                }
                return childRelations;
            }

            Optional<TableRef> localTableRef = extractLocalPhysicalTableRef(scopePlan);
            if (localTableRef.isPresent()) {
                RelationInstanceNode relationInstanceNode = registerRelationInstance(
                        event,
                        scopeNodeId,
                        path,
                        localTableRef.get(),
                        null,
                        scopePlan.nodeName(),
                        builder
                );
                connectOperatorToRelations(
                        event,
                        operatorInstanceId,
                        Collections.singletonList(relationInstanceNode),
                        "reads_relation",
                        builder
                );
                return Collections.singletonList(relationInstanceNode);
            }

            LinkedHashMap<String, RelationInstanceNode> relations = new LinkedHashMap<>();
            for (LogicalPlan child : ScalaInterop.toJavaList(scopePlan.children())) {
                for (RelationInstanceNode relationInstanceNode : collectScopeContext(
                        event,
                        child,
                        scopeNodeId,
                        path + "." + normalizeIdentifierToken(child.nodeName()),
                        operatorInstanceId,
                        builder,
                        resolutionContext,
                        visited
                )) {
                    relations.put(relationInstanceNode.getNodeId(), relationInstanceNode);
                }
            }
            connectOperatorToRelations(event, operatorInstanceId, new ArrayList<>(relations.values()), "input", builder);
            return new ArrayList<>(relations.values());
        } finally {
            visited.remove(visitKey);
        }
    }

    private void attachOperatorContextExpression(
            ExecutionCaptureEvent event,
            Expression expression,
            String scopeNodeId,
            String operatorInstanceId,
            String role,
            String path,
            GraphBuilder builder,
            ResolutionContext resolutionContext
    ) {
        if (expression == null || operatorInstanceId == null) {
            return;
        }
        ExpressionNode expressionNode = new ExpressionNode(
                expressionNodeId(operatorInstanceId + "|" + role + "|" + path, expression),
                expression.prettyName(),
                safeExpressionSql(expression),
                normalizeExpression(expression)
        );
        builder.addExpression(expressionNode);
        if (scopeNodeId != null) {
            builder.addEdge(new LineageGraphEdge(
                    scopeNodeId,
                    expressionNode.getNodeId(),
                    "SCOPE_TO_EXPRESSION",
                    "scope_expression",
                    event.getEventId()
            ));
        }
        builder.addEdge(new LineageGraphEdge(
                operatorInstanceId,
                expressionNode.getNodeId(),
                "OPERATOR_TO_EXPRESSION",
                role,
                event.getEventId()
        ));

        LinkedHashMap<String, ColumnNode> sourceColumns = new LinkedHashMap<>();
        collectSourceColumns(expression, sourceColumns, resolutionContext);
        for (ColumnNode sourceColumn : sourceColumns.values()) {
            builder.addColumn(sourceColumn);
            builder.addEdge(new LineageGraphEdge(
                    sourceColumn.getNodeId(),
                    expressionNode.getNodeId(),
                    "COLUMN_TO_EXPRESSION",
                    "source_column",
                    event.getEventId()
            ));
            if (scopeNodeId != null) {
                for (ColumnInstanceNode columnInstanceNode : createColumnInstances(
                        event,
                        scopeNodeId,
                        sourceColumn,
                        "EXPRESSION_INPUT",
                        "expression_input",
                        null,
                        builder
                )) {
                    builder.addEdge(new LineageGraphEdge(
                            columnInstanceNode.getNodeId(),
                            expressionNode.getNodeId(),
                            "COLUMN_INSTANCE_TO_EXPRESSION",
                            "source_column_instance",
                            event.getEventId()
                    ));
                }
            }
        }
        collectLiteralNodes(expression, expressionNode.getNodeId(), builder, event, "LITERAL_TO_EXPRESSION", "literal");
    }

    private void attachPredicate(
            ExecutionCaptureEvent event,
            Expression predicateExpression,
            String predicateType,
            String planNodeName,
            String scopeNodeId,
            String operatorInstanceId,
            List<RelationInstanceNode> relationInstances,
            String path,
            GraphBuilder builder,
            ResolutionContext resolutionContext
    ) {
        if (predicateExpression == null) {
            return;
        }
        String predicateRole = normalizePredicateRole(predicateType);
        PredicateNode predicateNode = new PredicateNode(
                predicateNodeId(scopeNodeId, path, predicateType, predicateExpression),
                predicateType,
                safeExpressionSql(predicateExpression),
                normalizeExpression(predicateExpression),
                scopeNodeId,
                planNodeName
        );
        builder.addPredicate(predicateNode);
        builder.addEdge(new LineageGraphEdge(
                scopeNodeId,
                predicateNode.getNodeId(),
                "SCOPE_TO_PREDICATE",
                predicateRole,
                event.getEventId()
        ));
        if (operatorInstanceId != null) {
            builder.addEdge(new LineageGraphEdge(
                    operatorInstanceId,
                    predicateNode.getNodeId(),
                    "OPERATOR_TO_PREDICATE",
                    predicateRole,
                    event.getEventId()
            ));
        }

        for (RelationInstanceNode relationInstanceNode : relationInstances) {
            builder.addEdge(new LineageGraphEdge(
                    relationInstanceNode.getNodeId(),
                    predicateNode.getNodeId(),
                    "RELATION_INSTANCE_TO_PREDICATE",
                    predicateRole,
                    event.getEventId()
            ));
        }

        LinkedHashMap<String, ColumnNode> predicateColumns = new LinkedHashMap<>();
        collectSourceColumns(predicateExpression, predicateColumns, resolutionContext);
        for (ColumnNode predicateColumn : predicateColumns.values()) {
            builder.addColumn(predicateColumn);
            builder.addEdge(new LineageGraphEdge(
                    predicateColumn.getNodeId(),
                    predicateNode.getNodeId(),
                    "COLUMN_TO_PREDICATE",
                    "predicate_column",
                    event.getEventId()
            ));
            for (ColumnInstanceNode columnInstanceNode : createColumnInstances(
                    event,
                    scopeNodeId,
                    predicateColumn,
                    "PREDICATE_INPUT",
                    "predicate_input",
                    null,
                    builder
            )) {
                builder.addEdge(new LineageGraphEdge(
                        columnInstanceNode.getNodeId(),
                        predicateNode.getNodeId(),
                        "COLUMN_INSTANCE_TO_PREDICATE",
                        "predicate_column_instance",
                        event.getEventId()
                ));
            }
        }

        collectLiteralNodes(predicateExpression, predicateNode.getNodeId(), builder, event, "LITERAL_TO_PREDICATE", "literal");
    }

    private LogicalPlan safeGet(Supplier<LogicalPlan> supplier) {
        try {
            return supplier.get();
        } catch (Exception error) {
            LOGGER.debug("Failed to resolve logical plan snapshot for lineage parsing.", error);
            return null;
        }
    }

    private List<TargetExpression> selectBestTargetExpressions(
            LogicalPlan logicalPlan,
            LogicalPlan analyzedPlan,
            LogicalPlan optimizedPlan,
            ResolutionContext resolutionContext
    ) {
        List<List<TargetExpression>> candidates = new ArrayList<>();
        addCandidate(candidates, tryCollectTargetExpressions(logicalPlan, resolutionContext));
        addCandidate(candidates, tryCollectTargetExpressions(analyzedPlan, resolutionContext));
        addCandidate(candidates, tryCollectTargetExpressions(optimizedPlan, resolutionContext));
        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        List<TargetExpression> best = candidates.get(0);
        int bestScore = scoreTargetExpressions(best);
        for (int i = 1; i < candidates.size(); i++) {
            List<TargetExpression> candidate = candidates.get(i);
            int score = scoreTargetExpressions(candidate);
            if (score > bestScore) {
                best = candidate;
                bestScore = score;
            }
        }
        return best;
    }

    private List<TargetExpression> tryCollectTargetExpressions(LogicalPlan plan, ResolutionContext resolutionContext) {
        if (plan == null) {
            return Collections.emptyList();
        }
        try {
            return collectTargetExpressions(plan, resolutionContext);
        } catch (Exception error) {
            LOGGER.debug("Skip unresolved target expressions for plan={}", plan.nodeName(), error);
            return Collections.emptyList();
        }
    }

    private void addCandidate(List<List<TargetExpression>> candidates, List<TargetExpression> candidate) {
        if (!candidate.isEmpty()) {
            candidates.add(candidate);
        }
    }

    private int scoreTargetExpressions(List<TargetExpression> targets) {
        int score = 0;
        for (TargetExpression target : targets) {
            Expression expression = target.getExpression();
            if (!(expression instanceof AttributeReference)) {
                score += 10;
                continue;
            }
            List<String> qualifier = toStringList(((AttributeReference) expression).qualifier());
            score += qualifier.isEmpty() ? 0 : 2;
        }
        return score;
    }

    private ResolutionContext buildResolutionContext(LogicalPlan logicalPlan, LogicalPlan analyzedPlan, LogicalPlan optimizedPlan) {
        LinkedHashMap<String, TableRef> aliasToTable = new LinkedHashMap<>();
        LinkedHashMap<String, LogicalPlan> namedPlans = new LinkedHashMap<>();
        collectAliasMappings(logicalPlan, aliasToTable);
        collectAliasMappings(analyzedPlan, aliasToTable);
        collectAliasMappings(optimizedPlan, aliasToTable);
        collectNamedPlans(logicalPlan, namedPlans);
        collectNamedPlans(analyzedPlan, namedPlans);
        collectNamedPlans(optimizedPlan, namedPlans);
        return new ResolutionContext(aliasToTable, namedPlans);
    }

    private void collectAliasMappings(LogicalPlan plan, Map<String, TableRef> aliasToTable) {
        if (plan == null) {
            return;
        }
        if (plan instanceof SubqueryAlias) {
            SubqueryAlias alias = (SubqueryAlias) plan;
            Optional<TableRef> tableRef = extractBaseTableRef(alias.child());
            if (tableRef.isPresent()) {
                aliasToTable.put(alias.alias(), tableRef.get());
            }
        }
        for (LogicalPlan child : ScalaInterop.toJavaList(plan.children())) {
            collectAliasMappings(child, aliasToTable);
        }
    }

    private Optional<TableRef> extractBaseTableRef(LogicalPlan plan) {
        if (plan == null) {
            return Optional.empty();
        }
        if (plan instanceof UnresolvedRelation) {
            List<String> parts = toStringList(((UnresolvedRelation) plan).multipartIdentifier());
            if (parts.isEmpty() || "cte".equalsIgnoreCase(resolveRelationKind(parts))) {
                return Optional.empty();
            }
            return Optional.of(TableRef.fromParts(parts));
        }
        Optional<TableRef> direct = extractTableRefFromPlan(plan);
        if (direct.isPresent() && !"subquery".equalsIgnoreCase(direct.get().getDatabase())) {
            return direct;
        }
        List<LogicalPlan> children = ScalaInterop.toJavaList(plan.children());
        if (children.size() == 1) {
            return extractBaseTableRef(children.get(0));
        }
        return Optional.empty();
    }

    private boolean shouldDescend(LogicalPlan plan) {
        String nodeName = plan.nodeName();
        return ScalaInterop.toJavaList(plan.children()).size() == 1
                && ("Filter".equals(nodeName)
                || "Sort".equals(nodeName)
                || "GlobalLimit".equals(nodeName)
                || "LocalLimit".equals(nodeName)
                || "Distinct".equals(nodeName)
                || "Repartition".equals(nodeName)
                || "RepartitionByExpression".equals(nodeName));
    }

    private void collectNamedPlans(LogicalPlan plan, Map<String, LogicalPlan> namedPlans) {
        if (plan == null) {
            return;
        }
        if (isWithWrapper(plan)) {
            for (Tuple2<String, LogicalPlan> cteRelation : extractCteRelations(plan)) {
                registerNamedPlan(namedPlans, cteRelation._1(), cteRelation._2());
                collectNamedPlans(cteRelation._2(), namedPlans);
            }
        }
        if (plan instanceof SubqueryAlias) {
            SubqueryAlias alias = (SubqueryAlias) plan;
            registerNamedPlan(namedPlans, alias.alias(), alias.child());
        }
        for (LogicalPlan child : ScalaInterop.toJavaList(plan.children())) {
            collectNamedPlans(child, namedPlans);
        }
    }

    private void registerNamedPlan(Map<String, LogicalPlan> namedPlans, String name, LogicalPlan plan) {
        if (name == null || name.trim().isEmpty() || plan == null) {
            return;
        }
        String normalized = normalizeIdentifierToken(name);
        if (!namedPlans.containsKey(normalized)) {
            namedPlans.put(normalized, plan);
        }
    }

    private List<Tuple2<String, LogicalPlan>> extractCteRelations(LogicalPlan plan) {
        Optional<Object> result = invokeNoArg(plan, "cteRelations");
        if (!result.isPresent() || !(result.get() instanceof Seq)) {
            return Collections.emptyList();
        }

        List<Tuple2<String, LogicalPlan>> cteRelations = new ArrayList<>();
        for (Object value : ScalaInterop.toJavaList((Seq<?>) result.get())) {
            if (value instanceof Tuple2) {
                Tuple2<?, ?> tuple = (Tuple2<?, ?>) value;
                if (tuple._1() != null && tuple._2() instanceof LogicalPlan) {
                    cteRelations.add(new Tuple2<>(String.valueOf(tuple._1()), (LogicalPlan) tuple._2()));
                }
            }
        }
        return cteRelations;
    }

    private List<TargetExpression> collectTargetExpressions(LogicalPlan plan, ResolutionContext resolutionContext) {
        if (plan == null) {
            return Collections.emptyList();
        }
        if (isWithWrapper(plan)) {
            List<LogicalPlan> children = ScalaInterop.toJavaList(plan.children());
            if (!children.isEmpty()) {
                return collectTargetExpressions(children.get(0), resolutionContext);
            }
        }
        if (plan instanceof UnresolvedRelation) {
            return resolveNamedRelationTargets((UnresolvedRelation) plan, resolutionContext);
        }
        if (plan instanceof InsertIntoStatement) {
            return collectTargetExpressions(((InsertIntoStatement) plan).query(), resolutionContext);
        }
        if (plan instanceof SubqueryAlias) {
            return collectTargetExpressions(((SubqueryAlias) plan).child(), resolutionContext);
        }
        if (plan instanceof Project) {
            return resolveProjectExpressions((Project) plan, resolutionContext);
        }
        if (plan instanceof Aggregate) {
            return resolveAggregateExpressions((Aggregate) plan, resolutionContext);
        }
        if (plan instanceof Union) {
            return resolveUnionTargets((Union) plan, resolutionContext);
        }
        if ("Join".equals(plan.nodeName())) {
            List<TargetExpression> targets = new ArrayList<>();
            for (LogicalPlan child : ScalaInterop.toJavaList(plan.children())) {
                targets.addAll(collectTargetExpressions(child, resolutionContext));
            }
            if (!targets.isEmpty()) {
                return targets;
            }
        }

        Optional<LogicalPlan> reflectedQuery = extractCommandQueryPlan(plan);
        if (reflectedQuery.isPresent()) {
            List<TargetExpression> queryTargets = collectTargetExpressions(reflectedQuery.get(), resolutionContext);
            if (!queryTargets.isEmpty()) {
                return applyOutputColumnNames(
                        queryTargets,
                        extractStringValuesByReflection(plan, "outputColumnNames"),
                        extractAttributeTypesByReflection(plan, "outputColumns")
                );
            }
        }

        List<TargetExpression> reflectedOutputs = toTargetExpressions(extractNamedExpressionsByReflection(plan, "outputColumns"));
        if (!reflectedOutputs.isEmpty()) {
            return reflectedOutputs;
        }
        Optional<LogicalPlan> reflectedChild = extractTransparentChildPlan(plan);
        if (reflectedChild.isPresent() && reflectedChild.get() != plan) {
            return collectTargetExpressions(reflectedChild.get(), resolutionContext);
        }
        if (shouldDescend(plan)) {
            return collectTargetExpressions(ScalaInterop.toJavaList(plan.children()).get(0), resolutionContext);
        }

        List<TargetExpression> outputs = new ArrayList<>();
        try {
            for (Attribute attribute : ScalaInterop.toJavaList(plan.output())) {
                outputs.add(TargetExpression.fromAttribute(attribute));
            }
            return outputs;
        } catch (Exception error) {
            return Collections.emptyList();
        }
    }

    private List<TargetExpression> resolveNamedRelationTargets(UnresolvedRelation relation, ResolutionContext resolutionContext) {
        List<String> parts = toStringList(relation.multipartIdentifier());
        Optional<LogicalPlan> namedPlan = resolutionContext.resolveNamedPlan(parts);
        if (!namedPlan.isPresent()) {
            return Collections.emptyList();
        }

        String relationKey = resolutionContext.normalizePlanKey(parts);
        if (!resolutionContext.enterNamedPlan(relationKey)) {
            return Collections.emptyList();
        }
        try {
            return collectTargetExpressions(namedPlan.get(), resolutionContext);
        } finally {
            resolutionContext.exitNamedPlan(relationKey);
        }
    }

    private List<TableRef> collectInputTables(LogicalPlan plan) {
        LinkedHashMap<String, TableRef> tables = new LinkedHashMap<>();
        collectInputTables(plan, tables);
        return new ArrayList<>(tables.values());
    }

    private void collectInputTables(LogicalPlan plan, LinkedHashMap<String, TableRef> tables) {
        if (plan == null) {
            return;
        }

        if (isWithWrapper(plan)) {
            for (Tuple2<String, LogicalPlan> cteRelation : extractCteRelations(plan)) {
                collectInputTables(cteRelation._2(), tables);
            }
            List<LogicalPlan> children = ScalaInterop.toJavaList(plan.children());
            if (!children.isEmpty()) {
                collectInputTables(children.get(0), tables);
            }
            return;
        }

        if (plan instanceof LogicalRelation) {
            logicalRelationToTable((LogicalRelation) plan).ifPresent(ref -> tables.put(ref.normalizedName(), ref));
        } else if (plan instanceof UnresolvedRelation) {
            List<String> parts = toStringList(((UnresolvedRelation) plan).multipartIdentifier());
            if (!"cte".equalsIgnoreCase(resolveRelationKind(parts))) {
                TableRef ref = TableRef.fromParts(parts);
                tables.put(ref.normalizedName(), ref);
            }
        } else if (ScalaInterop.toJavaList(plan.children()).isEmpty()) {
            extractTableRefByReflection(plan).ifPresent(ref -> tables.put(ref.normalizedName(), ref));
        }

        for (LogicalPlan child : ScalaInterop.toJavaList(plan.children())) {
            collectInputTables(child, tables);
        }
    }

    private List<TableRef> collectOutputTables(LogicalPlan plan) {
        LinkedHashMap<String, TableRef> tables = new LinkedHashMap<>();
        collectOutputTables(plan, tables);
        return new ArrayList<>(tables.values());
    }

    private void collectOutputTables(LogicalPlan plan, LinkedHashMap<String, TableRef> tables) {
        if (plan == null) {
            return;
        }

        if (plan instanceof InsertIntoStatement) {
            extractTableRefFromPlan(((InsertIntoStatement) plan).table()).ifPresent(ref -> tables.put(ref.normalizedName(), ref));
        } else if (looksLikeWriteCommand(plan.nodeName())) {
            extractTableRefByReflection(plan).ifPresent(ref -> tables.put(ref.normalizedName(), ref));
        }

        for (LogicalPlan child : ScalaInterop.toJavaList(plan.children())) {
            collectOutputTables(child, tables);
        }
    }

    private boolean looksLikeWriteCommand(String nodeName) {
        String upper = nodeName == null ? "" : nodeName.toUpperCase();
        return upper.contains("INSERT") || upper.contains("CREATE") || upper.contains("REPLACE") || upper.contains("SAVE");
    }

    private Optional<TableRef> logicalRelationToTable(LogicalRelation relation) {
        Option<CatalogTable> catalogTable = relation.catalogTable();
        if (catalogTable != null && catalogTable.isDefined()) {
            return Optional.of(TableRef.fromTableIdentifier(catalogTable.get().identifier()));
        }
        return extractTableRefByReflection(relation);
    }

    private Optional<TableRef> extractTableRefFromPlan(LogicalPlan plan) {
        if (plan instanceof LogicalRelation) {
            return logicalRelationToTable((LogicalRelation) plan);
        }
        if (plan instanceof UnresolvedRelation) {
            return Optional.of(TableRef.fromParts(toStringList(((UnresolvedRelation) plan).multipartIdentifier())));
        }
        if (plan instanceof SubqueryAlias) {
            SubqueryAlias alias = (SubqueryAlias) plan;
            Optional<TableRef> child = extractTableRefFromPlan(alias.child());
            if (child.isPresent()) {
                return child;
            }
            return Optional.of(TableRef.fromParts(Collections.singletonList(alias.alias()), "subquery"));
        }
        return extractTableRefByReflection(plan);
    }

    private Optional<TableRef> extractLocalPhysicalTableRef(LogicalPlan plan) {
        if (plan == null) {
            return Optional.empty();
        }
        if (plan instanceof LogicalRelation) {
            return logicalRelationToTable((LogicalRelation) plan);
        }
        if (plan instanceof UnresolvedRelation) {
            List<String> parts = toStringList(((UnresolvedRelation) plan).multipartIdentifier());
            if ("cte".equalsIgnoreCase(resolveRelationKind(parts))) {
                return Optional.empty();
            }
            return Optional.of(TableRef.fromParts(parts));
        }
        if (ScalaInterop.toJavaList(plan.children()).isEmpty()) {
            Optional<TableRef> tableRef = extractTableRefByReflection(plan);
            if (tableRef.isPresent() && !"cte".equalsIgnoreCase(tableRef.get().getSourceType())) {
                return tableRef;
            }
        }
        return Optional.empty();
    }

    private Optional<LogicalPlan> extractCommandQueryPlan(Object target) {
        Optional<Object> result = invokeNoArg(target, "query");
        if (result.isPresent() && result.get() instanceof LogicalPlan) {
            return Optional.of((LogicalPlan) result.get());
        }
        return Optional.empty();
    }

    private Optional<LogicalPlan> extractTransparentChildPlan(Object target) {
        for (String methodName : new String[]{"child", "plan", "queryPlan", "inputPlan"}) {
            Optional<Object> result = invokeNoArg(target, methodName);
            if (result.isPresent() && result.get() instanceof LogicalPlan) {
                return Optional.of((LogicalPlan) result.get());
            }
        }
        return Optional.empty();
    }

    private List<NamedExpression> extractNamedExpressionsByReflection(Object target, String methodName) {
        Optional<Object> result = invokeNoArg(target, methodName);
        if (!result.isPresent()) {
            return Collections.emptyList();
        }
        if (!(result.get() instanceof Seq)) {
            return Collections.emptyList();
        }

        List<?> values = ScalaInterop.toJavaList((Seq<?>) result.get());
        List<NamedExpression> expressions = new ArrayList<>();
        for (Object value : values) {
            if (value instanceof NamedExpression) {
                expressions.add((NamedExpression) value);
            }
        }
        return expressions;
    }

    private List<String> extractStringValuesByReflection(Object target, String methodName) {
        Optional<Object> result = invokeNoArg(target, methodName);
        if (!result.isPresent() || !(result.get() instanceof Seq)) {
            return Collections.emptyList();
        }
        return toStringList((Seq<?>) result.get());
    }

    private List<String> extractAttributeTypesByReflection(Object target, String methodName) {
        Optional<Object> result = invokeNoArg(target, methodName);
        if (!result.isPresent() || !(result.get() instanceof Seq)) {
            return Collections.emptyList();
        }

        List<String> types = new ArrayList<>();
        for (Object value : ScalaInterop.toJavaList((Seq<?>) result.get())) {
            if (value instanceof Attribute) {
                types.add(((Attribute) value).dataType().simpleString());
            }
        }
        return types;
    }

    private Optional<TableRef> extractTableRefByReflection(Object target) {
        if (target == null) {
            return Optional.empty();
        }

        if (target instanceof TableRef) {
            return Optional.of((TableRef) target);
        }
        if (target instanceof TableIdentifier) {
            return Optional.of(TableRef.fromTableIdentifier((TableIdentifier) target));
        }
        if (target instanceof CatalogTable) {
            return Optional.of(TableRef.fromTableIdentifier(((CatalogTable) target).identifier()));
        }
        if (target instanceof Option) {
            Option<?> option = (Option<?>) target;
            return option.isDefined() ? extractTableRefByReflection(option.get()) : Optional.empty();
        }
        if (target instanceof Seq) {
            List<String> parts = toStringList((Seq<?>) target);
            if (!parts.isEmpty()) {
                return Optional.of(TableRef.fromParts(parts));
            }
        }
        if (target instanceof String) {
            String text = ((String) target).trim();
            if (!text.isEmpty() && text.contains(".")) {
                String[] split = text.split("\\.");
                List<String> parts = new ArrayList<>();
                Collections.addAll(parts, split);
                return Optional.of(TableRef.fromParts(parts));
            }
        }

        for (String methodName : new String[]{"tableIdentifier", "identifier", "catalogTable", "tableDesc", "multipartIdentifier", "nameParts", "table"}) {
            Optional<Object> result = invokeNoArg(target, methodName);
            if (result.isPresent() && result.get() != target) {
                Optional<TableRef> tableRef = extractTableRefByReflection(result.get());
                if (tableRef.isPresent()) {
                    return tableRef;
                }
            }
        }

        return Optional.empty();
    }

    private String resolveRelationKind(List<String> parts) {
        if (parts == null || parts.isEmpty()) {
            return "table";
        }
        if (parts.size() == 1) {
            return "cte";
        }
        return "table";
    }

    private Optional<Object> invokeNoArg(Object target, String methodName) {
        try {
            Method method = target.getClass().getMethod(methodName);
            method.setAccessible(true);
            return Optional.ofNullable(method.invoke(target));
        } catch (Exception ignored) {
        }

        try {
            Method method = target.getClass().getDeclaredMethod(methodName);
            method.setAccessible(true);
            return Optional.ofNullable(method.invoke(target));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private String detectStatementType(LogicalPlan plan, List<TableRef> outputTables) {
        if (plan == null) {
            return "UNKNOWN";
        }
        if (isWithWrapper(plan)) {
            List<LogicalPlan> children = ScalaInterop.toJavaList(plan.children());
            if (!children.isEmpty()) {
                return detectStatementType(children.get(0), outputTables);
            }
        }
        if (plan instanceof InsertIntoStatement) {
            return "INSERT";
        }
        String nodeName = plan.nodeName() == null ? "UNKNOWN" : plan.nodeName().toUpperCase();
        if (nodeName.contains("CREATE") && !outputTables.isEmpty()) {
            return "CREATE_TABLE_AS_SELECT";
        }
        if (!outputTables.isEmpty()) {
            return nodeName + "_WRITE";
        }
        return nodeName;
    }

    private boolean isWithWrapper(LogicalPlan plan) {
        String nodeName = plan.nodeName();
        return "With".equals(nodeName)
                || "WithCTE".equals(nodeName)
                || "UnresolvedWith".equals(nodeName);
    }

    private String writeModeFromStatement(String statementType) {
        if (statementType == null) {
            return null;
        }
        if ("INSERT".equals(statementType)) {
            return "INSERT";
        }
        if ("CREATE_TABLE_AS_SELECT".equals(statementType)) {
            return "CREATE";
        }
        if (statementType.contains("OVERWRITE")) {
            return "OVERWRITE";
        }
        if (statementType.contains("WRITE")) {
            return "WRITE";
        }
        return null;
    }

    private List<TableRef> deduplicateTables(List<TableRef> tables) {
        LinkedHashMap<String, TableRef> dedup = new LinkedHashMap<>();
        for (TableRef table : tables) {
            if (table.getName() != null && !dedup.containsKey(table.normalizedName())) {
                dedup.put(table.normalizedName(), table);
            }
        }
        List<TableRef> result = new ArrayList<>(dedup.values());
        result.sort(Comparator.comparing(TableRef::normalizedName));
        return result;
    }

    private ColumnNode buildTargetColumn(
            ExecutionCaptureEvent event,
            List<TableRef> outputTables,
            TargetExpression expression,
            int index
    ) {
        String ownerId = outputTables.size() == 1 ? outputTables.get(0).normalizedName() : "event:" + event.getEventId();
        String ownerType = outputTables.size() == 1 ? "TABLE" : "EXECUTION_OUTPUT";
        String columnName = expression.getName();
        return new ColumnNode(
                ownerId + "." + columnName,
                ownerId,
                ownerType,
                columnName,
                expression.getDataType(),
                Collections.singletonList(String.valueOf(index))
        );
    }

    private void attachExpression(
            ExecutionCaptureEvent event,
            TargetExpression targetExpression,
            String sinkNodeId,
            SinkKind sinkKind,
            String role,
            String path,
            String scopeNodeId,
            String operatorInstanceId,
            GraphBuilder builder,
            ResolutionContext resolutionContext
    ) {
        attachExpression(
                event,
                targetExpression,
                sinkNodeId,
                sinkKind,
                role,
                path,
                scopeNodeId,
                operatorInstanceId,
                Collections.<ColumnInstanceNode>emptyList(),
                builder,
                resolutionContext,
                new HashSet<String>()
        );
    }

    private void attachExpression(
            ExecutionCaptureEvent event,
            TargetExpression targetExpression,
            String sinkNodeId,
            SinkKind sinkKind,
            String role,
            String path,
            String scopeNodeId,
            String operatorInstanceId,
            List<ColumnInstanceNode> sinkColumnInstances,
            GraphBuilder builder,
            ResolutionContext resolutionContext
    ) {
        attachExpression(
                event,
                targetExpression,
                sinkNodeId,
                sinkKind,
                role,
                path,
                scopeNodeId,
                operatorInstanceId,
                sinkColumnInstances,
                builder,
                resolutionContext,
                new HashSet<String>()
        );
    }

    private void attachExpression(
            ExecutionCaptureEvent event,
            TargetExpression targetExpression,
            String sinkNodeId,
            SinkKind sinkKind,
            String role,
            String path,
            String scopeNodeId,
            String operatorInstanceId,
            List<ColumnInstanceNode> sinkColumnInstances,
            GraphBuilder builder,
            ResolutionContext resolutionContext,
            Set<String> visitingExpressions
    ) {
        List<ColumnNode> expandedSources = targetExpression.getSourceColumns();
        Expression expression = targetExpression.getExpression();
        boolean forceExpressionNode = shouldMaterializeExpressionNode(targetExpression);
        if (isDirectReferenceExpression(expression)
                && expandedSources.size() == 1
                && targetExpression.getDependentTargets().isEmpty()
                && !forceExpressionNode) {
            ColumnNode column = expandedSources.get(0);
            builder.addColumn(column);
            builder.addEdge(new LineageGraphEdge(
                    column.getNodeId(),
                    sinkNodeId,
                    edgeTypeFor(SourceKind.COLUMN, sinkKind),
                    role,
                    event.getEventId()
            ));
            return;
        }
        String visitingKey = sinkNodeId + "|" + normalizeExpression(expression) + "|" + path;
        if (!visitingExpressions.add(visitingKey)) {
            return;
        }
        try {
            attachExpression(
                    event,
                    expression,
                    sinkNodeId,
                    sinkKind,
                    role,
                    path,
                    scopeNodeId,
                    operatorInstanceId,
                    sinkColumnInstances,
                    builder,
                    resolutionContext,
                    expandedSources,
                    forceExpressionNode
            );
            String expressionNodeId = expressionNodeId(sinkNodeId, expression);
            int dependencyIndex = 0;
            for (TargetExpression dependentTarget : targetExpression.getDependentTargets()) {
                attachDependentTarget(
                        event,
                        dependentTarget,
                        expressionNodeId,
                        "source_expression:" + dependencyIndex,
                        path + ".dep." + dependencyIndex,
                        scopeNodeId,
                        builder,
                        resolutionContext,
                        visitingExpressions
                );
                dependencyIndex++;
            }
        } finally {
            visitingExpressions.remove(visitingKey);
        }
    }

    private boolean shouldMaterializeExpressionNode(TargetExpression targetExpression) {
        if (targetExpression == null || targetExpression.getExpression() == null) {
            return false;
        }
        if (!isDirectReferenceExpression(targetExpression.getExpression())) {
            return false;
        }
        return targetExpression.getSourceColumns().isEmpty() || !targetExpression.getDependentTargets().isEmpty();
    }

    private void attachDependentTarget(
            ExecutionCaptureEvent event,
            TargetExpression dependentTarget,
            String sinkExpressionNodeId,
            String role,
            String path,
            String scopeNodeId,
            GraphBuilder builder,
            ResolutionContext resolutionContext,
            Set<String> visitingExpressions
    ) {
        if (dependentTarget == null || dependentTarget.getExpression() == null) {
            return;
        }
        if (isDirectReferenceExpression(dependentTarget.getExpression()) && dependentTarget.getSourceColumns().size() == 1) {
            ColumnNode column = dependentTarget.getSourceColumns().get(0);
            builder.addColumn(column);
            builder.addEdge(new LineageGraphEdge(
                    column.getNodeId(),
                    sinkExpressionNodeId,
                    edgeTypeFor(SourceKind.COLUMN, SinkKind.EXPRESSION),
                    role,
                    event.getEventId()
            ));
            return;
        }
        attachExpression(
                event,
                dependentTarget,
                sinkExpressionNodeId,
                SinkKind.EXPRESSION,
                role,
                path,
                scopeNodeId,
                null,
                Collections.<ColumnInstanceNode>emptyList(),
                builder,
                resolutionContext,
                visitingExpressions
        );
    }

    private void attachExpression(
            ExecutionCaptureEvent event,
            Expression expression,
            String sinkNodeId,
            SinkKind sinkKind,
            String role,
            String path,
            String scopeNodeId,
            String operatorInstanceId,
            List<ColumnInstanceNode> sinkColumnInstances,
            GraphBuilder builder,
            ResolutionContext resolutionContext,
            List<ColumnNode> precomputedSourceColumns,
            boolean forceExpressionNode
    ) {
        if (!forceExpressionNode && expression instanceof AttributeReference) {
            ColumnNode column = toSourceColumn((AttributeReference) expression, resolutionContext);
            builder.addColumn(column);
            builder.addEdge(new LineageGraphEdge(
                    column.getNodeId(),
                    sinkNodeId,
                    edgeTypeFor(SourceKind.COLUMN, sinkKind),
                    role,
                    event.getEventId()
            ));
            connectDirectSourceToOutputColumnInstances(event, scopeNodeId, column, sinkColumnInstances, role, builder);
            return;
        }
        if (!forceExpressionNode && expression instanceof UnresolvedAttribute) {
            ColumnNode column = toSourceColumn((UnresolvedAttribute) expression, resolutionContext);
            builder.addColumn(column);
            builder.addEdge(new LineageGraphEdge(
                    column.getNodeId(),
                    sinkNodeId,
                    edgeTypeFor(SourceKind.COLUMN, sinkKind),
                    role,
                    event.getEventId()
            ));
            connectDirectSourceToOutputColumnInstances(event, scopeNodeId, column, sinkColumnInstances, role, builder);
            return;
        }

        ExpressionNode expressionNode = new ExpressionNode(
                expressionNodeId(sinkNodeId, expression),
                expression.prettyName(),
                safeExpressionSql(expression),
                normalizeExpression(expression)
        );
        builder.addExpression(expressionNode);
        if (scopeNodeId != null) {
            builder.addEdge(new LineageGraphEdge(
                    scopeNodeId,
                    expressionNode.getNodeId(),
                    "SCOPE_TO_EXPRESSION",
                    "scope_expression",
                    event.getEventId()
            ));
        }
        builder.addEdge(new LineageGraphEdge(
                expressionNode.getNodeId(),
                sinkNodeId,
                edgeTypeFor(SourceKind.EXPRESSION, sinkKind),
                role,
                event.getEventId()
        ));
        if (operatorInstanceId != null) {
            builder.addEdge(new LineageGraphEdge(
                    operatorInstanceId,
                    expressionNode.getNodeId(),
                    "OPERATOR_TO_EXPRESSION",
                    role,
                    event.getEventId()
            ));
        }
        for (ColumnInstanceNode sinkColumnInstance : sinkColumnInstances) {
            builder.addEdge(new LineageGraphEdge(
                    expressionNode.getNodeId(),
                    sinkColumnInstance.getNodeId(),
                    "EXPRESSION_TO_COLUMN_INSTANCE",
                    role,
                    event.getEventId()
            ));
        }

        LinkedHashMap<String, ColumnNode> sourceColumns = new LinkedHashMap<>();
        if (precomputedSourceColumns != null && !precomputedSourceColumns.isEmpty()) {
            for (ColumnNode precomputedSourceColumn : precomputedSourceColumns) {
                sourceColumns.put(precomputedSourceColumn.getNodeId(), precomputedSourceColumn);
            }
        } else {
            collectSourceColumns(expression, sourceColumns, resolutionContext);
        }
        for (ColumnNode sourceColumn : sourceColumns.values()) {
            builder.addColumn(sourceColumn);
            builder.addEdge(new LineageGraphEdge(
                    sourceColumn.getNodeId(),
                    expressionNode.getNodeId(),
                    edgeTypeFor(SourceKind.COLUMN, SinkKind.EXPRESSION),
                    "source_column",
                    event.getEventId()
            ));
            if (scopeNodeId != null) {
                for (ColumnInstanceNode columnInstanceNode : createColumnInstances(
                        event,
                        scopeNodeId,
                        sourceColumn,
                        "EXPRESSION_INPUT",
                        "expression_input",
                        null,
                        builder
                )) {
                    builder.addEdge(new LineageGraphEdge(
                            columnInstanceNode.getNodeId(),
                            expressionNode.getNodeId(),
                            "COLUMN_INSTANCE_TO_EXPRESSION",
                            "source_column_instance",
                            event.getEventId()
                    ));
                }
            }
        }
        collectLiteralNodes(expression, expressionNode.getNodeId(), builder, event, "LITERAL_TO_EXPRESSION", "literal");
    }

    private void connectDirectSourceToOutputColumnInstances(
            ExecutionCaptureEvent event,
            String scopeNodeId,
            ColumnNode sourceColumn,
            List<ColumnInstanceNode> sinkColumnInstances,
            String role,
            GraphBuilder builder
    ) {
        if (scopeNodeId == null || sinkColumnInstances == null || sinkColumnInstances.isEmpty() || sourceColumn == null) {
            return;
        }
        for (ColumnInstanceNode sourceColumnInstance : createColumnInstances(
                event,
                scopeNodeId,
                sourceColumn,
                "EXPRESSION_INPUT",
                "expression_input",
                null,
                builder
        )) {
            for (ColumnInstanceNode sinkColumnInstance : sinkColumnInstances) {
                builder.addEdge(new LineageGraphEdge(
                        sourceColumnInstance.getNodeId(),
                        sinkColumnInstance.getNodeId(),
                        "COLUMN_INSTANCE_TO_COLUMN_INSTANCE",
                        role,
                        event.getEventId()
                ));
            }
        }
    }

    private void connectOperatorToOutputInstances(
            ExecutionCaptureEvent event,
            String operatorInstanceId,
            List<ColumnInstanceNode> outputColumnInstances,
            GraphBuilder builder
    ) {
        if (operatorInstanceId == null || outputColumnInstances == null || outputColumnInstances.isEmpty()) {
            return;
        }
        for (ColumnInstanceNode outputColumnInstance : outputColumnInstances) {
            builder.addEdge(new LineageGraphEdge(
                    operatorInstanceId,
                    outputColumnInstance.getNodeId(),
                    "OPERATOR_TO_COLUMN_INSTANCE",
                    "emits_output",
                    event.getEventId()
            ));
        }
    }

    private void connectOperatorToRelations(
            ExecutionCaptureEvent event,
            String operatorInstanceId,
            List<RelationInstanceNode> relationInstances,
            String role,
            GraphBuilder builder
    ) {
        if (operatorInstanceId == null || relationInstances == null || relationInstances.isEmpty()) {
            return;
        }
        for (RelationInstanceNode relationInstanceNode : relationInstances) {
            builder.addEdge(new LineageGraphEdge(
                    operatorInstanceId,
                    relationInstanceNode.getNodeId(),
                    "OPERATOR_TO_RELATION_INSTANCE",
                    role,
                    event.getEventId()
            ));
        }
    }

    private void connectJoinRelations(
            ExecutionCaptureEvent event,
            Join join,
            List<List<RelationInstanceNode>> relationGroups,
            GraphBuilder builder
    ) {
        if (relationGroups.size() < 2) {
            return;
        }
        String joinRole = normalizeJoinType(join) + "_join";
        for (int i = 0; i < relationGroups.size(); i++) {
            for (int j = i + 1; j < relationGroups.size(); j++) {
                for (RelationInstanceNode leftRelation : relationGroups.get(i)) {
                    for (RelationInstanceNode rightRelation : relationGroups.get(j)) {
                        builder.addEdge(new LineageGraphEdge(
                                leftRelation.getNodeId(),
                                rightRelation.getNodeId(),
                                "RELATION_INSTANCE_TO_RELATION_INSTANCE",
                                joinRole,
                                event.getEventId()
                        ));
                        builder.addEdge(new LineageGraphEdge(
                                rightRelation.getNodeId(),
                                leftRelation.getNodeId(),
                                "RELATION_INSTANCE_TO_RELATION_INSTANCE",
                                joinRole,
                                event.getEventId()
                        ));
                    }
                }
            }
        }
    }

    private String registerOperatorInstance(
            ExecutionCaptureEvent event,
            String scopeNodeId,
            String path,
            LogicalPlan plan,
            String parentOperatorId,
            GraphBuilder builder
    ) {
        if (scopeNodeId == null || plan == null) {
            return null;
        }
        String operatorType = normalizeIdentifierToken(plan.nodeName());
        String nodeId = operatorInstanceNodeId(event.getEventId(), scopeNodeId, path, operatorType);
        builder.addOperatorInstance(new OperatorInstanceNode(
                nodeId,
                scopeNodeId,
                operatorType,
                describeOperatorSubType(plan),
                path,
                parentOperatorId,
                plan.nodeName()
        ));
        builder.addEdge(new LineageGraphEdge(
                scopeNodeId,
                nodeId,
                "SCOPE_TO_OPERATOR_INSTANCE",
                "contains_operator",
                event.getEventId()
        ));
        if (parentOperatorId != null) {
            builder.addEdge(new LineageGraphEdge(
                    parentOperatorId,
                    nodeId,
                    "OPERATOR_TO_OPERATOR_INSTANCE",
                    "child_operator",
                    event.getEventId()
            ));
        }
        return nodeId;
    }

    private RelationInstanceNode registerRelationInstance(
            ExecutionCaptureEvent event,
            String scopeNodeId,
            String path,
            TableRef tableRef,
            String aliasName,
            String planNodeName,
            GraphBuilder builder
    ) {
        RelationInstanceNode relationInstanceNode = new RelationInstanceNode(
                relationInstanceNodeId(event.getEventId(), scopeNodeId, path, tableRef),
                aliasName == null || aliasName.isEmpty() ? tableRef.normalizedName() : aliasName,
                scopeNodeId,
                tableRef.normalizedName(),
                tableRef.getSourceType(),
                aliasName,
                planNodeName
        );
        builder.addRelationInstance(relationInstanceNode);
        builder.addEdge(new LineageGraphEdge(
                scopeNodeId,
                relationInstanceNode.getNodeId(),
                "SCOPE_TO_RELATION_INSTANCE",
                "reads",
                event.getEventId()
        ));
        builder.addEdge(new LineageGraphEdge(
                relationInstanceNode.getNodeId(),
                "table:" + tableRef.normalizedName(),
                "RELATION_INSTANCE_TO_TABLE",
                "of_table",
                event.getEventId()
        ));
        return relationInstanceNode;
    }

    private RelationInstanceNode registerNamedRelationInstance(
            ExecutionCaptureEvent event,
            String scopeNodeId,
            String path,
            String relationName,
            String planNodeName,
            GraphBuilder builder
    ) {
        String normalizedRelationName = normalizeIdentifierToken(relationName);
        RelationInstanceNode relationInstanceNode = new RelationInstanceNode(
                relationInstanceNodeId(event.getEventId(), scopeNodeId, path, normalizedRelationName),
                normalizedRelationName,
                scopeNodeId,
                normalizedRelationName,
                "named_plan",
                normalizedRelationName,
                planNodeName
        );
        builder.addRelationInstance(relationInstanceNode);
        builder.addEdge(new LineageGraphEdge(
                scopeNodeId,
                relationInstanceNode.getNodeId(),
                "SCOPE_TO_RELATION_INSTANCE",
                "reads",
                event.getEventId()
        ));
        return relationInstanceNode;
    }

    private Optional<LogicalPlan> resolveNamedRelationPlan(
            UnresolvedRelation unresolvedRelation,
            ResolutionContext resolutionContext
    ) {
        if (unresolvedRelation == null || resolutionContext == null) {
            return Optional.empty();
        }
        List<String> identifierParts = toStringList(unresolvedRelation.multipartIdentifier());
        if (identifierParts.isEmpty()) {
            return Optional.empty();
        }
        if (identifierParts.size() > 1 && !"cte".equalsIgnoreCase(resolveRelationKind(identifierParts))) {
            return Optional.empty();
        }
        return resolutionContext.resolveNamedPlan(identifierParts);
    }

    private String namedRelationName(List<String> identifierParts) {
        if (identifierParts == null || identifierParts.isEmpty()) {
            return "unknown_named_relation";
        }
        return normalizeIdentifierToken(identifierParts.get(identifierParts.size() - 1));
    }

    private String relationInstanceNodeId(
            String eventId,
            String scopeNodeId,
            String path,
            String sourceTableId
    ) {
        return "relation_instance:"
                + HashUtils.sha1(eventId
                + "|" + (scopeNodeId == null ? "" : scopeNodeId)
                + "|" + (path == null ? "" : path)
                + "|" + (sourceTableId == null ? "" : sourceTableId));
    }

    private String joinInputRole(int index, int totalInputs) {
        if (totalInputs == 2) {
            return index == 0 ? "left_input" : "right_input";
        }
        return "join_input_" + index;
    }

    private String normalizePredicateRole(String predicateType) {
        if (predicateType == null || predicateType.trim().isEmpty()) {
            return "predicate_condition";
        }
        return normalizeIdentifierToken(predicateType) + "_condition";
    }

    private String describeOperatorSubType(LogicalPlan plan) {
        if (plan instanceof Join) {
            return normalizeIdentifierToken(String.valueOf(((Join) plan).joinType()));
        }
        if (plan instanceof Filter) {
            return "where";
        }
        return null;
    }

    private String normalizeJoinType(Join join) {
        if (join == null) {
            return "join";
        }
        String joinType = normalizeIdentifierToken(String.valueOf(join.joinType()));
        return joinType.isEmpty() ? "join" : joinType;
    }

    private LogicalPlan unwrapOutputPlan(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }
        if (plan instanceof InsertIntoStatement) {
            return ((InsertIntoStatement) plan).query();
        }
        if (isWithWrapper(plan)) {
            List<LogicalPlan> children = ScalaInterop.toJavaList(plan.children());
            if (children.isEmpty()) {
                return plan;
            }
            return unwrapOutputPlan(children.get(0));
        }
        return plan;
    }

    private List<ColumnInstanceNode> createColumnInstances(
            ExecutionCaptureEvent event,
            String scopeNodeId,
            ColumnNode columnNode,
            String instanceType,
            String role,
            Integer ordinal,
            GraphBuilder builder
    ) {
        if (scopeNodeId == null || columnNode == null) {
            return Collections.emptyList();
        }
        List<RelationInstanceNode> relationInstances = builder.findRelationInstances(scopeNodeId, columnNode);
        List<ColumnInstanceNode> instances = new ArrayList<>();
        if (relationInstances.isEmpty()) {
            instances.add(registerColumnInstance(
                    event,
                    scopeNodeId,
                    null,
                    columnNode,
                    instanceType,
                    role,
                    ordinal,
                    builder
            ));
            return instances;
        }
        for (RelationInstanceNode relationInstanceNode : relationInstances) {
            instances.add(registerColumnInstance(
                    event,
                    scopeNodeId,
                    relationInstanceNode,
                    columnNode,
                    instanceType,
                    role,
                    ordinal,
                    builder
            ));
        }
        return instances;
    }

    private ColumnInstanceNode registerColumnInstance(
            ExecutionCaptureEvent event,
            String scopeNodeId,
            RelationInstanceNode relationInstanceNode,
            ColumnNode columnNode,
            String instanceType,
            String role,
            Integer ordinal,
            GraphBuilder builder
    ) {
        String relationInstanceId = relationInstanceNode == null ? null : relationInstanceNode.getNodeId();
        ColumnInstanceNode columnInstanceNode = new ColumnInstanceNode(
                columnInstanceNodeId(scopeNodeId, relationInstanceId, columnNode, instanceType, ordinal),
                columnNode.getNodeId(),
                columnNode.getName(),
                scopeNodeId,
                relationInstanceId,
                instanceType,
                columnNode.getDataType(),
                ordinal
        );
        builder.addColumnInstance(columnInstanceNode);
        builder.addEdge(new LineageGraphEdge(
                scopeNodeId,
                columnInstanceNode.getNodeId(),
                "SCOPE_TO_COLUMN_INSTANCE",
                role,
                event.getEventId()
        ));
        if (relationInstanceNode != null) {
            builder.addEdge(new LineageGraphEdge(
                    relationInstanceNode.getNodeId(),
                    columnInstanceNode.getNodeId(),
                    "RELATION_INSTANCE_TO_COLUMN_INSTANCE",
                    role,
                    event.getEventId()
            ));
        }
        builder.addEdge(new LineageGraphEdge(
                columnNode.getNodeId(),
                columnInstanceNode.getNodeId(),
                "COLUMN_TO_COLUMN_INSTANCE",
                role,
                event.getEventId()
        ));
        return columnInstanceNode;
    }

    private void collectLiteralNodes(
            Expression expression,
            String sinkExpressionNodeId,
            GraphBuilder builder,
            ExecutionCaptureEvent event,
            String edgeType,
            String role
    ) {
        if (expression == null) {
            return;
        }
        if (expression instanceof Literal) {
            Literal literal = (Literal) expression;
            LiteralNode literalNode = new LiteralNode(
                    literalNodeId(literal),
                    literal.dataType().simpleString(),
                    safeExpressionSql(literal),
                    normalizeLiteral(literal)
            );
            builder.addLiteral(literalNode);
            builder.addEdge(new LineageGraphEdge(
                    literalNode.getNodeId(),
                    sinkExpressionNodeId,
                    edgeType,
                    role,
                    event.getEventId()
            ));
            return;
        }
        for (Expression child : ScalaInterop.toJavaList(expression.children())) {
            collectLiteralNodes(child, sinkExpressionNodeId, builder, event, edgeType, role);
        }
    }

    private void collectSourceColumns(
            Expression expression,
            LinkedHashMap<String, ColumnNode> sourceColumns,
            ResolutionContext resolutionContext
    ) {
        if (expression instanceof AttributeReference) {
            ColumnNode column = toSourceColumn((AttributeReference) expression, resolutionContext);
            sourceColumns.put(column.getNodeId(), column);
            return;
        }
        if (expression instanceof UnresolvedAttribute) {
            ColumnNode column = toSourceColumn((UnresolvedAttribute) expression, resolutionContext);
            sourceColumns.put(column.getNodeId(), column);
            return;
        }

        for (Expression child : ScalaInterop.toJavaList(expression.children())) {
            collectSourceColumns(child, sourceColumns, resolutionContext);
        }
    }

    private ColumnNode toSourceColumn(AttributeReference attribute, ResolutionContext resolutionContext) {
        return toSourceColumn(attribute, resolutionContext, null);
    }

    private ColumnNode toSourceColumn(AttributeReference attribute, ResolutionContext resolutionContext, TableRef defaultTable) {
        List<String> qualifier = toStringList(attribute.qualifier());
        TableRef resolvedTable = resolutionContext.resolveQualifier(qualifier);
        String ownerId = resolvedTable != null
                ? resolvedTable.normalizedName()
                : defaultOwnerId(qualifier, defaultTable);
        String ownerType = resolvedTable != null ? "TABLE" : (qualifier.isEmpty() ? "UNKNOWN" : "TABLE_OR_SUBQUERY");
        if (resolvedTable == null && qualifier.isEmpty() && defaultTable != null) {
            ownerType = "TABLE";
        }

        return new ColumnNode(
                ownerId + "." + attribute.name(),
                ownerId,
                ownerType,
                attribute.name(),
                attribute.dataType().simpleString(),
                qualifier
        );
    }

    private ColumnNode toSourceColumn(UnresolvedAttribute attribute, ResolutionContext resolutionContext) {
        return toSourceColumn(attribute, resolutionContext, null);
    }

    private ColumnNode toSourceColumn(UnresolvedAttribute attribute, ResolutionContext resolutionContext, TableRef defaultTable) {
        List<String> nameParts = toStringList(attribute.nameParts());
        if (nameParts.isEmpty()) {
            return new ColumnNode(
                    "unresolved.unknown",
                    "unresolved",
                    "UNKNOWN",
                    "unknown",
                    "unknown",
                    Collections.<String>emptyList()
            );
        }
        String columnName = nameParts.get(nameParts.size() - 1);
        List<String> qualifier = nameParts.size() <= 1
                ? Collections.<String>emptyList()
                : new ArrayList<>(nameParts.subList(0, nameParts.size() - 1));
        TableRef resolvedTable = resolutionContext.resolveQualifier(qualifier);
        String ownerId = resolvedTable != null
                ? resolvedTable.normalizedName()
                : defaultOwnerId(qualifier, defaultTable);
        String ownerType = resolvedTable != null ? "TABLE" : (qualifier.isEmpty() ? "UNKNOWN" : "TABLE_OR_SUBQUERY");
        if (resolvedTable == null && qualifier.isEmpty() && defaultTable != null) {
            ownerType = "TABLE";
        }
        return new ColumnNode(
                ownerId + "." + columnName,
                ownerId,
                ownerType,
                columnName,
                "unknown",
                qualifier
        );
    }

    private String defaultOwnerId(List<String> qualifier, TableRef defaultTable) {
        if (qualifier != null && !qualifier.isEmpty()) {
            return TableRef.fromParts(qualifier, "table_or_subquery").normalizedName();
        }
        if (defaultTable != null) {
            return defaultTable.normalizedName();
        }
        return "unresolved";
    }

    private String edgeTypeFor(SourceKind sourceKind, SinkKind sinkKind) {
        if (sourceKind == SourceKind.COLUMN && sinkKind == SinkKind.COLUMN) {
            return "COLUMN_TO_COLUMN";
        }
        if (sourceKind == SourceKind.COLUMN) {
            return "COLUMN_TO_EXPRESSION";
        }
        if (sinkKind == SinkKind.COLUMN) {
            return "EXPRESSION_TO_COLUMN";
        }
        return "EXPRESSION_TO_EXPRESSION";
    }

    private String safeExpressionSql(Expression expression) {
        try {
            return expression.sql();
        } catch (Exception error) {
            return expression.prettyName();
        }
    }

    private String normalizeExpression(Expression expression) {
        return safeExpressionSql(expression).replaceAll("\\s+", " ").trim().toLowerCase();
    }

    private String expressionNodeId(String sinkNodeId, Expression expression) {
        return "expr:" + HashUtils.sha1(sinkNodeId + "|" + normalizeExpression(expression));
    }

    private String relationInstanceNodeId(String eventId, String scopeNodeId, String path, TableRef tableRef) {
        return "relation_instance:" + HashUtils.sha1(eventId + "|" + scopeNodeId + "|" + path + "|" + tableRef.normalizedName());
    }

    private String predicateNodeId(String scopeNodeId, String path, String predicateType, Expression predicateExpression) {
        return "predicate:" + HashUtils.sha1(scopeNodeId + "|" + path + "|" + predicateType + "|" + normalizeExpression(predicateExpression));
    }

    private String operatorInstanceNodeId(String eventId, String scopeNodeId, String path, String operatorType) {
        return "operator_instance:" + HashUtils.sha1(eventId + "|" + scopeNodeId + "|" + path + "|" + operatorType);
    }

    private String columnInstanceNodeId(
            String scopeNodeId,
            String relationInstanceId,
            ColumnNode columnNode,
            String instanceType,
            Integer ordinal
    ) {
        return "column_instance:" + HashUtils.sha1(
                scopeNodeId
                        + "|"
                        + (relationInstanceId == null ? "" : relationInstanceId)
                        + "|"
                        + columnNode.getNodeId()
                        + "|"
                        + instanceType
                        + "|"
                        + (ordinal == null ? "" : String.valueOf(ordinal))
        );
    }

    private String literalNodeId(Literal literal) {
        return "literal:" + HashUtils.sha1(literal.dataType().simpleString() + "|" + normalizeLiteral(literal));
    }

    private String normalizeLiteral(Literal literal) {
        return safeExpressionSql(literal).replaceAll("\\s+", " ").trim().toLowerCase();
    }

    private String scopeNodeId(String eventId, String scopeType, String path, String scopeName) {
        return "scope:" + HashUtils.sha1(eventId + "|" + scopeType + "|" + path + "|" + scopeName);
    }

    private List<TargetExpression> resolveProjectExpressions(Project project, ResolutionContext resolutionContext) {
        List<TargetExpression> childTargets;
        try {
            childTargets = collectTargetExpressions(project.child(), resolutionContext);
        } catch (Exception error) {
            childTargets = Collections.emptyList();
        }
        TargetScope scope = buildTargetScope(project.child(), childTargets, resolutionContext);

        List<TargetExpression> targets = new ArrayList<>();
        for (Object projectItem : ScalaInterop.toJavaList((Seq<?>) project.projectList())) {
            if (isStarProjection(projectItem)) {
                targets.addAll(resolveStarTargets(projectItem, scope));
                continue;
            }
            if (projectItem instanceof NamedExpression) {
                NamedExpression expression = (NamedExpression) projectItem;
                Expression originalExpression = asExpression(expression);
                TargetExpression forwardedTarget = resolveForwardedChildTarget(originalExpression, scope);
                Expression resolvedExpression = forwardedTarget == null
                        ? resolveProjectedExpression(originalExpression, scope)
                        : resolveResolvedChildExpression(originalExpression, forwardedTarget.getExpression(), scope);
                targets.add(new TargetExpression(
                        safeTargetName(expression, originalExpression),
                        safeTargetDataType(expression),
                        resolvedExpression,
                        exprIdOf(expression),
                        resolveProjectedSources(resolvedExpression, forwardedTarget, scope, resolutionContext),
                        resolveProjectedDependencies(resolvedExpression, forwardedTarget, scope)
                ));
            } else if (projectItem instanceof Expression) {
                Expression expression = (Expression) projectItem;
                TargetExpression forwardedTarget = resolveForwardedChildTarget(expression, scope);
                Expression resolvedExpression = forwardedTarget == null
                        ? resolveProjectedExpression(expression, scope)
                        : resolveResolvedChildExpression(expression, forwardedTarget.getExpression(), scope);
                targets.add(new TargetExpression(
                        safeTargetName(expression),
                        safeTargetDataType(expression),
                        resolvedExpression,
                        exprIdOf(expression),
                        resolveProjectedSources(resolvedExpression, forwardedTarget, scope, resolutionContext),
                        resolveProjectedDependencies(resolvedExpression, forwardedTarget, scope)
                ));
            }
        }
        return targets;
    }

    private List<TargetExpression> resolveAggregateExpressions(Aggregate aggregate, ResolutionContext resolutionContext) {
        List<TargetExpression> childTargets;
        try {
            childTargets = collectTargetExpressions(aggregate.child(), resolutionContext);
        } catch (Exception error) {
            childTargets = Collections.emptyList();
        }
        TargetScope scope = buildTargetScope(aggregate.child(), childTargets, resolutionContext);
        List<TargetExpression> targets = new ArrayList<>();
        for (NamedExpression expression : ScalaInterop.toJavaList(aggregate.aggregateExpressions())) {
            Expression originalExpression = asExpression(expression);
            Expression resolvedExpression = resolveProjectedExpression(originalExpression, scope);
            List<ColumnNode> resolvedSources = resolveSourceColumns(resolvedExpression, scope, resolutionContext);
            resolvedSources = fallbackAggregateSources(resolvedExpression, resolvedSources, scope);
            targets.add(new TargetExpression(
                    safeTargetName(expression, originalExpression),
                    safeTargetDataType(expression),
                    resolvedExpression,
                    exprIdOf(expression),
                    resolvedSources,
                    resolveDependentTargets(resolvedExpression, scope)
            ));
        }
        return targets;
    }

    private List<TargetExpression> resolveUnionTargets(Union union, ResolutionContext resolutionContext) {
        List<List<TargetExpression>> childTargetGroups = new ArrayList<>();
        int maxTargets = 0;
        for (LogicalPlan child : ScalaInterop.toJavaList(union.children())) {
            List<TargetExpression> childTargets = tryCollectTargetExpressions(child, resolutionContext);
            childTargetGroups.add(childTargets);
            maxTargets = Math.max(maxTargets, childTargets.size());
        }

        List<Attribute> outputAttributes = safeOutputAttributes(union);
        maxTargets = Math.max(maxTargets, outputAttributes.size());
        List<TargetExpression> mergedTargets = new ArrayList<>();
        for (int index = 0; index < maxTargets; index++) {
            TargetExpression seed = firstUnionTargetAt(childTargetGroups, index);
            Attribute outputAttribute = index < outputAttributes.size() ? outputAttributes.get(index) : null;

            String targetName = outputAttribute != null
                    ? normalizeColumnNameToken(outputAttribute.name())
                    : (seed == null ? "col_" + index : seed.getName());
            String dataType = outputAttribute != null
                    ? safeTargetDataType((Expression) outputAttribute)
                    : (seed == null ? "unknown" : seed.getDataType());
            Expression expression = outputAttribute != null
                    ? (Expression) outputAttribute
                    : (seed == null ? null : seed.getExpression());
            Object exprId = outputAttribute != null ? outputAttribute.exprId() : (seed == null ? null : seed.getExprId());

            LinkedHashMap<String, ColumnNode> mergedSources = new LinkedHashMap<>();
            LinkedHashMap<String, TargetExpression> mergedDependencies = new LinkedHashMap<>();
            for (List<TargetExpression> childTargets : childTargetGroups) {
                if (index >= childTargets.size()) {
                    continue;
                }
                TargetExpression childTarget = childTargets.get(index);
                for (ColumnNode sourceColumn : childTarget.getSourceColumns()) {
                    mergedSources.put(sourceColumn.getNodeId(), sourceColumn);
                }
                if (shouldAttachDependentTarget(childTarget)) {
                    mergedDependencies.put(childTarget.getName() + "|" + childTarget.getExprId(), childTarget);
                }
                for (TargetExpression dependency : childTarget.getDependentTargets()) {
                    mergedDependencies.put(dependency.getName() + "|" + dependency.getExprId(), dependency);
                }
            }

            if (expression == null && seed != null) {
                expression = seed.getExpression();
            }
            if (expression == null) {
                continue;
            }

            mergedTargets.add(new TargetExpression(
                    targetName,
                    dataType,
                    expression,
                    exprId,
                    new ArrayList<>(mergedSources.values()),
                    new ArrayList<>(mergedDependencies.values())
            ));
        }
        return mergedTargets;
    }

    private List<Attribute> safeOutputAttributes(LogicalPlan plan) {
        if (plan == null) {
            return Collections.emptyList();
        }
        try {
            return ScalaInterop.toJavaList(plan.output());
        } catch (Exception error) {
            LOGGER.debug("Skip unresolved output attributes for plan={}", plan.nodeName(), error);
            return Collections.emptyList();
        }
    }

    private TargetExpression firstUnionTargetAt(List<List<TargetExpression>> childTargetGroups, int index) {
        for (List<TargetExpression> childTargets : childTargetGroups) {
            if (index < childTargets.size()) {
                return childTargets.get(index);
            }
        }
        return null;
    }

    private List<TargetExpression> resolveDependentTargets(Expression expression, TargetScope scope) {
        LinkedHashMap<String, TargetExpression> dependencies = new LinkedHashMap<>();
        collectDependentTargets(expression, scope, dependencies);
        return new ArrayList<>(dependencies.values());
    }

    private void collectDependentTargets(
            Expression expression,
            TargetScope scope,
            LinkedHashMap<String, TargetExpression> dependencies
    ) {
        if (expression == null) {
            return;
        }
        if (expression instanceof Alias) {
            collectDependentTargets(((Alias) expression).child(), scope, dependencies);
            return;
        }
        if (expression instanceof Cast) {
            collectDependentTargets(((Cast) expression).child(), scope, dependencies);
            return;
        }

        TargetExpression childTarget = null;
        if (expression instanceof AttributeReference) {
            childTarget = resolveChildTarget((AttributeReference) expression, scope);
        } else if (expression instanceof UnresolvedAttribute) {
            childTarget = resolveChildTarget((UnresolvedAttribute) expression, scope);
        }
        if (childTarget != null) {
            if (shouldAttachDependentTarget(childTarget)) {
                dependencies.put(childTarget.getName() + "|" + childTarget.getExprId(), childTarget);
            }
            return;
        }

        for (Expression child : ScalaInterop.toJavaList(expression.children())) {
            collectDependentTargets(child, scope, dependencies);
        }
    }

    private boolean shouldAttachDependentTarget(TargetExpression childTarget) {
        if (childTarget == null || childTarget.getExpression() == null) {
            return false;
        }
        if (!isDirectReferenceExpression(childTarget.getExpression())) {
            return true;
        }
        return childTarget.getSourceColumns().size() != 1;
    }

    private List<ColumnNode> fallbackAggregateSources(
            Expression expression,
            List<ColumnNode> resolvedSources,
            TargetScope scope
    ) {
        if (!containsUnresolvedSource(resolvedSources)) {
            return resolvedSources;
        }

        Set<String> referencedNames = collectReferencedColumnNames(expression);
        String normalizedSql = normalizeIdentifierToken(safeExpressionSql(expression));
        String normalizedText = normalizeIdentifierToken(String.valueOf(expression));
        LinkedHashMap<String, ColumnNode> fallbackColumns = new LinkedHashMap<>();
        for (TargetExpression childTarget : scope.allChildTargets) {
            List<ColumnNode> childSources = childTarget.getSourceColumns();
            if (childSources.isEmpty()) {
                continue;
            }
            String childName = normalizeColumnNameToken(childTarget.getName());
            if (childName.isEmpty()) {
                continue;
            }
            if (!referencedNames.contains(childName)
                    && !containsIdentifier(normalizedSql, childName)
                    && !containsIdentifier(normalizedText, childName)) {
                continue;
            }
            for (ColumnNode childSource : childSources) {
                fallbackColumns.put(childSource.getNodeId(), childSource);
            }
        }
        if (!fallbackColumns.isEmpty()) {
            return new ArrayList<>(fallbackColumns.values());
        }
        return resolvedSources;
    }

    private Set<String> collectReferencedColumnNames(Expression expression) {
        LinkedHashMap<String, String> identifiers = new LinkedHashMap<>();
        collectReferencedColumnNames(expression, identifiers);
        return identifiers.keySet();
    }

    private void collectReferencedColumnNames(Expression expression, LinkedHashMap<String, String> identifiers) {
        if (expression == null) {
            return;
        }
        if (expression instanceof Alias) {
            collectReferencedColumnNames(((Alias) expression).child(), identifiers);
            return;
        }
        if (expression instanceof Cast) {
            collectReferencedColumnNames(((Cast) expression).child(), identifiers);
            return;
        }
        if (expression instanceof AttributeReference) {
            String columnName = normalizeColumnNameToken(((AttributeReference) expression).name());
            if (!columnName.isEmpty()) {
                identifiers.put(columnName, columnName);
            }
            return;
        }
        if (expression instanceof UnresolvedAttribute) {
            List<String> nameParts = toStringList(((UnresolvedAttribute) expression).nameParts());
            if (!nameParts.isEmpty()) {
                String columnName = normalizeColumnNameToken(nameParts.get(nameParts.size() - 1));
                if (!columnName.isEmpty()) {
                    identifiers.put(columnName, columnName);
                }
            }
            return;
        }
        for (Expression child : ScalaInterop.toJavaList(expression.children())) {
            collectReferencedColumnNames(child, identifiers);
        }
    }

    private boolean containsIdentifier(String normalizedSql, String identifier) {
        if (normalizedSql == null || normalizedSql.isEmpty() || identifier == null || identifier.isEmpty()) {
            return false;
        }
        Pattern pattern = Pattern.compile("(^|[^a-z0-9_])" + Pattern.quote(identifier) + "([^a-z0-9_]|$)");
        return pattern.matcher(normalizedSql).find();
    }

    private TargetScope buildTargetScope(LogicalPlan childPlan, List<TargetExpression> childTargets, ResolutionContext resolutionContext) {
        LinkedHashMap<String, LinkedHashMap<String, TargetExpression>> childByQualifier = new LinkedHashMap<>();
        collectQualifiedChildTargets(childPlan, resolutionContext, childByQualifier);
        return new TargetScope(
                new ArrayList<>(childTargets),
                indexByExprId(childTargets),
                indexByName(childTargets),
                childByQualifier,
                resolveDefaultSourceTable(childPlan)
        );
    }

    private void collectQualifiedChildTargets(
            LogicalPlan plan,
            ResolutionContext resolutionContext,
            LinkedHashMap<String, LinkedHashMap<String, TargetExpression>> childByQualifier
    ) {
        if (plan == null) {
            return;
        }
        if (plan instanceof SubqueryAlias) {
            SubqueryAlias alias = (SubqueryAlias) plan;
            String aliasName = normalizeIdentifierToken(alias.alias());
            List<TargetExpression> targets = tryCollectTargetExpressions(plan, resolutionContext);
            childByQualifier.put(aliasName, indexByName(targets));
            return;
        }
        for (LogicalPlan child : ScalaInterop.toJavaList(plan.children())) {
            collectQualifiedChildTargets(child, resolutionContext, childByQualifier);
        }
    }

    private LinkedHashMap<Object, TargetExpression> indexByExprId(List<TargetExpression> targets) {
        LinkedHashMap<Object, TargetExpression> byExprId = new LinkedHashMap<>();
        for (TargetExpression target : targets) {
            if (target.getExprId() != null) {
                byExprId.put(target.getExprId(), target);
            }
        }
        return byExprId;
    }

    private LinkedHashMap<String, TargetExpression> indexByName(List<TargetExpression> targets) {
        LinkedHashMap<String, TargetExpression> byName = new LinkedHashMap<>();
        for (TargetExpression target : targets) {
            if (target.getName() != null && !byName.containsKey(target.getName())) {
                byName.put(target.getName(), target);
            }
        }
        return byName;
    }

    private List<ColumnNode> resolveProjectedSources(
            Expression resolvedExpression,
            TargetExpression forwardedTarget,
            TargetScope scope,
            ResolutionContext resolutionContext
    ) {
        if (forwardedTarget != null && !forwardedTarget.getSourceColumns().isEmpty()) {
            return forwardedTarget.getSourceColumns();
        }
        return resolveSourceColumns(resolvedExpression, scope, resolutionContext);
    }

    private List<TargetExpression> resolveProjectedDependencies(
            Expression resolvedExpression,
            TargetExpression forwardedTarget,
            TargetScope scope
    ) {
        LinkedHashMap<String, TargetExpression> dependencies = new LinkedHashMap<>();
        for (TargetExpression dependency : resolveDependentTargets(resolvedExpression, scope)) {
            dependencies.put(dependency.getName() + "|" + dependency.getExprId(), dependency);
        }
        if (forwardedTarget != null) {
            for (TargetExpression dependency : forwardedTarget.getDependentTargets()) {
                dependencies.put(dependency.getName() + "|" + dependency.getExprId(), dependency);
            }
        }
        return new ArrayList<>(dependencies.values());
    }

    private Expression resolveProjectedExpression(Expression expression, TargetScope scope) {
        if (expression instanceof UnresolvedAttribute) {
            TargetExpression childTarget = resolveChildTarget((UnresolvedAttribute) expression, scope);
            return childTarget == null
                    ? expression
                    : resolveResolvedChildExpression(expression, childTarget.getExpression(), scope);
        }
        if (expression instanceof AttributeReference) {
            TargetExpression childTarget = resolveChildTarget((AttributeReference) expression, scope);
            return childTarget == null
                    ? expression
                    : resolveResolvedChildExpression(expression, childTarget.getExpression(), scope);
        }
        if (expression instanceof Alias) {
            Expression resolvedChild = resolveForwardingWrapper(((Alias) expression).child(), scope);
            return resolvedChild == null ? expression : resolvedChild;
        }
        return expression;
    }

    private Expression resolveForwardingWrapper(Expression expression, TargetScope scope) {
        if (expression instanceof UnresolvedAttribute) {
            TargetExpression childTarget = resolveChildTarget((UnresolvedAttribute) expression, scope);
            return childTarget == null
                    ? expression
                    : resolveResolvedChildExpression(expression, childTarget.getExpression(), scope);
        }
        if (expression instanceof AttributeReference) {
            TargetExpression childTarget = resolveChildTarget((AttributeReference) expression, scope);
            return childTarget == null
                    ? expression
                    : resolveResolvedChildExpression(expression, childTarget.getExpression(), scope);
        }
        if (expression instanceof Cast) {
            return resolveForwardingWrapper(((Cast) expression).child(), scope);
        }
        return null;
    }

    private TargetExpression resolveForwardedChildTarget(Expression expression, TargetScope scope) {
        if (expression instanceof Alias) {
            return resolveForwardedChildTarget(((Alias) expression).child(), scope);
        }
        if (expression instanceof Cast) {
            return resolveForwardedChildTarget(((Cast) expression).child(), scope);
        }
        if (expression instanceof UnresolvedAttribute) {
            return resolveChildTarget((UnresolvedAttribute) expression, scope);
        }
        if (expression instanceof AttributeReference) {
            return resolveChildTarget((AttributeReference) expression, scope);
        }
        return null;
    }

    private Expression resolveResolvedChildExpression(Expression originalExpression, Expression childExpression, TargetScope scope) {
        if (childExpression == null || childExpression == originalExpression) {
            return originalExpression;
        }
        Expression recursivelyResolved = resolveProjectedExpression(childExpression, scope);
        return recursivelyResolved == null ? childExpression : recursivelyResolved;
    }

    private List<ColumnNode> resolveSourceColumns(
            Expression expression,
            TargetScope scope,
            ResolutionContext resolutionContext
    ) {
        LinkedHashMap<String, ColumnNode> sourceColumns = new LinkedHashMap<>();
        collectExpandedSourceColumns(expression, sourceColumns, scope, resolutionContext, new HashSet<String>());
        if (containsUnresolvedSource(sourceColumns.values())) {
            LinkedHashMap<String, ColumnNode> fallbackColumns = new LinkedHashMap<>();
            collectFallbackSourceColumns(expression, scope, fallbackColumns, new HashSet<String>());
            if (!fallbackColumns.isEmpty()) {
                return new ArrayList<>(fallbackColumns.values());
            }
        }
        return new ArrayList<>(sourceColumns.values());
    }

    private void collectExpandedSourceColumns(
            Expression expression,
            LinkedHashMap<String, ColumnNode> sourceColumns,
            TargetScope scope,
            ResolutionContext resolutionContext,
            Set<String> visiting
    ) {
        if (expression == null) {
            return;
        }
        if (expression instanceof Alias) {
            collectExpandedSourceColumns(((Alias) expression).child(), sourceColumns, scope, resolutionContext, visiting);
            return;
        }
        if (expression instanceof Cast) {
            collectExpandedSourceColumns(((Cast) expression).child(), sourceColumns, scope, resolutionContext, visiting);
            return;
        }
        if (expression instanceof AttributeReference) {
            TargetExpression childTarget = resolveChildTarget((AttributeReference) expression, scope);
            if (childTarget != null) {
                if (collectFromChildTarget(childTarget, sourceColumns, visiting)) {
                    return;
                }
            }
            ColumnNode column = toSourceColumn((AttributeReference) expression, resolutionContext, scope.defaultSourceTable);
            sourceColumns.put(column.getNodeId(), column);
            return;
        }
        if (expression instanceof UnresolvedAttribute) {
            TargetExpression childTarget = resolveChildTarget((UnresolvedAttribute) expression, scope);
            if (childTarget != null) {
                if (collectFromChildTarget(childTarget, sourceColumns, visiting)) {
                    return;
                }
            }
            ColumnNode column = toSourceColumn((UnresolvedAttribute) expression, resolutionContext, scope.defaultSourceTable);
            sourceColumns.put(column.getNodeId(), column);
            return;
        }
        for (Expression child : ScalaInterop.toJavaList(expression.children())) {
            collectExpandedSourceColumns(child, sourceColumns, scope, resolutionContext, visiting);
        }
    }

    private boolean collectFromChildTarget(
            TargetExpression childTarget,
            LinkedHashMap<String, ColumnNode> sourceColumns,
            Set<String> visiting
    ) {
        if (childTarget == null) {
            return false;
        }
        String targetKey = childTarget.getName() + "|" + childTarget.getExprId();
        if (!visiting.add(targetKey)) {
            return true;
        }
        try {
            if (childTarget.getSourceColumns().isEmpty()) {
                return false;
            }
            for (ColumnNode sourceColumn : childTarget.getSourceColumns()) {
                sourceColumns.put(sourceColumn.getNodeId(), sourceColumn);
            }
            return true;
        } finally {
            visiting.remove(targetKey);
        }
    }

    private boolean containsUnresolvedSource(Iterable<ColumnNode> sourceColumns) {
        for (ColumnNode sourceColumn : sourceColumns) {
            if ("unresolved".equals(sourceColumn.getOwnerId())) {
                return true;
            }
        }
        return false;
    }

    private void collectFallbackSourceColumns(
            Expression expression,
            TargetScope scope,
            LinkedHashMap<String, ColumnNode> sourceColumns,
            Set<String> visiting
    ) {
        if (expression == null) {
            return;
        }
        if (expression instanceof Alias) {
            collectFallbackSourceColumns(((Alias) expression).child(), scope, sourceColumns, visiting);
            return;
        }
        if (expression instanceof Cast) {
            collectFallbackSourceColumns(((Cast) expression).child(), scope, sourceColumns, visiting);
            return;
        }
        if (expression instanceof AttributeReference) {
            collectFallbackByName(normalizeColumnNameToken(((AttributeReference) expression).name()), scope, sourceColumns, visiting);
            return;
        }
        if (expression instanceof UnresolvedAttribute) {
            List<String> nameParts = toStringList(((UnresolvedAttribute) expression).nameParts());
            if (!nameParts.isEmpty()) {
                collectFallbackByName(normalizeColumnNameToken(nameParts.get(nameParts.size() - 1)), scope, sourceColumns, visiting);
            }
            return;
        }
        for (Expression child : ScalaInterop.toJavaList(expression.children())) {
            collectFallbackSourceColumns(child, scope, sourceColumns, visiting);
        }
    }

    private void collectFallbackByName(
            String columnName,
            TargetScope scope,
            LinkedHashMap<String, ColumnNode> sourceColumns,
            Set<String> visiting
    ) {
        if (columnName == null || columnName.isEmpty() || !visiting.add(columnName)) {
            return;
        }
        try {
            for (TargetExpression childTarget : scope.allChildTargets) {
                if (!columnName.equals(normalizeColumnNameToken(childTarget.getName()))) {
                    continue;
                }
                for (ColumnNode sourceColumn : childTarget.getSourceColumns()) {
                    sourceColumns.put(sourceColumn.getNodeId(), sourceColumn);
                }
            }
        } finally {
            visiting.remove(columnName);
        }
    }

    private TargetExpression resolveChildTarget(
            AttributeReference attributeReference,
            TargetScope scope
    ) {
        TargetExpression childTarget = scope.childByExprId.get(exprIdOf(attributeReference));
        if (childTarget != null) {
            return childTarget;
        }
        String columnName = normalizeColumnNameToken(attributeReference.name());
        List<String> qualifier = toStringList(attributeReference.qualifier());
        if (!qualifier.isEmpty()) {
            LinkedHashMap<String, TargetExpression> qualifiedTargets = scope.childByQualifier.get(normalizeIdentifierToken(qualifier.get(0)));
            if (qualifiedTargets != null) {
                TargetExpression qualifiedTarget = qualifiedTargets.get(columnName);
                if (qualifiedTarget != null) {
                    return qualifiedTarget;
                }
            }
        }
        TargetExpression directTarget = scope.childByName.get(columnName);
        if (directTarget != null) {
            return directTarget;
        }
        for (TargetExpression scopedTarget : scope.allChildTargets) {
            if (columnName.equals(normalizeColumnNameToken(scopedTarget.getName()))) {
                return scopedTarget;
            }
        }
        return null;
    }

    private TargetExpression resolveChildTarget(UnresolvedAttribute unresolvedAttribute, TargetScope scope) {
        List<String> nameParts = toStringList(unresolvedAttribute.nameParts());
        if (nameParts.isEmpty()) {
            return null;
        }
        String columnName = normalizeColumnNameToken(nameParts.get(nameParts.size() - 1));
        if (nameParts.size() > 1) {
            LinkedHashMap<String, TargetExpression> qualifiedTargets = scope.childByQualifier.get(normalizeIdentifierToken(nameParts.get(0)));
            if (qualifiedTargets != null) {
                TargetExpression qualifiedTarget = qualifiedTargets.get(columnName);
                if (qualifiedTarget != null) {
                    return qualifiedTarget;
                }
            }
        }
        TargetExpression directTarget = scope.childByName.get(columnName);
        if (directTarget != null) {
            return directTarget;
        }

        TargetExpression uniqueByScan = null;
        for (TargetExpression childTarget : scope.allChildTargets) {
            if (!columnName.equals(normalizeColumnNameToken(childTarget.getName()))) {
                continue;
            }
            if (uniqueByScan != null && uniqueByScan != childTarget) {
                uniqueByScan = null;
                break;
            }
            uniqueByScan = childTarget;
        }
        if (uniqueByScan != null) {
            return uniqueByScan;
        }

        TargetExpression uniqueQualifiedTarget = null;
        for (LinkedHashMap<String, TargetExpression> qualifiedTargets : scope.childByQualifier.values()) {
            TargetExpression candidate = qualifiedTargets.get(columnName);
            if (candidate == null) {
                continue;
            }
            if (uniqueQualifiedTarget != null && uniqueQualifiedTarget != candidate) {
                return null;
            }
            uniqueQualifiedTarget = candidate;
        }
        return uniqueQualifiedTarget;
    }

    private List<TargetExpression> applyOutputColumnNames(
            List<TargetExpression> targets,
            List<String> outputNames,
            List<String> outputTypes
    ) {
        if (outputNames.isEmpty()) {
            return targets;
        }

        List<TargetExpression> renamed = new ArrayList<>(targets.size());
        for (int i = 0; i < targets.size(); i++) {
            TargetExpression target = targets.get(i);
            String outputName = i < outputNames.size() ? outputNames.get(i) : target.getName();
            String outputType = i < outputTypes.size() ? outputTypes.get(i) : target.getDataType();
            renamed.add(new TargetExpression(
                    outputName,
                    outputType,
                    target.getExpression(),
                    target.getExprId(),
                    target.getSourceColumns(),
                    target.getDependentTargets()
            ));
        }
        return renamed;
    }

    private List<TargetExpression> toTargetExpressions(List<? extends NamedExpression> expressions) {
        List<TargetExpression> targets = new ArrayList<>(expressions.size());
        for (NamedExpression expression : expressions) {
            targets.add(TargetExpression.fromNamedExpression(expression));
        }
        return targets;
    }

    private TableRef resolveDefaultSourceTable(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }
        if (isWithWrapper(plan)) {
            List<LogicalPlan> children = ScalaInterop.toJavaList(plan.children());
            return children.isEmpty() ? null : resolveDefaultSourceTable(children.get(0));
        }
        if (plan instanceof SubqueryAlias) {
            Optional<TableRef> childTable = extractBaseTableRef(((SubqueryAlias) plan).child());
            return childTable.orElse(null);
        }
        if (plan instanceof LogicalRelation || plan instanceof UnresolvedRelation) {
            Optional<TableRef> tableRef = extractTableRefFromPlan(plan);
            return tableRef.orElse(null);
        }
        List<LogicalPlan> children = ScalaInterop.toJavaList(plan.children());
        if (children.size() == 1) {
            return resolveDefaultSourceTable(children.get(0));
        }
        return null;
    }

    private boolean isDirectReferenceExpression(Expression expression) {
        return expression instanceof AttributeReference || expression instanceof UnresolvedAttribute;
    }

    private boolean isStarProjection(Object projectItem) {
        if (projectItem == null) {
            return false;
        }
        String text = normalizeIdentifierToken(String.valueOf(projectItem));
        return "*".equals(text) || text.endsWith(".*");
    }

    private List<TargetExpression> resolveStarTargets(Object projectItem, TargetScope scope) {
        if (projectItem == null) {
            return Collections.emptyList();
        }
        String text = normalizeIdentifierToken(String.valueOf(projectItem));
        if ("*".equals(text)) {
            return new ArrayList<>(scope.allChildTargets);
        }
        if (text.endsWith(".*")) {
            String qualifier = normalizeIdentifierToken(text.substring(0, text.length() - 2));
            LinkedHashMap<String, TargetExpression> qualifiedTargets = scope.childByQualifier.get(qualifier);
            if (qualifiedTargets != null) {
                return new ArrayList<>(qualifiedTargets.values());
            }
        }
        return Collections.emptyList();
    }

    private static String normalizeIdentifierToken(String raw) {
        if (raw == null) {
            return "";
        }
        return raw.trim().replace("`", "").toLowerCase();
    }

    private Object exprIdOf(Object expression) {
        Optional<Object> exprId = invokeNoArg(expression, "exprId");
        return exprId.orElse(null);
    }

    private Expression asExpression(NamedExpression expression) {
        if (expression instanceof Expression) {
            return (Expression) expression;
        }
        return expression.toAttribute();
    }

    private String safeTargetName(NamedExpression expression, Expression fallbackExpression) {
        try {
            String name = expression.name();
            if (name != null && !name.trim().isEmpty()) {
                return normalizeColumnNameToken(name);
            }
        } catch (Exception ignored) {
        }
        try {
            String name = expression.toAttribute().name();
            if (name != null && !name.trim().isEmpty()) {
                return normalizeColumnNameToken(name);
            }
        } catch (Exception ignored) {
        }
        return normalizeColumnNameToken(safeExpressionSql(fallbackExpression));
    }

    private String safeTargetName(Expression expression) {
        if (expression instanceof UnresolvedAttribute) {
            List<String> nameParts = toStringList(((UnresolvedAttribute) expression).nameParts());
            if (!nameParts.isEmpty()) {
                return normalizeColumnNameToken(nameParts.get(nameParts.size() - 1));
            }
        }
        return normalizeColumnNameToken(safeExpressionSql(expression));
    }

    private String safeTargetDataType(NamedExpression expression) {
        try {
            return expression.toAttribute().dataType().simpleString();
        } catch (Exception ignored) {
            return "unknown";
        }
    }

    private String safeTargetDataType(Expression expression) {
        try {
            return expression.dataType().simpleString();
        } catch (Exception ignored) {
            return "unknown";
        }
    }

    private String normalizeColumnNameToken(String raw) {
        if (raw == null) {
            return "unknown";
        }
        String cleaned = raw.trim().replace("`", "");
        while (!cleaned.isEmpty() && (cleaned.charAt(0) == '\'' || cleaned.charAt(0) == '"')) {
            cleaned = cleaned.substring(1).trim();
        }
        while (!cleaned.isEmpty() && (cleaned.charAt(cleaned.length() - 1) == '\'' || cleaned.charAt(cleaned.length() - 1) == '"')) {
            cleaned = cleaned.substring(0, cleaned.length() - 1).trim();
        }
        int dotIndex = cleaned.lastIndexOf('.');
        if (dotIndex >= 0 && dotIndex + 1 < cleaned.length()) {
            cleaned = cleaned.substring(dotIndex + 1).trim();
        }
        return cleaned.isEmpty() ? "unknown" : cleaned;
    }

    private List<String> toStringList(Seq<?> seq) {
        List<?> values = ScalaInterop.toJavaList(seq);
        List<String> result = new ArrayList<>(values.size());
        for (Object value : values) {
            if (value instanceof Tuple2) {
                Tuple2<?, ?> tuple = (Tuple2<?, ?>) value;
                result.add(String.valueOf(tuple._1()));
                result.add(String.valueOf(tuple._2()));
            } else if (value != null) {
                result.add(String.valueOf(value));
            }
        }
        return result;
    }

    private enum SinkKind {
        COLUMN,
        EXPRESSION
    }

    private enum SourceKind {
        COLUMN,
        EXPRESSION
    }

    private static final class TargetExpression {
        private final String name;
        private final String dataType;
        private final Expression expression;
        private final Object exprId;
        private final List<ColumnNode> sourceColumns;
        private final List<TargetExpression> dependentTargets;

        private TargetExpression(
                String name,
                String dataType,
                Expression expression,
                Object exprId,
                List<ColumnNode> sourceColumns,
                List<TargetExpression> dependentTargets
        ) {
            this.name = name;
            this.dataType = dataType;
            this.expression = expression;
            this.exprId = exprId;
            this.sourceColumns = sourceColumns == null ? Collections.<ColumnNode>emptyList() : new ArrayList<>(sourceColumns);
            this.dependentTargets = dependentTargets == null ? Collections.<TargetExpression>emptyList() : new ArrayList<>(dependentTargets);
        }

        private static TargetExpression fromNamedExpression(NamedExpression expression) {
            return new TargetExpression(
                    expression.name(),
                    safeDataType(expression),
                    expression instanceof Expression ? (Expression) expression : expression.toAttribute(),
                    expression.exprId(),
                    Collections.<ColumnNode>emptyList(),
                    Collections.<TargetExpression>emptyList()
            );
        }

        private static TargetExpression fromAttribute(Attribute attribute) {
            return new TargetExpression(
                    attribute.name(),
                    safeDataType(attribute),
                    (Expression) attribute,
                    attribute.exprId(),
                    Collections.<ColumnNode>emptyList(),
                    Collections.<TargetExpression>emptyList()
            );
        }

        private static String safeDataType(NamedExpression expression) {
            try {
                return expression.toAttribute().dataType().simpleString();
            } catch (Exception error) {
                return "unknown";
            }
        }

        private static String safeDataType(Attribute attribute) {
            try {
                return attribute.dataType().simpleString();
            } catch (Exception error) {
                return "unknown";
            }
        }

        private String getName() {
            return name;
        }

        private String getDataType() {
            return dataType;
        }

        private Expression getExpression() {
            return expression;
        }

        private Object getExprId() {
            return exprId;
        }

        private List<ColumnNode> getSourceColumns() {
            return new ArrayList<>(sourceColumns);
        }

        private List<TargetExpression> getDependentTargets() {
            return new ArrayList<>(dependentTargets);
        }
    }

    private static final class ResolutionContext {
        private final Map<String, TableRef> aliasToTable;
        private final Map<String, LogicalPlan> namedPlans;
        private final Set<String> resolvingNamedPlans = new HashSet<>();

        private ResolutionContext(Map<String, TableRef> aliasToTable, Map<String, LogicalPlan> namedPlans) {
            this.aliasToTable = aliasToTable;
            this.namedPlans = namedPlans;
        }

        private TableRef resolveQualifier(List<String> qualifier) {
            if (qualifier == null || qualifier.isEmpty()) {
                return null;
            }
            TableRef direct = aliasToTable.get(qualifier.get(0));
            if (direct != null) {
                return direct;
            }
            if (qualifier.size() > 1) {
                return TableRef.fromParts(qualifier, "table");
            }
            return null;
        }

        private Optional<LogicalPlan> resolveNamedPlan(List<String> identifierParts) {
            if (identifierParts == null || identifierParts.isEmpty()) {
                return Optional.empty();
            }
            String exact = normalizePlanKey(identifierParts);
            LogicalPlan exactPlan = namedPlans.get(exact);
            if (exactPlan != null) {
                return Optional.of(exactPlan);
            }
            String last = normalizeIdentifierToken(identifierParts.get(identifierParts.size() - 1));
            return Optional.ofNullable(namedPlans.get(last));
        }

        private String normalizePlanKey(List<String> identifierParts) {
            if (identifierParts == null || identifierParts.isEmpty()) {
                return "";
            }
            List<String> normalized = new ArrayList<>(identifierParts.size());
            for (String identifierPart : identifierParts) {
                normalized.add(normalizeIdentifierToken(identifierPart));
            }
            return String.join(".", normalized);
        }

        private boolean enterNamedPlan(String planKey) {
            return resolvingNamedPlans.add(planKey);
        }

        private void exitNamedPlan(String planKey) {
            resolvingNamedPlans.remove(planKey);
        }
    }

    private static final class TargetScope {
        private final List<TargetExpression> allChildTargets;
        private final LinkedHashMap<Object, TargetExpression> childByExprId;
        private final LinkedHashMap<String, TargetExpression> childByName;
        private final LinkedHashMap<String, LinkedHashMap<String, TargetExpression>> childByQualifier;
        private final TableRef defaultSourceTable;

        private TargetScope(
                List<TargetExpression> allChildTargets,
                LinkedHashMap<Object, TargetExpression> childByExprId,
                LinkedHashMap<String, TargetExpression> childByName,
                LinkedHashMap<String, LinkedHashMap<String, TargetExpression>> childByQualifier,
                TableRef defaultSourceTable
        ) {
            this.allChildTargets = allChildTargets;
            this.childByExprId = childByExprId;
            this.childByName = childByName;
            this.childByQualifier = childByQualifier;
            this.defaultSourceTable = defaultSourceTable;
        }
    }

    private static final class GraphBuilder {
        private final LinkedHashMap<String, ColumnNode> columns = new LinkedHashMap<>();
        private final LinkedHashMap<String, ExpressionNode> expressions = new LinkedHashMap<>();
        private final LinkedHashMap<String, ScopeNode> scopes = new LinkedHashMap<>();
        private final LinkedHashMap<String, LiteralNode> literals = new LinkedHashMap<>();
        private final LinkedHashMap<String, OperatorInstanceNode> operatorInstances = new LinkedHashMap<>();
        private final LinkedHashMap<String, ColumnInstanceNode> columnInstances = new LinkedHashMap<>();
        private final LinkedHashMap<String, List<ColumnInstanceNode>> columnInstancesByColumnId = new LinkedHashMap<>();
        private final LinkedHashMap<String, RelationInstanceNode> relationInstances = new LinkedHashMap<>();
        private final LinkedHashMap<String, PredicateNode> predicates = new LinkedHashMap<>();
        private final LinkedHashMap<String, LineageGraphEdge> edges = new LinkedHashMap<>();
        private final LinkedHashMap<String, List<LineageGraphEdge>> outgoingEdgesBySource = new LinkedHashMap<>();
        private final LinkedHashMap<String, List<LineageGraphEdge>> incomingEdgesByTarget = new LinkedHashMap<>();
        private final String eventId;

        private GraphBuilder(String eventId) {
            this.eventId = eventId;
        }

        private void addColumn(ColumnNode columnNode) {
            columns.put(columnNode.getNodeId(), columnNode);
        }

        private void addExpression(ExpressionNode expressionNode) {
            expressions.put(expressionNode.getNodeId(), expressionNode);
        }

        private void addScope(ScopeNode scopeNode) {
            scopes.put(scopeNode.getNodeId(), scopeNode);
        }

        private void addLiteral(LiteralNode literalNode) {
            literals.put(literalNode.getNodeId(), literalNode);
        }

        private void addOperatorInstance(OperatorInstanceNode operatorInstanceNode) {
            operatorInstances.put(operatorInstanceNode.getNodeId(), operatorInstanceNode);
        }

        private void addColumnInstance(ColumnInstanceNode columnInstanceNode) {
            if (columnInstances.put(columnInstanceNode.getNodeId(), columnInstanceNode) == null) {
                columnInstancesByColumnId
                        .computeIfAbsent(columnInstanceNode.getColumnId(), ignored -> new ArrayList<ColumnInstanceNode>())
                        .add(columnInstanceNode);
            }
        }

        private void addRelationInstance(RelationInstanceNode relationInstanceNode) {
            relationInstances.put(relationInstanceNode.getNodeId(), relationInstanceNode);
        }

        private void addPredicate(PredicateNode predicateNode) {
            predicates.put(predicateNode.getNodeId(), predicateNode);
        }

        private void addEdge(LineageGraphEdge edge) {
            String key = edge.getSourceNodeId()
                    + "|"
                    + edge.getTargetNodeId()
                    + "|"
                    + edge.getEdgeType()
                    + "|"
                    + (edge.getRole() == null ? "" : edge.getRole())
                    + "|"
                    + eventId;
            if (edges.put(key, edge) == null) {
                outgoingEdgesBySource
                        .computeIfAbsent(edge.getSourceNodeId(), ignored -> new ArrayList<LineageGraphEdge>())
                        .add(edge);
                incomingEdgesByTarget
                        .computeIfAbsent(edge.getTargetNodeId(), ignored -> new ArrayList<LineageGraphEdge>())
                        .add(edge);
            }
        }

        private List<ColumnNode> columns() {
            return new ArrayList<>(columns.values());
        }

        private List<ExpressionNode> expressions() {
            return new ArrayList<>(expressions.values());
        }

        private List<ScopeNode> scopes() {
            return new ArrayList<>(scopes.values());
        }

        private List<LiteralNode> literals() {
            return new ArrayList<>(literals.values());
        }

        private List<OperatorInstanceNode> operatorInstances() {
            return new ArrayList<>(operatorInstances.values());
        }

        private List<ColumnInstanceNode> columnInstances() {
            return new ArrayList<>(columnInstances.values());
        }

        private ColumnNode findColumn(String columnId) {
            return columns.get(columnId);
        }

        private ColumnInstanceNode findColumnInstance(String columnInstanceId) {
            return columnInstances.get(columnInstanceId);
        }

        private List<ColumnInstanceNode> findColumnInstancesByColumnId(String columnId) {
            List<ColumnInstanceNode> matches = columnInstancesByColumnId.get(columnId);
            if (matches == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(matches);
        }

        private List<LineageGraphEdge> incomingEdges(String targetNodeId) {
            List<LineageGraphEdge> matches = incomingEdgesByTarget.get(targetNodeId);
            if (matches == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(matches);
        }

        private List<LineageGraphEdge> outgoingEdges(String sourceNodeId) {
            List<LineageGraphEdge> matches = outgoingEdgesBySource.get(sourceNodeId);
            if (matches == null) {
                return Collections.emptyList();
            }
            return new ArrayList<>(matches);
        }

        private List<RelationInstanceNode> relationInstances() {
            return new ArrayList<>(relationInstances.values());
        }

        private List<RelationInstanceNode> findRelationInstances(String scopeNodeId, ColumnNode columnNode) {
            LinkedHashMap<String, RelationInstanceNode> matches = new LinkedHashMap<>();
            if (scopeNodeId == null || columnNode == null) {
                return new ArrayList<>(matches.values());
            }
            String ownerId = columnNode.getOwnerId();
            if (ownerId == null || ownerId.isEmpty()) {
                return new ArrayList<>(matches.values());
            }

            for (RelationInstanceNode relationInstanceNode : relationInstances.values()) {
                if (!scopeNodeId.equals(relationInstanceNode.getScopeId())) {
                    continue;
                }
                if (ownerId.equals(relationInstanceNode.getSourceTableId())) {
                    matches.put(relationInstanceNode.getNodeId(), relationInstanceNode);
                }
            }
            if (!matches.isEmpty()) {
                return new ArrayList<>(matches.values());
            }

            String columnName = columnNode.getName() == null ? "" : columnNode.getName();
            for (RelationInstanceNode relationInstanceNode : relationInstances.values()) {
                if (!ownerId.equals(relationInstanceNode.getSourceTableId())) {
                    continue;
                }
                if (!isScopeSameOrDescendant(scopeNodeId, relationInstanceNode.getScopeId())) {
                    continue;
                }
                if (scopeSubtreeExposesColumn(relationInstanceNode.getScopeId(), columnName)) {
                    matches.put(relationInstanceNode.getNodeId(), relationInstanceNode);
                }
            }
            if (!matches.isEmpty()) {
                return new ArrayList<>(matches.values());
            }

            for (ScopeNode ownerScope : findReachableOwnerScopes(scopeNodeId, ownerId)) {
                for (RelationInstanceNode relationInstanceNode : relationInstances.values()) {
                    if (!isScopeSameOrDescendant(ownerScope.getNodeId(), relationInstanceNode.getScopeId())) {
                        continue;
                    }
                    if (!columnName.isEmpty() && !scopeSubtreeExposesColumn(relationInstanceNode.getScopeId(), columnName)) {
                        continue;
                    }
                    matches.put(relationInstanceNode.getNodeId(), relationInstanceNode);
                }
            }
            return new ArrayList<>(matches.values());
        }

        private List<ScopeNode> findReachableOwnerScopes(String currentScopeId, String ownerId) {
            List<ScopeNode> matchedScopes = new ArrayList<>();
            if (ownerId == null || ownerId.isEmpty()) {
                return matchedScopes;
            }
            if (ownerId.startsWith("scope:")) {
                ScopeNode scopeNode = scopes.get(ownerId);
                if (scopeNode != null && isScopeSameOrDescendant(currentScopeId, scopeNode.getNodeId())) {
                    matchedScopes.add(scopeNode);
                }
                return matchedScopes;
            }
            for (ScopeNode scopeNode : scopes.values()) {
                if (!ownerId.equals(scopeNode.getScopeName())) {
                    continue;
                }
                if (!isScopeSameOrDescendant(currentScopeId, scopeNode.getNodeId())) {
                    continue;
                }
                matchedScopes.add(scopeNode);
            }
            return matchedScopes;
        }

        private boolean scopeSubtreeExposesColumn(String ancestorScopeId, String columnName) {
            if (ancestorScopeId == null || ancestorScopeId.isEmpty() || columnName == null || columnName.isEmpty()) {
                return false;
            }
            for (LineageGraphEdge edge : edges.values()) {
                if (!"SCOPE_TO_COLUMN".equals(edge.getEdgeType())) {
                    continue;
                }
                if (!isScopeSameOrDescendant(ancestorScopeId, edge.getSourceNodeId())) {
                    continue;
                }
                ColumnNode columnNode = columns.get(edge.getTargetNodeId());
                if (columnNode != null && columnName.equals(columnNode.getName())) {
                    return true;
                }
            }
            return false;
        }

        private boolean isScopeSameOrDescendant(String ancestorScopeId, String candidateScopeId) {
            if (ancestorScopeId == null || candidateScopeId == null) {
                return false;
            }
            if (ancestorScopeId.equals(candidateScopeId)) {
                return true;
            }
            ScopeNode current = scopes.get(candidateScopeId);
            while (current != null && current.getParentScopeId() != null) {
                if (ancestorScopeId.equals(current.getParentScopeId())) {
                    return true;
                }
                current = scopes.get(current.getParentScopeId());
            }
            return false;
        }

        private List<PredicateNode> predicates() {
            return new ArrayList<>(predicates.values());
        }

        private List<LineageGraphEdge> edges() {
            return new ArrayList<>(edges.values());
        }
    }

    private static final class DerivedAttribution {
        private final Map<String, String> relationInstanceRoles;
        private final Map<String, String> predicateRoles;

        private DerivedAttribution(Map<String, String> relationInstanceRoles, Map<String, String> predicateRoles) {
            this.relationInstanceRoles = relationInstanceRoles;
            this.predicateRoles = predicateRoles;
        }
    }
}
