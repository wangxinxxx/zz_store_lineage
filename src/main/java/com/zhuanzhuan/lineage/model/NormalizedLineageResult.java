package com.zhuanzhuan.lineage.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class NormalizedLineageResult {
    private final String eventId;
    private final long captureTimeEpochMs;
    private final String statementType;
    private final List<TableRef> inputTables;
    private final List<TableRef> outputTables;
    private final List<TableLineageEdge> tableEdges;
    private final List<ColumnNode> columnNodes;
    private final List<ExpressionNode> expressionNodes;
    private final List<ScopeNode> scopeNodes;
    private final List<LiteralNode> literalNodes;
    private final List<OperatorInstanceNode> operatorInstanceNodes;
    private final List<ColumnInstanceNode> columnInstanceNodes;
    private final List<RelationInstanceNode> relationInstanceNodes;
    private final List<PredicateNode> predicateNodes;
    private final List<LineageGraphEdge> graphEdges;
    private final List<LineageWarning> warnings;

    public NormalizedLineageResult(
            String eventId,
            long captureTimeEpochMs,
            String statementType,
            List<TableRef> inputTables,
            List<TableRef> outputTables,
            List<TableLineageEdge> tableEdges,
            List<ColumnNode> columnNodes,
            List<ExpressionNode> expressionNodes,
            List<ScopeNode> scopeNodes,
            List<LiteralNode> literalNodes,
            List<OperatorInstanceNode> operatorInstanceNodes,
            List<ColumnInstanceNode> columnInstanceNodes,
            List<RelationInstanceNode> relationInstanceNodes,
            List<PredicateNode> predicateNodes,
            List<LineageGraphEdge> graphEdges,
            List<LineageWarning> warnings
    ) {
        this.eventId = eventId;
        this.captureTimeEpochMs = captureTimeEpochMs;
        this.statementType = statementType;
        this.inputTables = unmodifiableCopy(inputTables);
        this.outputTables = unmodifiableCopy(outputTables);
        this.tableEdges = unmodifiableCopy(tableEdges);
        this.columnNodes = unmodifiableCopy(columnNodes);
        this.expressionNodes = unmodifiableCopy(expressionNodes);
        this.scopeNodes = unmodifiableCopy(scopeNodes);
        this.literalNodes = unmodifiableCopy(literalNodes);
        this.operatorInstanceNodes = unmodifiableCopy(operatorInstanceNodes);
        this.columnInstanceNodes = unmodifiableCopy(columnInstanceNodes);
        this.relationInstanceNodes = unmodifiableCopy(relationInstanceNodes);
        this.predicateNodes = unmodifiableCopy(predicateNodes);
        this.graphEdges = unmodifiableCopy(graphEdges);
        this.warnings = unmodifiableCopy(warnings);
    }

    public String getEventId() {
        return eventId;
    }

    public long getCaptureTimeEpochMs() {
        return captureTimeEpochMs;
    }

    public String getStatementType() {
        return statementType;
    }

    public List<TableRef> getInputTables() {
        return inputTables;
    }

    public List<TableRef> getOutputTables() {
        return outputTables;
    }

    public List<TableLineageEdge> getTableEdges() {
        return tableEdges;
    }

    public List<ColumnNode> getColumnNodes() {
        return columnNodes;
    }

    public List<ExpressionNode> getExpressionNodes() {
        return expressionNodes;
    }

    public List<ScopeNode> getScopeNodes() {
        return scopeNodes;
    }

    public List<LiteralNode> getLiteralNodes() {
        return literalNodes;
    }

    public List<OperatorInstanceNode> getOperatorInstanceNodes() {
        return operatorInstanceNodes;
    }

    public List<ColumnInstanceNode> getColumnInstanceNodes() {
        return columnInstanceNodes;
    }

    public List<RelationInstanceNode> getRelationInstanceNodes() {
        return relationInstanceNodes;
    }

    public List<PredicateNode> getPredicateNodes() {
        return predicateNodes;
    }

    public List<LineageGraphEdge> getGraphEdges() {
        return graphEdges;
    }

    public List<LineageWarning> getWarnings() {
        return warnings;
    }

    private static <T> List<T> unmodifiableCopy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }
}
