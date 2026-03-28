package com.zhuanzhuan.lineage.model;

public final class ExpressionNode {
    private final String nodeId;
    private final String expressionType;
    private final String expressionSql;
    private final String normalizedExpression;

    public ExpressionNode(String nodeId, String expressionType, String expressionSql, String normalizedExpression) {
        this.nodeId = nodeId;
        this.expressionType = expressionType;
        this.expressionSql = expressionSql;
        this.normalizedExpression = normalizedExpression;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public String getExpressionSql() {
        return expressionSql;
    }

    public String getNormalizedExpression() {
        return normalizedExpression;
    }
}
