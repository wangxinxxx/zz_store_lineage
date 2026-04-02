package com.zhuanzhuan.lineage.model;

public final class ExpressionNode {
    private final String nodeId;
    private final String expressionType;
    private final String expressionSql;
    private final String normalizedExpression;
    private final String expressionCategory;
    private final String displaySql;
    private final boolean blackBox;
    private final String queryBlockId;
    private final String planNodeId;

    public ExpressionNode(String nodeId, String expressionType, String expressionSql, String normalizedExpression) {
        this(nodeId, expressionType, expressionSql, normalizedExpression, expressionType, expressionSql, false, null, null);
    }

    public ExpressionNode(
            String nodeId,
            String expressionType,
            String expressionSql,
            String normalizedExpression,
            String expressionCategory,
            String displaySql,
            boolean blackBox,
            String queryBlockId,
            String planNodeId
    ) {
        this.nodeId = nodeId;
        this.expressionType = expressionType;
        this.expressionSql = expressionSql;
        this.normalizedExpression = normalizedExpression;
        this.expressionCategory = expressionCategory;
        this.displaySql = displaySql;
        this.blackBox = blackBox;
        this.queryBlockId = queryBlockId;
        this.planNodeId = planNodeId;
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

    public String getExpressionCategory() {
        return expressionCategory;
    }

    public String getDisplaySql() {
        return displaySql;
    }

    public boolean isBlackBox() {
        return blackBox;
    }

    public String getQueryBlockId() {
        return queryBlockId;
    }

    public String getPlanNodeId() {
        return planNodeId;
    }
}
