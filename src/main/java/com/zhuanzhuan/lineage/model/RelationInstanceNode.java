package com.zhuanzhuan.lineage.model;

public final class RelationInstanceNode {
    private final String nodeId;
    private final String instanceName;
    private final String scopeId;
    private final String sourceTableId;
    private final String sourceType;
    private final String aliasName;
    private final String planNodeName;
    private final String queryBlockId;
    private final String planNodeId;
    private final String joinType;
    private final String nullSupplySide;
    private final boolean subquerySource;

    public RelationInstanceNode(
            String nodeId,
            String instanceName,
            String scopeId,
            String sourceTableId,
            String sourceType,
            String aliasName,
            String planNodeName
    ) {
        this(
                nodeId,
                instanceName,
                scopeId,
                sourceTableId,
                sourceType,
                aliasName,
                planNodeName,
                scopeId,
                null,
                null,
                null,
                "named_plan".equalsIgnoreCase(sourceType)
        );
    }

    public RelationInstanceNode(
            String nodeId,
            String instanceName,
            String scopeId,
            String sourceTableId,
            String sourceType,
            String aliasName,
            String planNodeName,
            String queryBlockId,
            String planNodeId,
            String joinType,
            String nullSupplySide,
            boolean subquerySource
    ) {
        this.nodeId = nodeId;
        this.instanceName = instanceName;
        this.scopeId = scopeId;
        this.sourceTableId = sourceTableId;
        this.sourceType = sourceType;
        this.aliasName = aliasName;
        this.planNodeName = planNodeName;
        this.queryBlockId = queryBlockId;
        this.planNodeId = planNodeId;
        this.joinType = joinType;
        this.nullSupplySide = nullSupplySide;
        this.subquerySource = subquerySource;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getScopeId() {
        return scopeId;
    }

    public String getSourceTableId() {
        return sourceTableId;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getAliasName() {
        return aliasName;
    }

    public String getPlanNodeName() {
        return planNodeName;
    }

    public String getQueryBlockId() {
        return queryBlockId;
    }

    public String getPlanNodeId() {
        return planNodeId;
    }

    public String getJoinType() {
        return joinType;
    }

    public String getNullSupplySide() {
        return nullSupplySide;
    }

    public boolean isSubquerySource() {
        return subquerySource;
    }
}
