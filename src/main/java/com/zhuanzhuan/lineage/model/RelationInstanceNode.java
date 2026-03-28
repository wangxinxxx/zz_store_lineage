package com.zhuanzhuan.lineage.model;

public final class RelationInstanceNode {
    private final String nodeId;
    private final String instanceName;
    private final String scopeId;
    private final String sourceTableId;
    private final String sourceType;
    private final String aliasName;
    private final String planNodeName;

    public RelationInstanceNode(
            String nodeId,
            String instanceName,
            String scopeId,
            String sourceTableId,
            String sourceType,
            String aliasName,
            String planNodeName
    ) {
        this.nodeId = nodeId;
        this.instanceName = instanceName;
        this.scopeId = scopeId;
        this.sourceTableId = sourceTableId;
        this.sourceType = sourceType;
        this.aliasName = aliasName;
        this.planNodeName = planNodeName;
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
}
