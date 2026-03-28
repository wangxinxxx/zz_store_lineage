package com.zhuanzhuan.lineage.model;

public final class ScopeNode {
    private final String nodeId;
    private final String scopeName;
    private final String scopeType;
    private final String parentScopeId;
    private final String planNodeName;

    public ScopeNode(String nodeId, String scopeName, String scopeType, String parentScopeId, String planNodeName) {
        this.nodeId = nodeId;
        this.scopeName = scopeName;
        this.scopeType = scopeType;
        this.parentScopeId = parentScopeId;
        this.planNodeName = planNodeName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getScopeName() {
        return scopeName;
    }

    public String getScopeType() {
        return scopeType;
    }

    public String getParentScopeId() {
        return parentScopeId;
    }

    public String getPlanNodeName() {
        return planNodeName;
    }
}
