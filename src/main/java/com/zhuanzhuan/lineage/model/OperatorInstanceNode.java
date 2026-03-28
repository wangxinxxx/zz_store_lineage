package com.zhuanzhuan.lineage.model;

public final class OperatorInstanceNode {
    private final String nodeId;
    private final String scopeId;
    private final String operatorType;
    private final String operatorSubType;
    private final String operatorPath;
    private final String parentOperatorId;
    private final String planNodeName;

    public OperatorInstanceNode(
            String nodeId,
            String scopeId,
            String operatorType,
            String operatorSubType,
            String operatorPath,
            String parentOperatorId,
            String planNodeName
    ) {
        this.nodeId = nodeId;
        this.scopeId = scopeId;
        this.operatorType = operatorType;
        this.operatorSubType = operatorSubType;
        this.operatorPath = operatorPath;
        this.parentOperatorId = parentOperatorId;
        this.planNodeName = planNodeName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getScopeId() {
        return scopeId;
    }

    public String getOperatorType() {
        return operatorType;
    }

    public String getOperatorSubType() {
        return operatorSubType;
    }

    public String getOperatorPath() {
        return operatorPath;
    }

    public String getParentOperatorId() {
        return parentOperatorId;
    }

    public String getPlanNodeName() {
        return planNodeName;
    }
}
