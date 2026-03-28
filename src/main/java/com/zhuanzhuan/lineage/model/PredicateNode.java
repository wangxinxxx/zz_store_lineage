package com.zhuanzhuan.lineage.model;

public final class PredicateNode {
    private final String nodeId;
    private final String predicateType;
    private final String predicateSql;
    private final String normalizedPredicate;
    private final String scopeId;
    private final String planNodeName;

    public PredicateNode(
            String nodeId,
            String predicateType,
            String predicateSql,
            String normalizedPredicate,
            String scopeId,
            String planNodeName
    ) {
        this.nodeId = nodeId;
        this.predicateType = predicateType;
        this.predicateSql = predicateSql;
        this.normalizedPredicate = normalizedPredicate;
        this.scopeId = scopeId;
        this.planNodeName = planNodeName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getPredicateType() {
        return predicateType;
    }

    public String getPredicateSql() {
        return predicateSql;
    }

    public String getNormalizedPredicate() {
        return normalizedPredicate;
    }

    public String getScopeId() {
        return scopeId;
    }

    public String getPlanNodeName() {
        return planNodeName;
    }
}
