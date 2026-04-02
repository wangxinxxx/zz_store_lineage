package com.zhuanzhuan.lineage.model;

public final class PredicateNode {
    private final String nodeId;
    private final String predicateType;
    private final String predicateSql;
    private final String normalizedPredicate;
    private final String scopeId;
    private final String planNodeName;
    private final String predicateRole;
    private final String displaySql;
    private final String queryBlockId;
    private final String planNodeId;

    public PredicateNode(
            String nodeId,
            String predicateType,
            String predicateSql,
            String normalizedPredicate,
            String scopeId,
            String planNodeName
    ) {
        this(
                nodeId,
                predicateType,
                predicateSql,
                normalizedPredicate,
                scopeId,
                planNodeName,
                predicateType,
                predicateSql,
                scopeId,
                null
        );
    }

    public PredicateNode(
            String nodeId,
            String predicateType,
            String predicateSql,
            String normalizedPredicate,
            String scopeId,
            String planNodeName,
            String predicateRole,
            String displaySql,
            String queryBlockId,
            String planNodeId
    ) {
        this.nodeId = nodeId;
        this.predicateType = predicateType;
        this.predicateSql = predicateSql;
        this.normalizedPredicate = normalizedPredicate;
        this.scopeId = scopeId;
        this.planNodeName = planNodeName;
        this.predicateRole = predicateRole;
        this.displaySql = displaySql;
        this.queryBlockId = queryBlockId;
        this.planNodeId = planNodeId;
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

    public String getPredicateRole() {
        return predicateRole;
    }

    public String getDisplaySql() {
        return displaySql;
    }

    public String getQueryBlockId() {
        return queryBlockId;
    }

    public String getPlanNodeId() {
        return planNodeId;
    }
}
