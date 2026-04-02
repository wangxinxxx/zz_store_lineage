package com.zhuanzhuan.lineage.model;

public final class ColumnInstanceNode {
    private final String nodeId;
    private final String columnId;
    private final String columnName;
    private final String scopeId;
    private final String relationInstanceId;
    private final String instanceType;
    private final String dataType;
    private final Integer ordinal;
    private final String instanceRole;
    private final boolean output;
    private final String queryBlockId;
    private final String planNodeId;

    public ColumnInstanceNode(
            String nodeId,
            String columnId,
            String columnName,
            String scopeId,
            String relationInstanceId,
            String instanceType,
            String dataType,
            Integer ordinal
    ) {
        this(
                nodeId,
                columnId,
                columnName,
                scopeId,
                relationInstanceId,
                instanceType,
                dataType,
                ordinal,
                null,
                null,
                scopeId,
                null
        );
    }

    public ColumnInstanceNode(
            String nodeId,
            String columnId,
            String columnName,
            String scopeId,
            String relationInstanceId,
            String instanceType,
            String dataType,
            Integer ordinal,
            String instanceRole,
            Boolean output,
            String queryBlockId,
            String planNodeId
    ) {
        this.nodeId = nodeId;
        this.columnId = columnId;
        this.columnName = columnName;
        this.scopeId = scopeId;
        this.relationInstanceId = relationInstanceId;
        this.instanceType = instanceType;
        this.dataType = dataType;
        this.ordinal = ordinal;
        this.instanceRole = instanceRole;
        this.output = output != null ? output.booleanValue() : "OUTPUT".equalsIgnoreCase(instanceType);
        this.queryBlockId = queryBlockId;
        this.planNodeId = planNodeId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getColumnId() {
        return columnId;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getScopeId() {
        return scopeId;
    }

    public String getRelationInstanceId() {
        return relationInstanceId;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public String getDataType() {
        return dataType;
    }

    public Integer getOrdinal() {
        return ordinal;
    }

    public String getInstanceRole() {
        return instanceRole;
    }

    public boolean isOutput() {
        return output;
    }

    public String getQueryBlockId() {
        return queryBlockId;
    }

    public String getPlanNodeId() {
        return planNodeId;
    }
}
