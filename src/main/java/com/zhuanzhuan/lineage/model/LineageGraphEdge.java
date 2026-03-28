package com.zhuanzhuan.lineage.model;

public final class LineageGraphEdge {
    private final String sourceNodeId;
    private final String targetNodeId;
    private final String edgeType;
    private final String role;
    private final String eventId;

    public LineageGraphEdge(String sourceNodeId, String targetNodeId, String edgeType, String role, String eventId) {
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.edgeType = edgeType;
        this.role = role;
        this.eventId = eventId;
    }

    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public String getTargetNodeId() {
        return targetNodeId;
    }

    public String getEdgeType() {
        return edgeType;
    }

    public String getRole() {
        return role;
    }

    public String getEventId() {
        return eventId;
    }
}
