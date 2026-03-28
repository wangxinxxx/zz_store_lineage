package com.zhuanzhuan.lineage.model;

public final class LiteralNode {
    private final String nodeId;
    private final String literalType;
    private final String literalValue;
    private final String normalizedValue;

    public LiteralNode(String nodeId, String literalType, String literalValue, String normalizedValue) {
        this.nodeId = nodeId;
        this.literalType = literalType;
        this.literalValue = literalValue;
        this.normalizedValue = normalizedValue;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getLiteralType() {
        return literalType;
    }

    public String getLiteralValue() {
        return literalValue;
    }

    public String getNormalizedValue() {
        return normalizedValue;
    }
}
