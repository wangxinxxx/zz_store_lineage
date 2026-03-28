package com.zhuanzhuan.lineage.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ColumnNode {
    private final String nodeId;
    private final String ownerId;
    private final String ownerType;
    private final String name;
    private final String dataType;
    private final List<String> qualifier;

    public ColumnNode(
            String nodeId,
            String ownerId,
            String ownerType,
            String name,
            String dataType,
            List<String> qualifier
    ) {
        this.nodeId = nodeId;
        this.ownerId = ownerId;
        this.ownerType = ownerType;
        this.name = name;
        this.dataType = dataType;
        this.qualifier = Collections.unmodifiableList(new ArrayList<>(qualifier));
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public String getOwnerType() {
        return ownerType;
    }

    public String getName() {
        return name;
    }

    public String getDataType() {
        return dataType;
    }

    public List<String> getQualifier() {
        return qualifier;
    }
}
