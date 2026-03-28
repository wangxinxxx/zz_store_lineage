package com.zhuanzhuan.lineage.model;

public final class TableLineageEdge {
    private final TableRef sourceTable;
    private final TableRef targetTable;
    private final String taskId;
    private final String runId;
    private final String eventId;
    private final String writeMode;
    private final long captureTimeEpochMs;
    private final String confidence;

    public TableLineageEdge(
            TableRef sourceTable,
            TableRef targetTable,
            String taskId,
            String runId,
            String eventId,
            String writeMode,
            long captureTimeEpochMs,
            String confidence
    ) {
        this.sourceTable = sourceTable;
        this.targetTable = targetTable;
        this.taskId = taskId;
        this.runId = runId;
        this.eventId = eventId;
        this.writeMode = writeMode;
        this.captureTimeEpochMs = captureTimeEpochMs;
        this.confidence = confidence;
    }

    public TableRef getSourceTable() {
        return sourceTable;
    }

    public TableRef getTargetTable() {
        return targetTable;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getRunId() {
        return runId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public long getCaptureTimeEpochMs() {
        return captureTimeEpochMs;
    }

    public String getConfidence() {
        return confidence;
    }
}
