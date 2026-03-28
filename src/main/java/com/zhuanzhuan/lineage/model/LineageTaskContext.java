package com.zhuanzhuan.lineage.model;

public final class LineageTaskContext {
    private static final LineageTaskContext EMPTY = new LineageTaskContext(null, null, null, null, null, null);

    private final String taskId;
    private final String taskName;
    private final String runId;
    private final String bizDate;
    private final String owner;
    private final String scriptPath;

    public LineageTaskContext(
            String taskId,
            String taskName,
            String runId,
            String bizDate,
            String owner,
            String scriptPath
    ) {
        this.taskId = blankToNull(taskId);
        this.taskName = blankToNull(taskName);
        this.runId = blankToNull(runId);
        this.bizDate = blankToNull(bizDate);
        this.owner = blankToNull(owner);
        this.scriptPath = blankToNull(scriptPath);
    }

    public static LineageTaskContext empty() {
        return EMPTY;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getRunId() {
        return runId;
    }

    public String getBizDate() {
        return bizDate;
    }

    public String getOwner() {
        return owner;
    }

    public String getScriptPath() {
        return scriptPath;
    }

    public boolean isEmpty() {
        return taskId == null
                && taskName == null
                && runId == null
                && bizDate == null
                && owner == null
                && scriptPath == null;
    }

    public LineageTaskContext merge(LineageTaskContext fallback) {
        return new LineageTaskContext(
                firstNonBlank(taskId, fallback.taskId),
                firstNonBlank(taskName, fallback.taskName),
                firstNonBlank(runId, fallback.runId),
                firstNonBlank(bizDate, fallback.bizDate),
                firstNonBlank(owner, fallback.owner),
                firstNonBlank(scriptPath, fallback.scriptPath)
        );
    }

    private static String firstNonBlank(String preferred, String fallback) {
        return blankToNull(preferred) != null ? blankToNull(preferred) : blankToNull(fallback);
    }

    private static String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
