package com.zhuanzhuan.lineage.model;

public final class ExecutionCaptureEvent {
    private final String eventId;
    private final ExecutionStatus status;
    private final LineageTaskContext taskContext;
    private final SparkAppContext appContext;
    private final String funcName;
    private final Long durationNs;
    private final long captureTimeEpochMs;
    private final String errorMessage;
    private final RawPlanSnapshot planSnapshot;

    public ExecutionCaptureEvent(
            String eventId,
            ExecutionStatus status,
            LineageTaskContext taskContext,
            SparkAppContext appContext,
            String funcName,
            Long durationNs,
            long captureTimeEpochMs,
            String errorMessage,
            RawPlanSnapshot planSnapshot
    ) {
        this.eventId = eventId;
        this.status = status;
        this.taskContext = taskContext;
        this.appContext = appContext;
        this.funcName = funcName;
        this.durationNs = durationNs;
        this.captureTimeEpochMs = captureTimeEpochMs;
        this.errorMessage = errorMessage;
        this.planSnapshot = planSnapshot;
    }

    public String getEventId() {
        return eventId;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public LineageTaskContext getTaskContext() {
        return taskContext;
    }

    public SparkAppContext getAppContext() {
        return appContext;
    }

    public String getFuncName() {
        return funcName;
    }

    public Long getDurationNs() {
        return durationNs;
    }

    public long getCaptureTimeEpochMs() {
        return captureTimeEpochMs;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public RawPlanSnapshot getPlanSnapshot() {
        return planSnapshot;
    }
}
