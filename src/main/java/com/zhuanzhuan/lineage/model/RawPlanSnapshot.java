package com.zhuanzhuan.lineage.model;

public final class RawPlanSnapshot {
    private final String logicalPlanText;
    private final String analyzedPlanText;
    private final String optimizedPlanText;
    private final String executedPlanText;

    public RawPlanSnapshot(
            String logicalPlanText,
            String analyzedPlanText,
            String optimizedPlanText,
            String executedPlanText
    ) {
        this.logicalPlanText = logicalPlanText;
        this.analyzedPlanText = analyzedPlanText;
        this.optimizedPlanText = optimizedPlanText;
        this.executedPlanText = executedPlanText;
    }

    public String getLogicalPlanText() {
        return logicalPlanText;
    }

    public String getAnalyzedPlanText() {
        return analyzedPlanText;
    }

    public String getOptimizedPlanText() {
        return optimizedPlanText;
    }

    public String getExecutedPlanText() {
        return executedPlanText;
    }
}
