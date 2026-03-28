package com.zhuanzhuan.lineage.app;

import com.zhuanzhuan.lineage.common.HashUtils;
import com.zhuanzhuan.lineage.model.ExecutionStatus;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import org.apache.spark.sql.execution.QueryExecution;

public final class DefaultEventIdGenerator implements EventIdGenerator {
    @Override
    public String generate(ExecutionStatus status, String funcName, QueryExecution qe, LineageTaskContext taskContext) {
        String runIdentity = taskContext.getRunId() != null
                ? taskContext.getRunId()
                : (taskContext.getTaskId() != null ? taskContext.getTaskId() : "unknown-run");

        String statementFingerprint;
        try {
            statementFingerprint = qe.analyzed().toString();
        } catch (Exception ignored) {
            try {
                statementFingerprint = qe.logical().toString();
            } catch (Exception secondIgnored) {
                statementFingerprint = funcName;
            }
        }

        return HashUtils.sha1(runIdentity + "|" + status.name() + "|" + funcName + "|" + statementFingerprint);
    }
}
