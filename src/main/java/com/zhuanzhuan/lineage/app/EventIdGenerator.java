package com.zhuanzhuan.lineage.app;

import com.zhuanzhuan.lineage.model.ExecutionStatus;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import org.apache.spark.sql.execution.QueryExecution;

public interface EventIdGenerator {
    String generate(ExecutionStatus status, String funcName, QueryExecution qe, LineageTaskContext taskContext);
}
