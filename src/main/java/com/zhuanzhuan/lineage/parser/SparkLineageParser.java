package com.zhuanzhuan.lineage.parser;

import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import org.apache.spark.sql.execution.QueryExecution;

public interface SparkLineageParser {
    NormalizedLineageResult parse(ExecutionCaptureEvent event, QueryExecution qe);
}
