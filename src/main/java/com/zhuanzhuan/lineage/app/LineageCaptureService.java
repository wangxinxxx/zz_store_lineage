package com.zhuanzhuan.lineage.app;

import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExecutionStatus;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import com.zhuanzhuan.lineage.model.RawPlanSnapshot;
import com.zhuanzhuan.lineage.model.SparkAppContext;
import com.zhuanzhuan.lineage.parser.DefaultSparkLineageParser;
import com.zhuanzhuan.lineage.parser.SparkLineageParser;
import com.zhuanzhuan.lineage.spark.context.TaskContextProvider;
import com.zhuanzhuan.lineage.storage.LineageStorage;
import com.zhuanzhuan.lineage.storage.LineageStorageFactory;
import org.apache.spark.sql.execution.QueryExecution;

import java.util.function.Supplier;

public final class LineageCaptureService {
    private final SparkLineageParser parser;
    private final LineageStorage storage;
    private final EventIdGenerator eventIdGenerator;
    private final TaskContextProvider taskContextProvider;

    public LineageCaptureService() {
        this(new DefaultSparkLineageParser(), LineageStorageFactory.createDefault(), new DefaultEventIdGenerator(), TaskContextProvider.DEFAULT);
    }

    public LineageCaptureService(
            SparkLineageParser parser,
            LineageStorage storage,
            EventIdGenerator eventIdGenerator,
            TaskContextProvider taskContextProvider
    ) {
        this.parser = parser;
        this.storage = storage;
        this.eventIdGenerator = eventIdGenerator;
        this.taskContextProvider = taskContextProvider;
    }

    public void captureSuccess(String funcName, QueryExecution qe, long durationNs) {
        ExecutionCaptureEvent event = buildEvent(ExecutionStatus.SUCCESS, funcName, qe, durationNs, null);
        storage.saveCapture(event);
        storage.saveLineage(parser.parse(event, qe));
    }

    public void captureFailure(String funcName, QueryExecution qe, Exception exception) {
        ExecutionCaptureEvent event = buildEvent(
                ExecutionStatus.FAILURE,
                funcName,
                qe,
                null,
                exception == null ? null : exception.getMessage()
        );
        storage.saveCapture(event);
    }

    public LineageStorage getStorage() {
        return storage;
    }

    private ExecutionCaptureEvent buildEvent(
            ExecutionStatus status,
            String funcName,
            QueryExecution qe,
            Long durationNs,
            String errorMessage
    ) {
        LineageTaskContext taskContext = taskContextProvider.current(qe);

        return new ExecutionCaptureEvent(
                eventIdGenerator.generate(status, funcName, qe, taskContext),
                status,
                taskContext,
                new SparkAppContext(
                        qe.sparkSession().sparkContext().applicationId(),
                        qe.sparkSession().sparkContext().appName(),
                        qe.sparkSession().sparkContext().sparkUser(),
                        qe.sparkSession().sparkContext().master()
                ),
                funcName,
                durationNs,
                System.currentTimeMillis(),
                errorMessage,
                new RawPlanSnapshot(
                        planText(() -> qe.logical().toString()),
                        planText(() -> qe.analyzed().toString()),
                        planText(() -> qe.optimizedPlan().toString()),
                        planText(() -> qe.executedPlan().toString())
                )
        );
    }

    private String planText(Supplier<String> supplier) {
        try {
            return supplier.get();
        } catch (Exception error) {
            return "";
        }
    }
}
