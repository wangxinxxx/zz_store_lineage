package com.zhuanzhuan.lineage.spark.listener;

import com.zhuanzhuan.lineage.app.LineageCaptureService;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StoreLineageQueryExecutionListener implements QueryExecutionListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(StoreLineageQueryExecutionListener.class);

    private final LineageCaptureService captureService;

    public StoreLineageQueryExecutionListener() {
        this(new LineageCaptureService());
    }

    public StoreLineageQueryExecutionListener(LineageCaptureService captureService) {
        this.captureService = captureService;
    }

    @Override
    public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
        swallow("onSuccess", funcName, () -> captureService.captureSuccess(funcName, qe, durationNs));
    }

    @Override
    public void onFailure(String funcName, QueryExecution qe, Exception exception) {
        swallow("onFailure", funcName, () -> captureService.captureFailure(funcName, qe, exception));
    }

    public LineageCaptureService getCaptureService() {
        return captureService;
    }

    private void swallow(String phase, String funcName, Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception error) {
            LOGGER.warn("Lineage listener ignored an internal error during {} for funcName={}", phase, funcName, error);
        }
    }
}
