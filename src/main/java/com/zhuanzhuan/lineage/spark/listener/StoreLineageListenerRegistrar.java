package com.zhuanzhuan.lineage.spark.listener;

import com.zhuanzhuan.lineage.app.LineageCaptureService;
import org.apache.spark.sql.SparkSession;

public final class StoreLineageListenerRegistrar {
    private StoreLineageListenerRegistrar() {
    }

    public static StoreLineageQueryExecutionListener register(SparkSession spark) {
        return register(spark, new LineageCaptureService());
    }

    public static StoreLineageQueryExecutionListener register(SparkSession spark, LineageCaptureService captureService) {
        StoreLineageQueryExecutionListener listener = new StoreLineageQueryExecutionListener(captureService);
        spark.listenerManager().register(listener);
        return listener;
    }
}
