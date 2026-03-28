package com.zhuanzhuan.lineage.spark.context;

import com.zhuanzhuan.lineage.model.LineageTaskContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.QueryExecution;

public interface TaskContextProvider {
    LineageTaskContext current(QueryExecution qe);

    final class Keys {
        public static final String TASK_ID = "zz.lineage.task.id";
        public static final String TASK_NAME = "zz.lineage.task.name";
        public static final String RUN_ID = "zz.lineage.run.id";
        public static final String BIZ_DATE = "zz.lineage.biz.date";
        public static final String OWNER = "zz.lineage.owner";
        public static final String SCRIPT_PATH = "zz.lineage.script.path";

        private Keys() {
        }
    }

    final class ThreadLocalTaskContextProvider implements TaskContextProvider {
        @Override
        public LineageTaskContext current(QueryExecution qe) {
            return LineageTaskContextHolder.current().orElse(LineageTaskContext.empty());
        }
    }

    final class SparkPropertyTaskContextProvider implements TaskContextProvider {
        @Override
        public LineageTaskContext current(QueryExecution qe) {
            SparkContext sparkContext = qe.sparkSession().sparkContext();
            SparkConf conf = sparkContext.getConf();

            return new LineageTaskContext(
                    read(sparkContext, conf, Keys.TASK_ID),
                    read(sparkContext, conf, Keys.TASK_NAME),
                    read(sparkContext, conf, Keys.RUN_ID),
                    read(sparkContext, conf, Keys.BIZ_DATE),
                    read(sparkContext, conf, Keys.OWNER),
                    read(sparkContext, conf, Keys.SCRIPT_PATH)
            );
        }

        private String read(SparkContext sparkContext, SparkConf conf, String key) {
            String localProperty = sparkContext.getLocalProperty(key);
            if (localProperty != null && !localProperty.trim().isEmpty()) {
                return localProperty;
            }
            if (conf.contains(key)) {
                String confValue = conf.get(key);
                if (confValue != null && !confValue.trim().isEmpty()) {
                    return confValue;
                }
            }
            return null;
        }
    }

    final class CompositeTaskContextProvider implements TaskContextProvider {
        private final TaskContextProvider[] providers;

        public CompositeTaskContextProvider(TaskContextProvider... providers) {
            this.providers = providers;
        }

        @Override
        public LineageTaskContext current(QueryExecution qe) {
            LineageTaskContext current = LineageTaskContext.empty();
            for (TaskContextProvider provider : providers) {
                current = current.merge(provider.current(qe));
            }
            return current;
        }
    }

    TaskContextProvider DEFAULT = new CompositeTaskContextProvider(
            new ThreadLocalTaskContextProvider(),
            new SparkPropertyTaskContextProvider()
    );
}
