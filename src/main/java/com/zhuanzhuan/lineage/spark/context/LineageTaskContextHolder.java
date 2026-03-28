package com.zhuanzhuan.lineage.spark.context;

import com.zhuanzhuan.lineage.model.LineageTaskContext;

import java.util.Optional;
import java.util.function.Supplier;

public final class LineageTaskContextHolder {
    private static final ThreadLocal<LineageTaskContext> HOLDER = new ThreadLocal<>();

    private LineageTaskContextHolder() {
    }

    public static Optional<LineageTaskContext> current() {
        return Optional.ofNullable(HOLDER.get());
    }

    public static void set(LineageTaskContext context) {
        HOLDER.set(context);
    }

    public static void clear() {
        HOLDER.remove();
    }

    public static <T> T withContext(LineageTaskContext context, Supplier<T> supplier) {
        set(context);
        try {
            return supplier.get();
        } finally {
            clear();
        }
    }

    public static void withContext(LineageTaskContext context, Runnable runnable) {
        set(context);
        try {
            runnable.run();
        } finally {
            clear();
        }
    }
}
