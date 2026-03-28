package com.zhuanzhuan.lineage.storage;

import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;
import com.zhuanzhuan.lineage.storage.nebula.NebulaLineageStorage;

public final class LineageStorageFactory {
    public static final String STORAGE_TYPE_PROPERTY = "zz.lineage.storage.type";
    public static final String STORAGE_TYPE_ENV = "ZZ_LINEAGE_STORAGE_TYPE";

    private LineageStorageFactory() {
    }

    public static LineageStorage createDefault() {
        String storageType = read(STORAGE_TYPE_PROPERTY, STORAGE_TYPE_ENV);
        if ("nebula".equalsIgnoreCase(storageType)) {
            return new NebulaLineageStorage(NebulaGraphConfig.fromSystem());
        }
        return new LineageStorage.InMemoryLineageStorage();
    }

    private static String read(String propertyKey, String envKey) {
        String property = System.getProperty(propertyKey);
        if (property != null && !property.trim().isEmpty()) {
            return property.trim();
        }
        String env = System.getenv(envKey);
        if (env != null && !env.trim().isEmpty()) {
            return env.trim();
        }
        return null;
    }
}
