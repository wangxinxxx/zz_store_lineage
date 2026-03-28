package com.zhuanzhuan.lineage.model;

public final class SparkAppContext {
    private final String appId;
    private final String appName;
    private final String user;
    private final String master;

    public SparkAppContext(String appId, String appName, String user, String master) {
        this.appId = appId;
        this.appName = appName;
        this.user = user;
        this.master = master;
    }

    public String getAppId() {
        return appId;
    }

    public String getAppName() {
        return appName;
    }

    public String getUser() {
        return user;
    }

    public String getMaster() {
        return master;
    }
}
