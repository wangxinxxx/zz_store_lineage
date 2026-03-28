package com.zhuanzhuan.lineage.model;

public final class LineageWarning {
    private final String code;
    private final String message;

    public LineageWarning(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
