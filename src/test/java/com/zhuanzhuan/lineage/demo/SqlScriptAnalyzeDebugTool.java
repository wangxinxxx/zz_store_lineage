package com.zhuanzhuan.lineage.demo;

import com.zhuanzhuan.lineage.app.SqlScriptImportService;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class SqlScriptAnalyzeDebugTool {
    private SqlScriptAnalyzeDebugTool() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: SqlScriptAnalyzeDebugTool <sql-file>");
            return;
        }

        Path scriptPath = Paths.get(args[0]).toAbsolutePath().normalize();
        String sql = new String(Files.readAllBytes(scriptPath), StandardCharsets.UTF_8).trim();

        try (SqlScriptImportService service = SqlScriptImportService.createDefault()) {
            Method method = SqlScriptImportService.class.getDeclaredMethod("analyzeStatement", String.class);
            method.setAccessible(true);
            try {
                Object bundle = method.invoke(service, sql);
                System.out.println("analysisBundle=" + bundle);
            } catch (Throwable error) {
                Throwable root = error;
                if (error instanceof java.lang.reflect.InvocationTargetException
                        && ((java.lang.reflect.InvocationTargetException) error).getTargetException() != null) {
                    root = ((java.lang.reflect.InvocationTargetException) error).getTargetException();
                }
                root.printStackTrace(System.out);
                throw new RuntimeException(root);
            }
        }
    }
}
