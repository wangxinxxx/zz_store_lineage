package com.zhuanzhuan.lineage.app;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class SqlScriptImportTool {
    private SqlScriptImportTool() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        boolean clearOnly = "--clear-only".equals(args[0]);
        boolean clearFirst = "--clear-first".equals(args[0]);
        boolean syncMetadataOnly = "--sync-metadata".equals(args[0]);

        try (SqlScriptImportService service = SqlScriptImportService.createDefault()) {
            if (clearOnly) {
                service.clearStorage();
                System.out.println("Nebula storage cleared.");
                return;
            }

            Path scriptPath;
            if (clearFirst || syncMetadataOnly) {
                if (args.length < 2) {
                    printUsage();
                    return;
                }
                scriptPath = Paths.get(args[1]);
                if (clearFirst) {
                    service.clearStorage();
                }
            } else {
                scriptPath = Paths.get(args[0]);
            }

            if (syncMetadataOnly) {
                printMetadataSyncSummary(service.syncMetadata(scriptPath));
                return;
            }

            if (Files.isDirectory(scriptPath)) {
                printDirectorySummary(service.importDirectory(scriptPath, new SqlScriptImportService.ImportProgressListener() {
                    @Override
                    public void onScriptStart(Path sqlFile, int index, int totalScripts) {
                        System.out.println("Importing script [" + index + "/" + totalScripts + "]: " + sqlFile.toAbsolutePath());
                    }

                    @Override
                    public void onScriptFinish(SqlScriptImportService.ImportSummary summary, int index, int totalScripts) {
                        System.out.println(
                                "Finished script [" + index + "/" + totalScripts + "]: "
                                        + summary.getScriptPath().toAbsolutePath()
                                        + " | imported=" + summary.getImportedStatements()
                                        + " | failed=" + summary.getFailedStatements()
                                        + " | skipped=" + summary.getSkippedStatements()
                                        + " | durationMs=" + summary.getDurationMs()
                        );
                    }
                }));
            } else {
                printScriptSummary(service.importScript(scriptPath));
            }
        }
    }

    private static void printScriptSummary(SqlScriptImportService.ImportSummary summary) {
        System.out.println("Script import finished.");
        System.out.println("Script path        : " + summary.getScriptPath().toAbsolutePath());
        System.out.println("Total statements   : " + summary.getTotalStatements());
        System.out.println("Skipped statements : " + summary.getSkippedStatements());
        System.out.println("Imported statements: " + summary.getImportedStatements());
        System.out.println("Failed statements  : " + summary.getFailedStatements());
        System.out.println("Duration ms        : " + summary.getDurationMs());
        if (!summary.getFailures().isEmpty()) {
            System.out.println("Failures:");
            for (String failure : summary.getFailures()) {
                System.out.println("  " + failure);
            }
        }
    }

    private static void printDirectorySummary(SqlScriptImportService.DirectoryImportSummary summary) {
        System.out.println("Directory import finished.");
        System.out.println("Directory path     : " + summary.getDirectoryPath().toAbsolutePath());
        System.out.println("Total scripts      : " + summary.getTotalScripts());
        System.out.println("Successful scripts : " + summary.getSuccessfulScripts());
        System.out.println("Partial scripts    : " + summary.getPartialScripts());
        System.out.println("Failed scripts     : " + summary.getFailedScripts());
        System.out.println("Skipped scripts    : " + summary.getSkippedScripts());
        System.out.println("Total statements   : " + summary.getTotalStatements());
        System.out.println("Skipped statements : " + summary.getSkippedStatements());
        System.out.println("Imported statements: " + summary.getImportedStatements());
        System.out.println("Failed statements  : " + summary.getFailedStatements());
        System.out.println("Duration ms        : " + summary.getDurationMs());
        for (SqlScriptImportService.ImportSummary fileSummary : summary.getFileSummaries()) {
            if (fileSummary.getImportedStatements() == 0 && fileSummary.getFailedStatements() == 0) {
                System.out.println("Skipped file       : " + fileSummary.getScriptPath().toAbsolutePath());
                continue;
            }
            if (fileSummary.getFailedStatements() > 0) {
                System.out.println("Failures in " + fileSummary.getScriptPath().toAbsolutePath() + ":");
                for (String failure : fileSummary.getFailures()) {
                    System.out.println("  " + failure);
                }
            }
        }
    }

    private static void printMetadataSyncSummary(SqlScriptImportService.MetadataSyncSummary summary) {
        System.out.println("Metadata sync finished.");
        System.out.println("Target path         : " + summary.getTargetPath().toAbsolutePath());
        System.out.println("Total scripts       : " + summary.getTotalScripts());
        System.out.println("Referenced tables   : " + summary.getTotalReferencedTables());
        System.out.println("Cached before       : " + summary.getCachedBefore());
        System.out.println("Cached after        : " + summary.getCachedAfter());
        System.out.println("Newly cached        : " + summary.getNewlyCached());
        System.out.println("Referenced list     : " + (summary.getReferencedTablesPath() == null ? "" : summary.getReferencedTablesPath().toAbsolutePath()));
        System.out.println("Failures file       : " + (summary.getFailuresPath() == null ? "" : summary.getFailuresPath().toAbsolutePath()));
        System.out.println("Failed tables       : " + summary.getFailures().size());
        System.out.println("Duration ms         : " + summary.getDurationMs());
        if (!summary.getFailures().isEmpty()) {
            System.out.println("Failures:");
            for (String failure : summary.getFailures()) {
                System.out.println("  " + failure);
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  SqlScriptImportTool --clear-only");
        System.out.println("  SqlScriptImportTool --sync-metadata <script-path-or-directory>");
        System.out.println("  SqlScriptImportTool --clear-first <script-path>");
        System.out.println("  SqlScriptImportTool <script-path-or-directory>");
        System.out.println("Config:");
        System.out.println("  -D" + SqlScriptImportService.DATAMAP_BASE_URL_PROPERTY + "=https://dp.58corp.com");
        System.out.println("  -D" + SqlScriptImportService.DATAMAP_COOKIE_PROPERTY + "='cookie string'");
        System.out.println("  -D" + SqlScriptImportService.LOCAL_METADATA_CACHE_DIR_PROPERTY + "=/path/to/local/metadata-cache");
        System.out.println("  (导入前可先用 --sync-metadata 批量同步依赖表元数据到本地缓存。)");
    }
}
