package com.zhuanzhuan.lineage.app;

import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;
import com.zhuanzhuan.lineage.storage.nebula.NebulaImporterBundleWriter;
import com.zhuanzhuan.lineage.storage.nebula.SparkNebulaConnectorImporter;

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
        boolean syncMetadataOnly = "--sync-metadata".equals(args[0]);
        boolean exportImporterBundle = "--export-importer-bundle".equals(args[0]);
        boolean sparkImportBundle = "--spark-import-bundle".equals(args[0]);

        if (sparkImportBundle) {
            if (args.length < 2) {
                printUsage();
                return;
            }
            printSparkImportSummary(new SparkNebulaConnectorImporter(
                    NebulaGraphConfig.fromSystem(),
                    SparkNebulaConnectorImporter.ImportOptions.fromSystem()
            ).importBundle(Paths.get(args[1])));
            return;
        }

        try (SqlScriptImportService service = SqlScriptImportService.createDefault()) {
            if (clearOnly) {
                service.clearStorage();
                System.out.println("Nebula storage cleared.");
                return;
            }

            Path targetPath;
            if (syncMetadataOnly || exportImporterBundle) {
                if (args.length < 2) {
                    printUsage();
                    return;
                }
                targetPath = Paths.get(args[1]);
            } else {
                targetPath = Paths.get(args[0]);
            }

            if (syncMetadataOnly) {
                printMetadataSyncSummary(service.syncMetadata(targetPath));
                return;
            }

            printBundleSummary(service.exportImporterBundle(targetPath));
        }
    }

    private static void printBundleSummary(NebulaImporterBundleWriter.BundleSummary summary) {
        System.out.println("Importer bundle export finished.");
        System.out.println("Bundle directory   : " + summary.getBundleDir().toAbsolutePath());
        System.out.println("Importer config    : " + summary.getImporterConfigPath().toAbsolutePath());
        System.out.println("Schema file        : " + summary.getSchemaPath().toAbsolutePath());
        System.out.println("Run script (ps1)   : " + summary.getRunPs1Path().toAbsolutePath());
        System.out.println("Run script (sh)    : " + summary.getRunShPath().toAbsolutePath());
        System.out.println("Total scripts      : " + summary.getTotalScripts());
        System.out.println("Total events       : " + summary.getTotalEvents());
        System.out.println("Total results      : " + summary.getTotalResults());
        System.out.println("Vertex files       : " + summary.getVertexFiles());
        System.out.println("Edge files         : " + summary.getEdgeFiles());
        System.out.println("Total vertices     : " + summary.getTotalVertices());
        System.out.println("Total edges        : " + summary.getTotalEdges());
        System.out.println("Duration ms        : " + summary.getDurationMs());
    }

    private static void printSparkImportSummary(SparkNebulaConnectorImporter.ImportSummary summary) {
        System.out.println("Spark connector import finished.");
        System.out.println("Bundle directory   : " + summary.getBundleDir().toAbsolutePath());
        System.out.println("Vertex files       : " + summary.getImportedVertexFiles());
        System.out.println("Edge files         : " + summary.getImportedEdgeFiles());
        System.out.println("Total vertices     : " + summary.getTotalVertices());
        System.out.println("Total edges        : " + summary.getTotalEdges());
        System.out.println("Duration ms        : " + summary.getDurationMs());
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
        System.out.println("  SqlScriptImportTool --export-importer-bundle <script-path-or-directory>");
        System.out.println("  SqlScriptImportTool --spark-import-bundle <bundle-directory>");
        System.out.println("  SqlScriptImportTool <script-path-or-directory>");
        System.out.println("Workflow:");
        System.out.println("  1. Parse SQL and export a Nebula importer bundle.");
        System.out.println("     Default invocation and --export-importer-bundle do the same thing.");
        System.out.println("  2. Wait for bundle generation to finish, then run --spark-import-bundle.");
        System.out.println("Config:");
        System.out.println("  -D" + SqlScriptImportService.DATAMAP_BASE_URL_PROPERTY + "=https://dp.58corp.com");
        System.out.println("  -D" + SqlScriptImportService.DATAMAP_COOKIE_PROPERTY + "='cookie string'");
        System.out.println("  -D" + SqlScriptImportService.LOCAL_METADATA_CACHE_DIR_PROPERTY + "=/path/to/local/metadata-cache");
        System.out.println("  -D" + NebulaImporterBundleWriter.BUNDLE_DIR_PROPERTY + "=/path/to/importer-bundles");
        System.out.println("  -D" + NebulaImporterBundleWriter.IMPORTER_BATCH_PROPERTY + "=1024");
        System.out.println("  -D" + NebulaImporterBundleWriter.IMPORTER_READER_CONCURRENCY_PROPERTY + "=8");
        System.out.println("  -D" + NebulaImporterBundleWriter.IMPORTER_CONCURRENCY_PROPERTY + "=64");
        System.out.println("  -D" + NebulaGraphConfig.META_ADDRESS_PROPERTY + "=127.0.0.1:9559");
        System.out.println("  -D" + NebulaGraphConfig.GRAPH_ADDRESS_PROPERTY + "=127.0.0.1:9669");
        System.out.println("  -D" + SparkNebulaConnectorImporter.SPARK_MASTER_PROPERTY + "=local[*]");
        System.out.println("  -D" + SparkNebulaConnectorImporter.REPARTITION_PROPERTY + "=8");
        System.out.println("  -D" + SparkNebulaConnectorImporter.BATCH_PROPERTY + "=1024");
        System.out.println("  -D" + SparkNebulaConnectorImporter.PARALLEL_SCHEMA_PROPERTY + "=4");
        System.out.println("  -D" + SparkNebulaConnectorImporter.VERTICES_ONLY_PROPERTY + "=true");
        System.out.println("  Run --sync-metadata before exporting when you want to warm the local metadata cache.");
    }
}
