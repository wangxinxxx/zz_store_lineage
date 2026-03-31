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
            if (exportImporterBundle) {
                if (args.length < 2) {
                    printUsage();
                    return;
                }
                targetPath = Paths.get(args[1]);
            } else {
                targetPath = Paths.get(args[0]);
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

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("  SqlScriptImportTool --clear-only");
        System.out.println("  SqlScriptImportTool --export-importer-bundle <script-path-or-directory>");
        System.out.println("  SqlScriptImportTool --spark-import-bundle <bundle-directory>");
        System.out.println("  SqlScriptImportTool <script-path-or-directory>");
        System.out.println("Workflow:");
        System.out.println("  1. Parse SQL and export a Nebula importer bundle.");
        System.out.println("     Default invocation and --export-importer-bundle do the same thing.");
        System.out.println("  2. Wait for bundle generation to finish, then run --spark-import-bundle.");
        System.out.println("Config:");
        System.out.println("  -Dzz.lineage.spark.conf.hive.metastore.uris=thrift://hive-metastore-host:9083");
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
        System.out.println("  SqlScriptImportService uses Spark enableHiveSupport for metadata lookup.");
    }
}
