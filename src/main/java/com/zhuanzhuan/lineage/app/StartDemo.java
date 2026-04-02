package com.zhuanzhuan.lineage.app;

import com.zhuanzhuan.lineage.storage.nebula.NebulaImporterBundleWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StartDemo {
    public static void main(String[] args) throws IOException {
        Path sqlPath = Paths.get("src/main/resources/sql/dw_trade_sale_store_pro_retail_offline_data_full_1d.sql");

        try (SqlScriptImportService service = SqlScriptImportService.createParserOnly()) {
            NebulaImporterBundleWriter.BundleSummary summary = service.exportImporterBundle(sqlPath);
            System.out.println("SQL source     : " + sqlPath.getFileName());
            System.out.println("Bundle dir     : " + summary.getBundleDir().toAbsolutePath());
            System.out.println("Vertex files   : " + summary.getVertexFiles());
            System.out.println("Edge files     : " + summary.getEdgeFiles());
            System.out.println("Total vertices : " + summary.getTotalVertices());
            System.out.println("Total edges    : " + summary.getTotalEdges());
        }
    }
}
