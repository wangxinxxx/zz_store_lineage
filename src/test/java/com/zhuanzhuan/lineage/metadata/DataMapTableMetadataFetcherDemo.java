package com.zhuanzhuan.lineage.metadata;

import com.zhuanzhuan.lineage.model.TableRef;

import java.util.Arrays;

public final class DataMapTableMetadataFetcherDemo {
    private DataMapTableMetadataFetcherDemo() {
    }

    public static void main(String[] args) throws Exception {
        String ddl = ""
                + "CREATE TABLE `hdp_ubu_zhuanzhuan_dm_c2b.dm_offline_store_action_data_full_1d`(\n"
                + "  `month` string COMMENT '统计月', \n"
                + "  `store_id` bigint COMMENT '门店id', \n"
                + "  `sale_net_gmv` decimal(18,2) COMMENT '净gmv'\n"
                + ")\n"
                + "COMMENT 'demo'\n"
                + "PARTITIONED BY ( \n"
                + "  `dt` string)\n"
                + "LOCATION\n"
                + "  'viewfs://58-cluster/home/demo'";

        TableMetadataSnapshot snapshot = DataMapTableMetadataFetcher.parseCreateSql(
                TableRef.fromParts(Arrays.asList("hdp_ubu_zhuanzhuan_dm_c2b", "dm_offline_store_action_data_full_1d")),
                ddl
        );

        require(snapshot.getColumns().size() == 3, "Expected 3 columns from create_sql_hive.");
        require(snapshot.getPartitionKeys().size() == 1, "Expected 1 partition key from create_sql_hive.");
        require("sale_net_gmv".equals(snapshot.getColumns().get(2).getName()), "Expected sale_net_gmv column.");
        require("decimal(18,2)".equals(snapshot.getColumns().get(2).getType()), "Expected decimal type.");
        require("统计月".equals(snapshot.getColumns().get(0).getComment()), "Expected month comment.");
        require("净gmv".equals(snapshot.getColumns().get(2).getComment()), "Expected sale_net_gmv comment.");
        require("dt".equals(snapshot.getPartitionKeys().get(0).getName()), "Expected dt partition.");
        require("viewfs://58-cluster/home/demo".equals(snapshot.getLocation()), "Expected location parsed.");

        System.out.println("DataMap metadata parser demo succeeded.");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
