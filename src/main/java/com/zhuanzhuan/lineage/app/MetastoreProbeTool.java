package com.zhuanzhuan.lineage.app;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.concurrent.TimeUnit;

public final class MetastoreProbeTool {
    private MetastoreProbeTool() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
            return;
        }

        String metastoreUri = args[0];
        String tableName = args[1];
        String[] parts = splitTableName(tableName);
        String database = parts[0];
        String table = parts[1];

        HiveConf hiveConf = new HiveConf();
        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_CAPABILITY_CHECK, false);
        hiveConf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, 120, TimeUnit.SECONDS);
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
        hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES, 3);
        hiveConf.set("hive.metastore.client.connect.retry.delay", "1s");

        HiveMetaStoreClient client = null;
        try {
            client = new HiveMetaStoreClient(hiveConf);
            Table tableObject = client.getTable(database, table);
            System.out.println("Metastore probe succeeded.");
            System.out.println("URI           : " + metastoreUri);
            System.out.println("Database      : " + database);
            System.out.println("Table         : " + table);
            System.out.println("Table type    : " + tableObject.getTableType());
            System.out.println("Owner         : " + tableObject.getOwner());
            System.out.println("Location      : " + (tableObject.getSd() == null ? "" : tableObject.getSd().getLocation()));
            System.out.println("Columns       : " + joinColumnNames(tableObject.getSd() == null ? null : tableObject.getSd().getCols()));
            System.out.println("Partitions    : " + joinColumnNames(tableObject.getPartitionKeys()));
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    private static String[] splitTableName(String tableName) {
        String normalized = tableName == null ? "" : tableName.trim();
        int index = normalized.lastIndexOf('.');
        if (index <= 0 || index >= normalized.length() - 1) {
            throw new IllegalArgumentException("Table name must be in <database>.<table> format: " + tableName);
        }
        return new String[]{normalized.substring(0, index), normalized.substring(index + 1)};
    }

    private static String joinColumnNames(List<FieldSchema> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            FieldSchema column = columns.get(i);
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(column.getName()).append(":").append(column.getType());
        }
        return builder.toString();
    }

    private static void printUsage() {
        System.out.println("Usage: MetastoreProbeTool <metastore-uri> <database.table>");
    }
}
