package com.zhuanzhuan.lineage.app;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;

import java.util.List;
import java.util.concurrent.TimeUnit;

public final class MetastoreFunctionsProbeTool {
    private MetastoreFunctionsProbeTool() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: MetastoreFunctionsProbeTool <metastore-uri>");
            return;
        }

        String metastoreUri = args[0];
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
            GetAllFunctionsResponse response = client.getAllFunctions();
            List<Function> functions = response == null ? null : response.getFunctions();
            int count = functions == null ? 0 : functions.size();
            System.out.println("Metastore getAllFunctions probe succeeded.");
            System.out.println("URI            : " + metastoreUri);
            System.out.println("Function count : " + count);
            if (functions != null) {
                for (int i = 0; i < Math.min(10, functions.size()); i++) {
                    Function function = functions.get(i);
                    if (function == null) {
                        continue;
                    }
                    System.out.println(
                            "Function[" + i + "]   : "
                                    + function.getDbName()
                                    + "."
                                    + function.getFunctionName()
                                    + " -> "
                                    + function.getClassName()
                    );
                }
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
