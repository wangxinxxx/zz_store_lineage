package com.zhuanzhuan.lineage.demo;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;
import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;
import com.zhuanzhuan.lineage.storage.nebula.NebulaLineageStorage;

import java.lang.reflect.Field;

public final class NebulaQueryTool {
    private NebulaQueryTool() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: NebulaQueryTool <nGQL>");
            return;
        }

        NebulaGraphConfig config = NebulaGraphConfig.fromSystem();
        NebulaLineageStorage storage = new NebulaLineageStorage(config);
        try {
            Session session = openSession(storage, config);
            try {
                ResultSet useResult = session.execute("USE `" + config.getSpace() + "`;");
                if (!useResult.isSucceeded()) {
                    throw new IllegalStateException(useResult.getErrorMessage());
                }

                String query = args[0];
                ResultSet resultSet = session.execute(query);
                System.out.println("succeeded=" + resultSet.isSucceeded());
                System.out.println("error=" + resultSet.getErrorMessage());
                System.out.println(resultSet.toString());
            } finally {
                session.release();
            }
        } finally {
            storage.close();
        }
    }

    private static Session openSession(NebulaLineageStorage storage, NebulaGraphConfig config) throws Exception {
        Field poolField = NebulaLineageStorage.class.getDeclaredField("pool");
        poolField.setAccessible(true);
        com.vesoft.nebula.client.graph.net.NebulaPool pool =
                (com.vesoft.nebula.client.graph.net.NebulaPool) poolField.get(storage);
        return pool.getSession(config.getUsername(), config.getPassword(), true);
    }
}
