package com.zhuanzhuan.lineage.demo;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;
import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;
import com.zhuanzhuan.lineage.storage.nebula.NebulaLineageStorage;

import java.lang.reflect.Field;

public final class NebulaAdminTool {
    private NebulaAdminTool() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: NebulaAdminTool [--use-space] <nGQL>");
            return;
        }

        boolean useSpace = args.length > 1 && "--use-space".equals(args[0]);
        String query = useSpace ? args[1] : args[0];

        NebulaGraphConfig config = NebulaGraphConfig.fromSystem();
        NebulaLineageStorage storage = new NebulaLineageStorage(config);
        try {
            Session session = openSession(storage, config);
            try {
                if (useSpace) {
                    ResultSet useResult = session.execute("USE `" + config.getSpace() + "`;");
                    if (!useResult.isSucceeded()) {
                        throw new IllegalStateException(useResult.getErrorMessage());
                    }
                }

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
