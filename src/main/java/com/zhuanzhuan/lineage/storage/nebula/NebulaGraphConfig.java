package com.zhuanzhuan.lineage.storage.nebula;

public final class NebulaGraphConfig {
    public static final String HOST_PROPERTY = "zz.lineage.nebula.host";
    public static final String PORT_PROPERTY = "zz.lineage.nebula.port";
    public static final String USERNAME_PROPERTY = "zz.lineage.nebula.username";
    public static final String PASSWORD_PROPERTY = "zz.lineage.nebula.password";
    public static final String SPACE_PROPERTY = "zz.lineage.nebula.space";
    public static final String MAX_CONN_PROPERTY = "zz.lineage.nebula.maxConn";
    public static final String MIN_CONN_PROPERTY = "zz.lineage.nebula.minConn";
    public static final String TIMEOUT_PROPERTY = "zz.lineage.nebula.timeoutMs";

    public static final String HOST_ENV = "ZZ_LINEAGE_NEBULA_HOST";
    public static final String PORT_ENV = "ZZ_LINEAGE_NEBULA_PORT";
    public static final String USERNAME_ENV = "ZZ_LINEAGE_NEBULA_USERNAME";
    public static final String PASSWORD_ENV = "ZZ_LINEAGE_NEBULA_PASSWORD";
    public static final String SPACE_ENV = "ZZ_LINEAGE_NEBULA_SPACE";
    public static final String MAX_CONN_ENV = "ZZ_LINEAGE_NEBULA_MAX_CONN";
    public static final String MIN_CONN_ENV = "ZZ_LINEAGE_NEBULA_MIN_CONN";
    public static final String TIMEOUT_ENV = "ZZ_LINEAGE_NEBULA_TIMEOUT_MS";

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String space;
    private final int minConnSize;
    private final int maxConnSize;
    private final int timeoutMs;

    public NebulaGraphConfig(
            String host,
            int port,
            String username,
            String password,
            String space,
            int minConnSize,
            int maxConnSize,
            int timeoutMs
    ) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.space = space;
        this.minConnSize = minConnSize;
        this.maxConnSize = maxConnSize;
        this.timeoutMs = timeoutMs;
    }

    public static NebulaGraphConfig fromSystem() {
        return new NebulaGraphConfig(
                read(HOST_PROPERTY, HOST_ENV, "127.0.0.1"),
                parseInt(read(PORT_PROPERTY, PORT_ENV, "9669"), 9669),
                read(USERNAME_PROPERTY, USERNAME_ENV, "root"),
                read(PASSWORD_PROPERTY, PASSWORD_ENV, "nebula"),
                read(SPACE_PROPERTY, SPACE_ENV, "store_lineage"),
                parseInt(read(MIN_CONN_PROPERTY, MIN_CONN_ENV, "1"), 1),
                parseInt(read(MAX_CONN_PROPERTY, MAX_CONN_ENV, "10"), 10),
                parseInt(read(TIMEOUT_PROPERTY, TIMEOUT_ENV, "0"), 0)
        );
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getSpace() {
        return space;
    }

    public int getMinConnSize() {
        return minConnSize;
    }

    public int getMaxConnSize() {
        return maxConnSize;
    }

    public int getTimeoutMs() {
        return timeoutMs;
    }

    private static String read(String propertyKey, String envKey, String defaultValue) {
        String property = System.getProperty(propertyKey);
        if (property != null && !property.trim().isEmpty()) {
            return property.trim();
        }
        String env = System.getenv(envKey);
        if (env != null && !env.trim().isEmpty()) {
            return env.trim();
        }
        return defaultValue;
    }

    private static int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException error) {
            return defaultValue;
        }
    }
}
