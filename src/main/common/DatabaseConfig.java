package main.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class DatabaseConfig {
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 12138;  // 您的端口
    private static final String DEFAULT_DATABASE = "sustc_recipe_db";
    private static final String DEFAULT_USER = "postgres";
    private static final String DEFAULT_PASSWORD = "ss060204";
    
    private String host;
    private int port;
    private String database;
    private String user;
    private String password;
    
    public static DatabaseConfig fromFile(String configFile) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            props.load(fis);
        }

        DatabaseConfig config = new DatabaseConfig();
        config.host = props.getProperty("db.host", DEFAULT_HOST);
        config.port = Integer.parseInt(props.getProperty("db.port", String.valueOf(DEFAULT_PORT)));
        config.database = props.getProperty("db.database", DEFAULT_DATABASE);
        config.user = props.getProperty("db.user", DEFAULT_USER);
        config.password = props.getProperty("db.password", DEFAULT_PASSWORD);

        return config;
    }

    public static DatabaseConfig defaultConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.host = DEFAULT_HOST;
        config.port = DEFAULT_PORT;
        config.database = DEFAULT_DATABASE;
        config.user = DEFAULT_USER;
        config.password = DEFAULT_PASSWORD;
        return config;
    }

    public static DatabaseConfig create(String host, int port, String database, String user, String password) {
        DatabaseConfig config = new DatabaseConfig();
        config.host = host;
        config.port = port;
        config.database = database;
        config.user = user;
        config.password = password;
        return config;
    }

    public String getJdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getDatabase() { return database; }
    public String getUser() { return user; }
    public String getPassword() { return password; }

    @Override
    public String toString() {
        return String.format("DatabaseConfig{host='%s', port=%d, database='%s', user='%s'}",
                host, port, database, user);
    }
}