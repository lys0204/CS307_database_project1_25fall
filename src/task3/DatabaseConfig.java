package task3;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * 数据库配置类
 * 支持从配置文件或常量读取数据库连接信息
 */
public class DatabaseConfig {
    // 默认配置常量
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 12138;
    private static final String DEFAULT_DATABASE = "CS307 数据库原理";
    private static final String DEFAULT_USER = "postgres";
    private static final String DEFAULT_PASSWORD = "ss060204";

    // 配置属性
    private String host;
    private int port;
    private String database;
    private String user;
    private String password;

    /**
     * 从配置文件加载配置
     * @param configFile 配置文件路径（如 db.properties）
     * @return DatabaseConfig 实例
     */
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

    /**
     * 使用默认配置
     */
    public static DatabaseConfig defaultConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.host = DEFAULT_HOST;
        config.port = DEFAULT_PORT;
        config.database = DEFAULT_DATABASE;
        config.user = DEFAULT_USER;
        config.password = DEFAULT_PASSWORD;
        return config;
    }

    /**
     * 使用自定义参数创建配置
     */
    public static DatabaseConfig create(String host, int port, String database, String user, String password) {
        DatabaseConfig config = new DatabaseConfig();
        config.host = host;
        config.port = port;
        config.database = database;
        config.user = user;
        config.password = password;
        return config;
    }

    /**
     * 生成 JDBC 连接 URL
     */
    public String getJdbcUrl() {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }

    // Getters
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
