package task3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 数据库连接管理类
 * 负责打开/关闭连接，管理事务
 */
public class ConnectionManager implements AutoCloseable {
    private Connection connection;
    private DatabaseConfig config;
    private boolean autoCommit;

    /**
     * 创建连接管理器
     * @param config 数据库配置
     * @param autoCommit 是否自动提交（false 表示手动管理事务）
     */
    public ConnectionManager(DatabaseConfig config, boolean autoCommit) throws SQLException {
        this.config = config;
        this.autoCommit = autoCommit;
        this.connection = openConnection();
    }

    /**
     * 打开数据库连接
     */
    private Connection openConnection() throws SQLException {
        try {
            // 加载 PostgreSQL 驱动
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("无法加载 PostgreSQL 驱动，请检查 CLASSPATH 中是否包含 postgresql-42.2.5.jar", e);
        }

        // 建立连接
        Connection conn = DriverManager.getConnection(
                config.getJdbcUrl(),
                config.getUser(),
                config.getPassword()
        );

        // 设置自动提交模式
        conn.setAutoCommit(autoCommit);

        return conn;
    }

    /**
     * 获取当前连接
     */
    public Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                try {
                    connection = openConnection();
                } catch (SQLException e) {
                    throw new RuntimeException("无法重新建立连接", e);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("检查连接状态失败", e);
        }
        return connection;
    }

    /**
     * 提交事务
     */
    public void commit() throws SQLException {
        if (connection != null && !connection.isClosed() && !connection.getAutoCommit()) {
            connection.commit();
        }
    }

    /**
     * 回滚事务
     */
    public void rollback() throws SQLException {
        if (connection != null && !connection.isClosed() && !connection.getAutoCommit()) {
            connection.rollback();
        }
    }

    /**
     * 关闭连接
     */
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            connection = null;
        }
    }

    /**
     * 检查连接是否有效
     */
    public boolean isConnectionValid() {
        try {
            return connection != null && !connection.isClosed() && connection.isValid(2);
        } catch (SQLException e) {
            return false;
        }
    }
}
