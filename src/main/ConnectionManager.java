package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager implements AutoCloseable {
    private Connection connection;
    private main.DatabaseConfig config;
    private boolean autoCommit;

    public ConnectionManager(main.DatabaseConfig config, boolean autoCommit) throws SQLException {
        this.config = config;
        this.autoCommit = autoCommit;
        this.connection = openConnection();
    }

    private Connection openConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("无法加载 PostgreSQL 驱动，请检查 CLASSPATH 中是否包含 postgresql-42.x.x.jar", e);
        }

        Connection conn = DriverManager.getConnection(
                config.getJdbcUrl(),
                config.getUser(),
                config.getPassword()
        );

        conn.setAutoCommit(autoCommit);

        return conn;
    }

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

    public void commit() throws SQLException {
        if (connection != null && !connection.isClosed() && !connection.getAutoCommit()) {
            connection.commit();
        }
    }

    public void rollback() throws SQLException {
        if (connection != null && !connection.isClosed() && !connection.getAutoCommit()) {
            connection.rollback();
        }
    }

    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
            connection = null;
        }
    }

    public boolean isConnectionValid() {
        try {
            return connection != null && !connection.isClosed() && connection.isValid(2);
        } catch (SQLException e) {
            return false;
        }
    }
}