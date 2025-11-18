package task3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据查询类
 * 提供条件筛选、统计数量、更新/删除等查询接口
 */
public class DataQuery {
    private Connection connection;

    public DataQuery(Connection connection) {
        this.connection = connection;
    }

    /**
     * 按照条件筛选记录
     * @param tableName 表名
     * @param conditions 条件 Map，key 为列名，value 为条件值
     * @param columns 要查询的列（null 表示查询所有列）
     * @return 查询结果列表
     */
    public List<Map<String, Object>> selectByConditions(String tableName,
                                                        Map<String, Object> conditions,
                                                        String[] columns) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("SELECT ");

        if (columns == null || columns.length == 0) {
            sqlBuilder.append("*");
        } else {
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(columns[i]);
            }
        }

        sqlBuilder.append(" FROM ").append(tableName);

        // 构建 WHERE 子句
        if (conditions != null && !conditions.isEmpty()) {
            sqlBuilder.append(" WHERE ");
            boolean first = true;
            for (String column : conditions.keySet()) {
                if (!first) {
                    sqlBuilder.append(" AND ");
                }
                sqlBuilder.append(column).append(" = ?");
                first = false;
            }
        }

        String sql = sqlBuilder.toString();
        List<Map<String, Object>> results = new ArrayList<>();

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            // 设置条件参数
            if (conditions != null && !conditions.isEmpty()) {
                int index = 1;
                for (Object value : conditions.values()) {
                    setParameter(pstmt, index++, value);
                }
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    if (columns == null || columns.length == 0) {
                        // 查询所有列
                        int columnCount = rs.getMetaData().getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            String columnName = rs.getMetaData().getColumnName(i);
                            row.put(columnName, rs.getObject(i));
                        }
                    } else {
                        // 查询指定列
                        for (String column : columns) {
                            row.put(column, rs.getObject(column));
                        }
                    }
                    results.add(row);
                }
            }
        }

        return results;
    }

    /**
     * 统计记录数量
     * @param tableName 表名
     * @param conditions 条件 Map（可选，null 表示统计所有记录）
     * @return 记录数量
     */
    public long count(String tableName, Map<String, Object> conditions) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("SELECT COUNT(*) FROM ").append(tableName);

        if (conditions != null && !conditions.isEmpty()) {
            sqlBuilder.append(" WHERE ");
            boolean first = true;
            for (String column : conditions.keySet()) {
                if (!first) {
                    sqlBuilder.append(" AND ");
                }
                sqlBuilder.append(column).append(" = ?");
                first = false;
            }
        }

        String sql = sqlBuilder.toString();

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            if (conditions != null && !conditions.isEmpty()) {
                int index = 1;
                for (Object value : conditions.values()) {
                    setParameter(pstmt, index++, value);
                }
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }

        return 0;
    }

    /**
     * 更新记录
     * @param tableName 表名
     * @param updates 要更新的字段 Map，key 为列名，value 为新值
     * @param conditions 条件 Map，key 为列名，value 为条件值
     * @return 更新的记录数
     */
    public int update(String tableName, Map<String, Object> updates,
                      Map<String, Object> conditions) throws SQLException {
        if (updates == null || updates.isEmpty()) {
            return 0;
        }

        StringBuilder sqlBuilder = new StringBuilder("UPDATE ").append(tableName).append(" SET ");

        boolean first = true;
        for (String column : updates.keySet()) {
            if (!first) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append(column).append(" = ?");
            first = false;
        }

        if (conditions != null && !conditions.isEmpty()) {
            sqlBuilder.append(" WHERE ");
            first = true;
            for (String column : conditions.keySet()) {
                if (!first) {
                    sqlBuilder.append(" AND ");
                }
                sqlBuilder.append(column).append(" = ?");
                first = false;
            }
        }

        String sql = sqlBuilder.toString();

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            int index = 1;

            // 设置更新参数
            for (Object value : updates.values()) {
                setParameter(pstmt, index++, value);
            }

            // 设置条件参数
            if (conditions != null && !conditions.isEmpty()) {
                for (Object value : conditions.values()) {
                    setParameter(pstmt, index++, value);
                }
            }

            return pstmt.executeUpdate();
        }
    }

    /**
     * 删除记录
     * @param tableName 表名
     * @param conditions 条件 Map，key 为列名，value 为条件值
     * @return 删除的记录数
     */
    public int delete(String tableName, Map<String, Object> conditions) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ").append(tableName);

        if (conditions != null && !conditions.isEmpty()) {
            sqlBuilder.append(" WHERE ");
            boolean first = true;
            for (String column : conditions.keySet()) {
                if (!first) {
                    sqlBuilder.append(" AND ");
                }
                sqlBuilder.append(column).append(" = ?");
                first = false;
            }
        } else {
            // 如果没有条件，使用更安全的限制
            throw new SQLException("删除操作必须提供条件，以防止误删所有数据");
        }

        String sql = sqlBuilder.toString();

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            int index = 1;
            for (Object value : conditions.values()) {
                setParameter(pstmt, index++, value);
            }

            return pstmt.executeUpdate();
        }
    }

    /**
     * 执行自定义 SQL 查询
     * @param sql SQL 查询语句
     * @param params 参数列表
     * @return 查询结果列表
     */
    public List<Map<String, Object>> executeQuery(String sql, Object... params) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                setParameter(pstmt, i + 1, params[i]);
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                int columnCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = rs.getMetaData().getColumnName(i);
                        row.put(columnName, rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        }

        return results;
    }

    /**
     * 设置 PreparedStatement 参数
     */
    private void setParameter(PreparedStatement pstmt, int index, Object value) throws SQLException {
        if (value == null) {
            pstmt.setNull(index, java.sql.Types.NULL);
        } else if (value instanceof Integer) {
            pstmt.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            pstmt.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            pstmt.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            pstmt.setFloat(index, (Float) value);
        } else if (value instanceof String) {
            pstmt.setString(index, (String) value);
        } else if (value instanceof Boolean) {
            pstmt.setBoolean(index, (Boolean) value);
        } else if (value instanceof java.sql.Timestamp) {
            pstmt.setTimestamp(index, (java.sql.Timestamp) value);
        } else if (value instanceof java.sql.Date) {
            pstmt.setDate(index, (java.sql.Date) value);
        } else {
            pstmt.setObject(index, value);
        }
    }
}
