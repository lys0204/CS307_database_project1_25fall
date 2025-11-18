package task3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * 数据写入类
 * 实现批量插入、清空表等功能
 */
public class DataWriter {
    private Connection connection;
    private int batchSize;

    /**
     * 创建数据写入器
     * @param connection 数据库连接
     * @param batchSize 批次大小（每批插入的记录数）
     */
    public DataWriter(Connection connection, int batchSize) {
        this.connection = connection;
        this.batchSize = batchSize;
    }

    /**
     * 清空目标表
     * @param tableName 表名
     * @param cascade 是否级联删除（CASCADE）
     */
    public void truncateTable(String tableName, boolean cascade) throws SQLException {
        String sql = cascade
                ? "TRUNCATE TABLE " + tableName + " CASCADE"
                : "TRUNCATE TABLE " + tableName;

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("已清空表: " + tableName);
        }
    }

    /**
     * 批量插入数据（通用方法）
     * @param tableName 表名
     * @param columns 列名数组
     * @param data 数据列表，每个 Map 的 key 为列名，value 为字段值
     */
    public int batchInsert(String tableName, String[] columns, List<Map<String, Object>> data) throws SQLException {
        if (data == null || data.isEmpty()) {
            return 0;
        }

        // 构建 SQL 语句
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append(tableName).append(" (");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append(columns[i]);
        }
        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append("?");
        }
        sqlBuilder.append(")");

        String sql = sqlBuilder.toString();
        int totalInserted = 0;
        int batchCounter = 0;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (Map<String, Object> row : data) {
                // 设置参数
                for (int i = 0; i < columns.length; i++) {
                    Object value = row.get(columns[i]);
                    setParameter(pstmt, i + 1, value);
                }

                pstmt.addBatch();
                batchCounter++;

                // 达到批次大小时执行
                if (batchCounter % batchSize == 0) {
                    int[] results = pstmt.executeBatch();
                    totalInserted += countSuccess(results);
                    pstmt.clearBatch();
                }
            }

            // 执行剩余的批次
            if (batchCounter % batchSize != 0) {
                int[] results = pstmt.executeBatch();
                totalInserted += countSuccess(results);
            }
        }

        System.out.println("批量插入完成: " + totalInserted + " 条记录到表 " + tableName);
        return totalInserted;
    }

    /**
     * 批量插入数据（使用自定义 SQL）
     * @param sql SQL 插入语句（带 ? 占位符）
     * @param dataSetter 数据设置器，用于设置 PreparedStatement 的参数
     */
    public int batchInsert(String sql, BatchDataSetter dataSetter) throws SQLException {
        int totalInserted = 0;
        int batchCounter = 0;

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            while (dataSetter.hasNext()) {
                dataSetter.setParameters(pstmt);
                pstmt.addBatch();
                batchCounter++;

                // 达到批次大小时执行
                if (batchCounter % batchSize == 0) {
                    int[] results = pstmt.executeBatch();
                    totalInserted += countSuccess(results);
                    pstmt.clearBatch();
                }
            }

            // 执行剩余的批次
            if (batchCounter % batchSize != 0) {
                int[] results = pstmt.executeBatch();
                totalInserted += countSuccess(results);
            }
        }

        return totalInserted;
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

    /**
     * 统计成功插入的记录数
     */
    private int countSuccess(int[] results) {
        int count = 0;
        for (int result : results) {
            if (result >= 0) {
                count++;
            }
        }
        return count;
    }

    /**
     * 数据设置器接口（用于自定义批量插入逻辑）
     */
    public interface BatchDataSetter {
        boolean hasNext();
        void setParameters(PreparedStatement pstmt) throws SQLException;
    }
}
