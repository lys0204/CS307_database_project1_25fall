package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;


public class DataWriter {
    private Connection connection;
    private int batchSize;


    public DataWriter(Connection connection, int batchSize) {
        this.connection = connection;
        this.batchSize = batchSize;
    }


    public void truncateTable(String tableName, boolean cascade) throws SQLException {
        String sql = cascade
                ? "TRUNCATE TABLE " + tableName + " CASCADE"
                : "TRUNCATE TABLE " + tableName;

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            System.out.println("已清空表: " + tableName);
        }
    }


    public int batchInsert(String tableName, String[] columns, List<Map<String, Object>> data) throws SQLException {
        if (data == null || data.isEmpty()) {
            return 0;
        }

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

            if (batchCounter % batchSize != 0) {
                int[] results = pstmt.executeBatch();
                totalInserted += countSuccess(results);
            }
        }

        System.out.println("批量插入完成: " + totalInserted + " 条记录到表 " + tableName);
        return totalInserted;
    }


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

            if (batchCounter % batchSize != 0) {
                int[] results = pstmt.executeBatch();
                totalInserted += countSuccess(results);
            }
        }

        return totalInserted;
    }

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

    private int countSuccess(int[] results) {
        int count = 0;
        for (int result : results) {
            if (result >= 0) {
                count++;
            }
        }
        return count;
    }

    public interface BatchDataSetter {
        boolean hasNext();
        void setParameters(PreparedStatement pstmt) throws SQLException;
    }
}