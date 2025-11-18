package task3;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import task4.PerformanceTest;

/**
 * PostgreSQL 连接程序主类
 * 演示所有功能的使用
 */
public class PostgresMain {

    public static void main(String[] args) {
        try {
            // ========== 1. 数据库配置 ==========
            System.out.println("========== 1. 数据库配置 ==========");
            DatabaseConfig config;

            // 方式1：从配置文件加载（如果存在）
            try {
                config = DatabaseConfig.fromFile("db.properties");
                System.out.println("从配置文件加载: " + config);
            } catch (Exception e) {
                // 方式2：使用默认配置
                System.out.println("使用默认配置");
                config = DatabaseConfig.defaultConfig();
                System.out.println(config);
            }

            // 方式3：也可以手动指定配置
            // config = DatabaseConfig.create("localhost", 5432, "lab_project", "postgres", "password");

            System.out.println();

            // ========== 2. 建立连接 ==========
            System.out.println("========== 2. 建立数据库连接 ==========");
            ConnectionManager connMgr = new ConnectionManager(config, false); // false 表示手动管理事务
            System.out.println("连接成功！");
            System.out.println("连接状态: " + (connMgr.isConnectionValid() ? "有效" : "无效"));
            System.out.println();

            try {
                // ========== 3. CSV 数据导入（Task 3 核心功能）==========
                System.out.println("========== 3. CSV 数据导入 ==========");
                System.out.println("Task 3 要求：完全自动化的数据导入（CSV -> 数据库表）");
                
                CsvDataImporter importer = new CsvDataImporter(connMgr);
                try {
                    // 导入所有 CSV 文件到数据库
                    importer.importAllCsvFiles();
                } catch (Exception e) {
                    System.out.println("导入失败: " + e.getMessage());
                    // 可以选择继续执行其他功能，或者退出
                }
                System.out.println();

                // ========== 4. 数据写入示例 ==========
                System.out.println("========== 4. 数据写入示例 ==========");
                DataWriter writer = new DataWriter(connMgr.getConnection(), 1000);

                // 创建测试表
                createTestTable(connMgr.getConnection());

                // 清空表
                writer.truncateTable("test_performance", false);
                System.out.println();

                // 批量插入示例数据
                List<Map<String, Object>> testData = createTestData();
                String[] columns = {"id", "name", "value", "category"};
                int inserted = writer.batchInsert("test_performance", columns, testData);
                System.out.println("成功插入 " + inserted + " 条记录");

                // 提交事务
                connMgr.commit();
                System.out.println();

                // ========== 5. 数据查询示例 ==========
                System.out.println("========== 5. 数据查询示例 ==========");
                DataQuery query = new DataQuery(connMgr.getConnection());

                // 统计记录数
                long totalCount = query.count("test_performance", null);
                System.out.println("总记录数: " + totalCount);

                // 条件查询
                Map<String, Object> conditions = new HashMap<>();
                conditions.put("category", "A");
                List<Map<String, Object>> results = query.selectByConditions("test_performance", conditions, null);
                System.out.println("category='A' 的记录数: " + results.size());

                // 更新记录
                Map<String, Object> updates = new HashMap<>();
                updates.put("value", 9999);
                Map<String, Object> updateConditions = new HashMap<>();
                updateConditions.put("id", 1);
                int updated = query.update("test_performance", updates, updateConditions);
                System.out.println("更新了 " + updated + " 条记录");
                connMgr.commit();

                // 再次查询验证更新
                Map<String, Object> verifyConditions = new HashMap<>();
                verifyConditions.put("id", 1);
                List<Map<String, Object>> verifyResults = query.selectByConditions("test_performance", verifyConditions, new String[]{"id", "value"});
                if (!verifyResults.isEmpty()) {
                    System.out.println("更新后的记录: " + verifyResults.get(0));
                }
                System.out.println();

                // ========== 6. 性能测试 ==========
                System.out.println("========== 6. 性能测试 ==========");
                PerformanceTest perfTest = new PerformanceTest(connMgr, "test_performance", 10000);
                perfTest.createTestTable();
                perfTest.runFullPerformanceTest();
                System.out.println();

                // ========== 7. 异常处理和回滚示例 ==========
                System.out.println("========== 7. 异常处理和回滚示例 ==========");
                try {
                    // 尝试插入无效数据（触发异常）
                    Map<String, Object> invalidData = new HashMap<>();
                    invalidData.put("id", null); // 主键不能为 null
                    invalidData.put("name", "Invalid");
                    invalidData.put("value", 0);
                    invalidData.put("category", "X");

                    List<Map<String, Object>> invalidList = new ArrayList<>();
                    invalidList.add(invalidData);

                    writer.batchInsert("test_performance", columns, invalidList);
                    connMgr.commit();
                } catch (Exception e) {
                    System.out.println("捕获异常: " + e.getMessage());
                    connMgr.rollback();
                    System.out.println("已回滚事务");
                }
                System.out.println();

            } finally {
                // ========== 8. 关闭连接 ==========
                System.out.println("========== 8. 关闭连接 ==========");
                connMgr.close();
                System.out.println("连接已关闭");
            }

        } catch (Exception e) {
            System.err.println("程序执行出错: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 创建测试表
     */
    private static void createTestTable(java.sql.Connection conn) throws SQLException {
        String createTableSql =
                "CREATE TABLE IF NOT EXISTS test_performance (" +
                        "id INTEGER PRIMARY KEY, " +
                        "name VARCHAR(100), " +
                        "value INTEGER, " +
                        "category VARCHAR(50)" +
                        ")";

        try (java.sql.Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableSql);
            System.out.println("测试表创建成功");
        }
    }

    /**
     * 创建测试数据
     */
    private static List<Map<String, Object>> createTestData() {
        List<Map<String, Object>> data = new ArrayList<>();
        String[] categories = {"A", "B", "C", "D", "E"};

        for (int i = 1; i <= 100; i++) {
            Map<String, Object> record = new HashMap<>();
            record.put("id", i);
            record.put("name", "Record_" + i);
            record.put("value", i * 10);
            record.put("category", categories[i % categories.length]);
            data.add(record);
        }

        return data;
    }
}
