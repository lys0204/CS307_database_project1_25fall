package task3;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CSV 数据导入器
 * 实现完整的 CSV 文件到数据库表的导入功能
 * 满足 Task 3 的基本要求：完全自动化（CSV输入 -> 数据库表输出）
 */
public class CsvDataImporter {
    private ConnectionManager connectionManager;
    private DataWriter dataWriter;
    private DataQuery dataQuery;
    
    public CsvDataImporter(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        Connection conn = connectionManager.getConnection();
        this.dataWriter = new DataWriter(conn, 1000); // 批次大小 1000
        this.dataQuery = new DataQuery(conn);
    }
    
    /**
     * 导入所有 CSV 文件到数据库
     * 这是 Task 3 的核心功能：完全自动化的数据导入
     */
    public void importAllCsvFiles() throws Exception {
        System.out.println("========== 开始导入 CSV 数据 ==========");
        
        try {
            // 1. 导入 users.csv
            importUsers("final_data/user.csv");
            
            // 2. 导入 recipes.csv
            importRecipes("final_data/recipes.csv");
            
            // 3. 导入 reviews.csv
            importReviews("final_data/reviews.csv");
            
            // 提交所有事务
            connectionManager.commit();
            System.out.println("\n========== 所有数据导入完成 ==========");
            
            // 统计各表的记录数（Task 3 要求）
            printTableStatistics();
            
        } catch (Exception e) {
            connectionManager.rollback();
            throw new Exception("数据导入失败，已回滚", e);
        }
    }
    
    /**
     * 导入用户数据
     */
    private void importUsers(String csvPath) throws Exception {
        System.out.println("\n--- 导入用户数据 (users.csv) ---");
        
        // 读取 CSV
        List<Map<String, String>> csvData = DataReader.readCsv(csvPath);
        System.out.println("读取了 " + csvData.size() + " 条用户记录");
        
        // 转换为数据库格式
        List<Map<String, Object>> dbData = new ArrayList<>();
        for (Map<String, String> row : csvData) {
            Map<String, Object> dbRow = new HashMap<>();
            
            // 字段映射和转换
            dbRow.put("authorid", DataReader.parseLong(row.get("AuthorId")));
            dbRow.put("authorname", DataReader.normalizeField(row.get("AuthorName")));
            dbRow.put("gender", DataReader.normalizeField(row.get("Gender")));
            dbRow.put("age", DataReader.parseInteger(row.get("Age")));
            
            dbData.add(dbRow);
        }
        
        // 清空表（可选）
        // dataWriter.truncateTable("users", false);
        
        // 批量插入
        String[] columns = {"authorid", "authorname", "gender", "age"};
        int inserted = dataWriter.batchInsert("users", columns, dbData);
        System.out.println("成功导入 " + inserted + " 条用户记录");
    }
    
    /**
     * 导入食谱数据
     */
    private void importRecipes(String csvPath) throws Exception {
        System.out.println("\n--- 导入食谱数据 (recipes.csv) ---");
        
        // 读取 CSV
        List<Map<String, String>> csvData = DataReader.readCsv(csvPath);
        System.out.println("读取了 " + csvData.size() + " 条食谱记录");
        
        // 转换为数据库格式
        List<Map<String, Object>> dbData = new ArrayList<>();
        for (Map<String, String> row : csvData) {
            Map<String, Object> dbRow = new HashMap<>();
            
            // 字段映射和转换（根据你的数据库表结构调整）
            dbRow.put("recipeid", DataReader.parseLong(row.get("RecipeId")));
            dbRow.put("authorid", DataReader.parseLong(row.get("AuthorId")));
            dbRow.put("name", DataReader.normalizeField(row.get("Name")));
            dbRow.put("cooktime", DataReader.normalizeField(row.get("CookTime")));
            dbRow.put("preptime", DataReader.normalizeField(row.get("PrepTime")));
            // ... 添加其他字段
            
            dbData.add(dbRow);
        }
        
        // 批量插入
        String[] columns = {"recipeid", "authorid", "name", "cooktime", "preptime"};
        int inserted = dataWriter.batchInsert("recipes", columns, dbData);
        System.out.println("成功导入 " + inserted + " 条食谱记录");
    }
    
    /**
     * 导入评论数据
     */
    private void importReviews(String csvPath) throws Exception {
        System.out.println("\n--- 导入评论数据 (reviews.csv) ---");
        
        // 读取 CSV
        List<Map<String, String>> csvData = DataReader.readCsv(csvPath);
        System.out.println("读取了 " + csvData.size() + " 条评论记录");
        
        // 转换为数据库格式
        List<Map<String, Object>> dbData = new ArrayList<>();
        for (Map<String, String> row : csvData) {
            Map<String, Object> dbRow = new HashMap<>();
            
            // 字段映射和转换
            dbRow.put("reviewid", DataReader.parseLong(row.get("ReviewId")));
            dbRow.put("recipeid", DataReader.parseLong(row.get("RecipeId")));
            dbRow.put("authorid", DataReader.parseLong(row.get("AuthorId")));
            dbRow.put("rating", DataReader.parseInteger(row.get("Rating")));
            dbRow.put("review", DataReader.normalizeField(row.get("Review")));
            // ... 添加其他字段
            
            dbData.add(dbRow);
        }
        
        // 批量插入
        String[] columns = {"reviewid", "recipeid", "authorid", "rating", "review"};
        int inserted = dataWriter.batchInsert("reviews", columns, dbData);
        System.out.println("成功导入 " + inserted + " 条评论记录");
    }
    
    /**
     * 打印各表的统计信息（Task 3 要求：提供每个实体表的记录数）
     */
    private void printTableStatistics() throws SQLException {
        System.out.println("\n========== 各表记录统计 ==========");
        
        String[] tables = {"users", "recipes", "reviews"};
        for (String table : tables) {
            try {
                long count = dataQuery.count(table, null);
                System.out.println(table + ": " + count + " 条记录");
            } catch (SQLException e) {
                System.out.println(table + ": 表不存在或查询失败");
            }
        }
    }
}

