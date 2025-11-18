package task4;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import task3.ConnectionManager;
import task3.DataQuery;
import task3.DataWriter;
import task3.DatabaseConfig;

/**
 * 性能测试类
 * 对比多线程 vs 单线程插入耗时，以及内存索引 vs 数据库查询耗时
 */
public class PerformanceTest {
    private ConnectionManager connectionManager;
    private DataWriter dataWriter;
    private DataQuery dataQuery;
    private String testTableName;
    private int testDataSize;

    /**
     * 测试数据记录
     */
    public static class TestRecord {
        public int id;
        public String name;
        public int value;
        public String category;

        public TestRecord(int id, String name, int value, String category) {
            this.id = id;
            this.name = name;
            this.value = value;
            this.category = category;
        }

        public Map<String, Object> toMap() {
            Map<String, Object> map = new HashMap<>();
            map.put("id", id);
            map.put("name", name);
            map.put("value", value);
            map.put("category", category);
            return map;
        }
    }

    public PerformanceTest(ConnectionManager connectionManager, String testTableName, int testDataSize) {
        this.connectionManager = connectionManager;
        this.testTableName = testTableName;
        this.testDataSize = testDataSize;

        Connection conn = connectionManager.getConnection();
        this.dataWriter = new DataWriter(conn, 1000);
        this.dataQuery = new DataQuery(conn);
    }

    /**
     * 创建测试表
     */
    public void createTestTable() throws SQLException {
        Connection conn = connectionManager.getConnection();
        String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id INTEGER PRIMARY KEY, " +
                        "name VARCHAR(100), " +
                        "value INTEGER, " +
                        "category VARCHAR(50)" +
                        ")", testTableName
        );

        try (java.sql.Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableSql);
            System.out.println("测试表创建成功: " + testTableName);
        }
    }

    /**
     * 生成随机测试数据
     */
    public List<TestRecord> generateTestData(int size) {
        List<TestRecord> data = new ArrayList<>();
        Random random = new Random();
        String[] categories = {"A", "B", "C", "D", "E"};

        for (int i = 1; i <= size; i++) {
            String name = "Record_" + i;
            int value = random.nextInt(10000);
            String category = categories[random.nextInt(categories.length)];
            data.add(new TestRecord(i, name, value, category));
        }

        return data;
    }

    /**
     * 单线程批量插入测试
     */
    public long testSingleThreadInsert(List<TestRecord> data) throws SQLException {
        // 清空表
        dataWriter.truncateTable(testTableName, false);

        // 转换数据格式
        List<Map<String, Object>> dataList = new ArrayList<>();
        for (TestRecord record : data) {
            dataList.add(record.toMap());
        }

        // 执行插入并计时
        long startTime = System.currentTimeMillis();

        String[] columns = {"id", "name", "value", "category"};
        dataWriter.batchInsert(testTableName, columns, dataList);

        connectionManager.commit();

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    /**
     * 多线程批量插入测试
     */
    public long testMultiThreadInsert(List<TestRecord> data, int threadCount) throws SQLException, InterruptedException {
        // 清空表
        dataWriter.truncateTable(testTableName, false);

        // 将数据分割到多个线程
        int chunkSize = data.size() / threadCount;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            final int start = threadIndex * chunkSize;
            final int end = (threadIndex == threadCount - 1) ? data.size() : (threadIndex + 1) * chunkSize;

            executor.submit(() -> {
                try {
                    // 每个线程使用自己的连接
                    ConnectionManager threadConnMgr = new ConnectionManager(
                            DatabaseConfig.defaultConfig(), false);
                    DataWriter threadWriter = new DataWriter(threadConnMgr.getConnection(), 1000);

                    List<Map<String, Object>> chunkData = new ArrayList<>();
                    for (int j = start; j < end; j++) {
                        chunkData.add(data.get(j).toMap());
                    }

                    String[] columns = {"id", "name", "value", "category"};
                    threadWriter.batchInsert(testTableName, columns, chunkData);

                    threadConnMgr.commit();
                    threadConnMgr.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待所有线程完成
        latch.await();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    /**
     * 数据库查询性能测试
     */
    public long testDatabaseQuery(int queryCount) throws SQLException {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(testDataSize) + 1;

            long startTime = System.nanoTime();

            Map<String, Object> conditions = new HashMap<>();
            conditions.put("id", randomId);
            dataQuery.selectByConditions(testTableName, conditions, null);

            long endTime = System.nanoTime();
            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount; // 返回平均时间（纳秒）
    }

    /**
     * 内存索引查询性能测试
     */
    public long testMemoryIndexQuery(List<TestRecord> data, int queryCount) {
        // 构建 BTree 索引
        BTreeIndex<Integer, TestRecord> index = new BTreeIndex<>();
        for (TestRecord record : data) {
            index.put(record.id, record);
        }

        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(testDataSize) + 1;

            long startTime = System.nanoTime();
            index.get(randomId);
            long endTime = System.nanoTime();

            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount; // 返回平均时间（纳秒）
    }

    /**
     * 运行完整的性能测试（包含 DBMS vs File I/O 对比）
     */
    public void runFullPerformanceTest() throws Exception {
        System.out.println("========== 性能测试开始 ==========");
        System.out.println("测试数据规模: " + testDataSize + " 条记录");
        System.out.println();

        // 生成测试数据
        System.out.println("生成测试数据...");
        List<TestRecord> testData = generateTestData(testDataSize);
        System.out.println("测试数据生成完成");
        System.out.println();

        // 1. 单线程插入测试
        System.out.println("1. 单线程批量插入测试...");
        long singleThreadTime = testSingleThreadInsert(testData);
        System.out.println("单线程插入耗时: " + singleThreadTime + " 毫秒");
        System.out.println("平均每条记录: " + (singleThreadTime * 1.0 / testDataSize) + " 毫秒");
        System.out.println();

        // 2. 多线程插入测试
        System.out.println("2. 多线程批量插入测试（4 线程）...");
        long multiThreadTime = testMultiThreadInsert(testData, 4);
        System.out.println("多线程插入耗时: " + multiThreadTime + " 毫秒");
        System.out.println("平均每条记录: " + (multiThreadTime * 1.0 / testDataSize) + " 毫秒");
        System.out.println("性能提升: " + String.format("%.2f%%",
                (1.0 - (double) multiThreadTime / singleThreadTime) * 100));
        System.out.println();

        // 3. 数据库查询 vs 内存索引查询
        int queryCount = 1000;
        System.out.println("3. 查询性能对比（执行 " + queryCount + " 次查询）...");

        long dbQueryTime = testDatabaseQuery(queryCount);
        System.out.println("数据库查询平均耗时: " + (dbQueryTime / 1_000_000.0) + " 毫秒");

        long indexQueryTime = testMemoryIndexQuery(testData, queryCount);
        System.out.println("内存索引查询平均耗时: " + (indexQueryTime / 1_000_000.0) + " 毫秒");
        System.out.println("性能提升: " + String.format("%.2f 倍",
                (double) dbQueryTime / indexQueryTime));
        System.out.println();

        // 4. DBMS vs File I/O 对比（Task 4 核心要求）
        System.out.println("4. DBMS vs File I/O 性能对比...");
        FileIOOperations fileIO = new FileIOOperations("test_data");
        
        // 文件写入测试
        long fileWriteTime = fileIO.writeDataToFile(testData, "test_data.csv");
        System.out.println("文件写入耗时: " + fileWriteTime + " 毫秒");
        
        // 文件读取测试（无索引）
        long fileReadTime = fileIO.readDataFromFile("test_data.csv", queryCount);
        System.out.println("文件读取平均耗时（无索引）: " + (fileReadTime / 1_000_000.0) + " 毫秒");
        
        // 文件读取测试（有索引）
        long fileReadTimeWithIndex = fileIO.readDataFromFileWithIndex("test_data.csv", queryCount);
        System.out.println("文件读取平均耗时（有索引）: " + (fileReadTimeWithIndex / 1_000_000.0) + " 毫秒");
        
        // 对比结果
        System.out.println("\n对比结果:");
        System.out.println("数据库查询 vs 文件读取（无索引）: " + String.format("%.2f 倍",
                (double) fileReadTime / dbQueryTime));
        System.out.println("数据库查询 vs 文件读取（有索引）: " + String.format("%.2f 倍",
                (double) fileReadTimeWithIndex / dbQueryTime));
        System.out.println();

        System.out.println("========== 性能测试完成 ==========");
    }
}
