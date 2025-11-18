package main;

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
import java.io.IOException;

public class PerformanceTest {
    private ConnectionManager connectionManager;
    private DataWriter dataWriter;
    private DataQuery dataQuery;
    private String testTableName;
    private int testDataSize;


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

    public long testSingleThreadInsert(List<TestRecord> data) throws SQLException {
        // 清空表
        dataWriter.truncateTable(testTableName, false);
        connectionManager.commit(); // 提交 TRUNCATE

        // 转换数据格式
        List<Map<String, Object>> dataList = new ArrayList<>();
        for (TestRecord record : data) {
            dataList.add(record.toMap());
        }

        // 执行插入并计时
        long startTime = System.currentTimeMillis();

        String[] columns = {"id", "name", "value", "category"};
        dataWriter.batchInsert(testTableName, columns, dataList);

        connectionManager.commit(); // 提交 INSERT

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public long testMultiThreadInsert(List<TestRecord> data, int threadCount) throws SQLException, InterruptedException {
        // 清空表
        dataWriter.truncateTable(testTableName, false);
        // *** 修复：必须立刻提交，释放 TRUNCATE 锁 ***
        connectionManager.commit();

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
                    // 每个线程使用自己的连接，并开启 autoCommit
                    ConnectionManager threadConnMgr = new ConnectionManager(
                            DatabaseConfig.defaultConfig(), true); // <-- true = autoCommit
                    DataWriter threadWriter = new DataWriter(threadConnMgr.getConnection(), 1000);

                    List<Map<String, Object>> chunkData = new ArrayList<>();
                    for (int j = start; j < end; j++) {
                        chunkData.add(data.get(j).toMap());
                    }

                    String[] columns = {"id", "name", "value", "category"};
                    threadWriter.batchInsert(testTableName, columns, chunkData);

                    // threadConnMgr.commit(); // <-- 不需要，因为 autoCommit=true
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


    public long testInMemorySearchNoIndex(List<TestRecord> allData, int queryCount) {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(allData.size()) + 1;

            long startTime = System.nanoTime();
            // 线性搜索 (O(n))
            for (TestRecord record : allData) {
                if (record.id == randomId) {
                    break;
                }
            }
            long endTime = System.nanoTime();
            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount; // 返回平均时间（纳秒）
    }

    public long testInMemorySearchWithIndex(BTreeIndex<Integer, TestRecord> index, int queryCount) {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(testDataSize) + 1;

            long startTime = System.nanoTime();
            index.get(randomId); // BTree 搜索 (O(log n))
            long endTime = System.nanoTime();

            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount; // 返回平均时间（纳秒）
    }

    public long testRawFileQuery(FileIOOperations fileIO, String filename, int queryCount) throws IOException {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(testDataSize) + 1;

            long startTime = System.nanoTime();
            // 每次查询都真正地读取文件
            fileIO.searchFileForId(filename, randomId);
            long endTime = System.nanoTime();

            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount; // 返回平均时间（纳秒）
    }


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
        System.out.println("2. 多线程批量插入测试（4 线程, 自动提交）...");
        long multiThreadTime = testMultiThreadInsert(testData, 4);
        System.out.println("多线程插入耗时: " + multiThreadTime + " 毫秒");
        System.out.println("平均每条记录: " + (multiThreadTime * 1.0 / testDataSize) + " 毫秒");
        System.out.println("性能对比 (多线程/单线程): " + String.format("%.2f 倍",
                (double) multiThreadTime / singleThreadTime));
        System.out.println();


        // 3. 查询性能对比
        int queryCount = 1000;
        System.out.println("3. 查询性能对比（执行 " + queryCount + " 次查询）...");

        // 加载数据到内存结构中
        System.out.println("将文件加载到内存中 (用于 File I/O 对比)...");
        FileIOOperations fileIO = new FileIOOperations("test_data");
        String testFilename = "test_data.csv";

        // 确保测试文件存在
        long fileWriteTime = fileIO.writeDataToFile(testData, testFilename);
        System.out.println("文件写入耗时: " + fileWriteTime + " 毫秒");


        // A. 数据库查询
        long dbQueryTime = testDatabaseQuery(queryCount);
        System.out.println("A. 数据库查询平均耗时: " + (dbQueryTime / 1_000_000.0) + " 毫秒");

        // B. 原始 File I/O 查询
        long rawFileQueryTime = testRawFileQuery(fileIO, testFilename, queryCount);
        System.out.println("B. 原始 File I/O 查询平均耗时: " + (rawFileQueryTime / 1_000_000.0) + " 毫秒");
        System.out.println();

        // C. 内存中搜索
        System.out.println("--- 附加对比：内存中搜索 ---");
        List<TestRecord> inMemoryData = fileIO.loadAllDataToMemory(testFilename);
        BTreeIndex<Integer, TestRecord> inMemoryIndex = fileIO.loadAllDataToBTree(testFilename);

        long memSearchNoIndexTime = testInMemorySearchNoIndex(inMemoryData, queryCount);
        System.out.println("C. 内存中搜索平均耗时 (无索引, ArrayList): " + (memSearchNoIndexTime / 1_000_000.0) + " 毫秒");

        long memSearchWithIndexTime = testInMemorySearchWithIndex(inMemoryIndex, queryCount);
        System.out.println("D. 内存中搜索平均耗时 (有索引, BTree): " + (memSearchWithIndexTime / 1_000_000.0) + " 毫秒");
        System.out.println();

        System.out.println("========== 对比结果 (Task 4) ==========");
        System.out.println("数据库 (A) vs 原始 File I/O (B): " + String.format("%.2f 倍",
                (double) rawFileQueryTime / dbQueryTime));
        System.out.println("数据库 (A) vs 内存搜索 (D): " + String.format("%.2f 倍",
                (double) dbQueryTime / memSearchWithIndexTime));


        System.out.println("\n========== 性能测试完成 ==========");
    }

    public long testRangeQuery(int queryCount) throws SQLException {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int minValue = random.nextInt(5000);
            int maxValue = minValue + random.nextInt(2000) + 500;

            long startTime = System.nanoTime();
            String sql = "SELECT * FROM " + testTableName + " WHERE value >= ? AND value <= ?";
            try (java.sql.PreparedStatement pstmt = connectionManager.getConnection().prepareStatement(sql)) {
                pstmt.setInt(1, minValue);
                pstmt.setInt(2, maxValue);
                pstmt.executeQuery();
            }
            long endTime = System.nanoTime();

            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount;
    }


    public long testComplexQuery(int queryCount) throws SQLException {
        Random random = new Random();
        String[] categories = {"A", "B", "C", "D", "E"};
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            String category = categories[random.nextInt(categories.length)];
            int minValue = random.nextInt(5000);

            long startTime = System.nanoTime();
            String sql = "SELECT category, COUNT(*) as count, AVG(value) as avg_value, MAX(value) as max_value " +
                    "FROM " + testTableName + " WHERE category = ? AND value >= ? GROUP BY category";
            try (java.sql.PreparedStatement pstmt = connectionManager.getConnection().prepareStatement(sql)) {
                pstmt.setString(1, category);
                pstmt.setInt(2, minValue);
                pstmt.executeQuery();
            }
            long endTime = System.nanoTime();

            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount;
    }


    public void testDifferentDataSizes() throws Exception {
        int[] dataSizes = {5000, 10000, 50000};
        int queryCount = 1000;

        for (int size : dataSizes) {
            List<TestRecord> testData = generateTestData(size);

            long singleThreadTime = testSingleThreadInsert(testData);
            long multiThreadTime = testMultiThreadInsert(testData, 4);

            this.testDataSize = size;
            long dbQueryTime = testDatabaseQuery(queryCount);

            FileIOOperations fileIO = new FileIOOperations("test_data");
            String testFilename = "test_data_" + size + ".csv";
            fileIO.writeDataToFile(testData, testFilename);
            long fileQueryTime = testRawFileQuery(fileIO, testFilename, queryCount);

            System.out.println(String.format("%d: 单线程=%dms, 多线程=%dms, 数据库=%.4fms, FileIO=%.4fms",
                    size, singleThreadTime, multiThreadTime,
                    dbQueryTime / 1_000_000.0, fileQueryTime / 1_000_000.0));
        }
    }


    public void testDifferentThreadCounts(int dataSize) throws Exception {
        int[] threadCounts = {2, 4, 8};
        List<TestRecord> testData = generateTestData(dataSize);
        long singleThreadTime = testSingleThreadInsert(testData);

        for (int threadCount : threadCounts) {
            long multiThreadTime = testMultiThreadInsert(testData, threadCount);
            System.out.println(String.format("%d线程: %dms (%.2fx)",
                    threadCount, multiThreadTime, (double) multiThreadTime / singleThreadTime));
        }
    }


    public void testDifferentQueryTypes(int queryCount) throws Exception {
        long pointQueryTime = testDatabaseQuery(queryCount);
        long rangeQueryTime = testRangeQuery(queryCount);
        long complexQueryTime = testComplexQuery(queryCount);

        System.out.println("点查询: " + (pointQueryTime / 1_000_000.0) + "ms");
        System.out.println("范围查询: " + (rangeQueryTime / 1_000_000.0) + "ms");
        System.out.println("复杂查询: " + (complexQueryTime / 1_000_000.0) + "ms");
    }


    public void runAdvancedPerformanceTest() throws Exception {
        System.out.println("4. 高级性能测试");
        System.out.println();

        testDifferentDataSizes();
        testDifferentThreadCounts(10000);

        this.testDataSize = 10000;
        List<TestRecord> testData = generateTestData(10000);
        testSingleThreadInsert(testData);
        testDifferentQueryTypes(1000);
    }
}