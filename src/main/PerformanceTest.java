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
        dataWriter.truncateTable(testTableName, false);
        connectionManager.commit();

        List<Map<String, Object>> dataList = new ArrayList<>();
        for (TestRecord record : data) {
            dataList.add(record.toMap());
        }

        long startTime = System.currentTimeMillis();

        String[] columns = {"id", "name", "value", "category"};
        dataWriter.batchInsert(testTableName, columns, dataList);

        connectionManager.commit();

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public long testMultiThreadInsert(List<TestRecord> data, int threadCount) throws SQLException, InterruptedException {
        dataWriter.truncateTable(testTableName, false);
        connectionManager.commit();

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
                    ConnectionManager threadConnMgr = new ConnectionManager(
                            DatabaseConfig.defaultConfig(), true);
                    DataWriter threadWriter = new DataWriter(threadConnMgr.getConnection(), 1000);

                    List<Map<String, Object>> chunkData = new ArrayList<>();
                    for (int j = start; j < end; j++) {
                        chunkData.add(data.get(j).toMap());
                    }

                    String[] columns = {"id", "name", "value", "category"};
                    threadWriter.batchInsert(testTableName, columns, chunkData);

                    threadConnMgr.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

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

        return totalTime / queryCount;
    }

    public long testInMemorySearchNoIndex(List<TestRecord> allData, int queryCount) {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(allData.size()) + 1;

            long startTime = System.nanoTime();
            for (TestRecord record : allData) {
                if (record.id == randomId) {
                    break;
                }
            }
            long endTime = System.nanoTime();
            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount;
    }

    public long testInMemorySearchWithIndex(BTreeIndex<Integer, TestRecord> index, int queryCount) {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(testDataSize) + 1;

            long startTime = System.nanoTime();
            index.get(randomId);
            long endTime = System.nanoTime();

            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount;
    }

    public long testRawFileQuery(FileIOOperations fileIO, String filename, int queryCount) throws IOException {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(testDataSize) + 1;

            long startTime = System.nanoTime();
            fileIO.searchFileForId(filename, randomId);
            long endTime = System.nanoTime();

            totalTime += (endTime - startTime);
        }

        return totalTime / queryCount;
    }

    public long testRangeQuery(int queryCount) throws SQLException {
        Random random = new Random();
        long totalTime = 0;

        for (int i = 0; i < queryCount; i++) {
            int minValue = random.nextInt(5000);
            int maxValue = minValue + random.nextInt(2000) + 500;

            long startTime = System.nanoTime();
            String sql = "SELECT * FROM " + testTableName + " WHERE value >= ? AND value <= ?";
            dataQuery.executeQuery(sql, minValue, maxValue);
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
            dataQuery.executeQuery(sql, category, minValue);
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

    public void runFullPerformanceTest() throws Exception {
        List<TestRecord> testData = generateTestData(testDataSize);

        long singleThreadTime = testSingleThreadInsert(testData);
        System.out.println("单线程插入: " + singleThreadTime + "ms");

        long multiThreadTime = testMultiThreadInsert(testData, 4);
        System.out.println("多线程插入: " + multiThreadTime + "ms");

        int queryCount = 1000;
        FileIOOperations fileIO = new FileIOOperations("test_data");
        String testFilename = "test_data.csv";
        fileIO.writeDataToFile(testData, testFilename);

        long dbQueryTime = testDatabaseQuery(queryCount);
        System.out.println("数据库查询: " + (dbQueryTime / 1_000_000.0) + "ms");

        long rawFileQueryTime = testRawFileQuery(fileIO, testFilename, queryCount);
        System.out.println("File I/O查询: " + (rawFileQueryTime / 1_000_000.0) + "ms");

        List<TestRecord> inMemoryData = fileIO.loadAllDataToMemory(testFilename);
        BTreeIndex<Integer, TestRecord> inMemoryIndex = fileIO.loadAllDataToBTree(testFilename);

        long memSearchNoIndexTime = testInMemorySearchNoIndex(inMemoryData, queryCount);
        System.out.println("内存搜索(无索引): " + (memSearchNoIndexTime / 1_000_000.0) + "ms");

        long memSearchWithIndexTime = testInMemorySearchWithIndex(inMemoryIndex, queryCount);
        System.out.println("内存搜索(有索引): " + (memSearchWithIndexTime / 1_000_000.0) + "ms");
    }

    public void runAdvancedPerformanceTest() throws Exception {
        testDifferentDataSizes();
        testDifferentThreadCounts(10000);

        this.testDataSize = 10000;
        List<TestRecord> testData = generateTestData(10000);
        testSingleThreadInsert(testData);
        testDifferentQueryTypes(1000);
    }
}