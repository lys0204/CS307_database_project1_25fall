package task4;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * File I/O 操作类
 * 实现文件系统的数据读写操作，用于与 DBMS 进行性能对比
 * 满足 Task 4 的要求：比较 DBMS 与 File I/O
 */
public class FileIOOperations {
    private String dataDir;
    
    public FileIOOperations(String dataDir) {
        this.dataDir = dataDir;
    }
    
    /**
     * 将数据写入文件（模拟数据库插入）
     */
    public long writeDataToFile(List<PerformanceTest.TestRecord> data, String filename) throws IOException {
        long startTime = System.currentTimeMillis();
        
        Path filePath = Paths.get(dataDir, filename);
        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            // 写入表头
            writer.write("id,name,value,category");
            writer.newLine();
            
            // 写入数据
            for (PerformanceTest.TestRecord record : data) {
                writer.write(String.format("%d,%s,%d,%s",
                    record.id, record.name, record.value, record.category));
                writer.newLine();
            }
        }
        
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
    
    /**
     * 从文件读取数据（模拟数据库查询）
     */
    public long readDataFromFile(String filename, int queryCount) throws IOException {
        Path filePath = Paths.get(dataDir, filename);
        if (!Files.exists(filePath)) {
            throw new IOException("文件不存在: " + filePath);
        }
        
        // 先加载所有数据到内存
        List<PerformanceTest.TestRecord> allData = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            reader.readLine(); // 跳过表头
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 4) {
                    int id = Integer.parseInt(parts[0]);
                    String name = parts[1];
                    int value = Integer.parseInt(parts[2]);
                    String category = parts[3];
                    allData.add(new PerformanceTest.TestRecord(id, name, value, category));
                }
            }
        }
        
        // 执行查询（模拟数据库查询）
        long totalTime = 0;
        java.util.Random random = new java.util.Random();
        
        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(allData.size()) + 1;
            
            long startTime = System.nanoTime();
            // 线性搜索（模拟文件系统查询）
            for (PerformanceTest.TestRecord record : allData) {
                if (record.id == randomId) {
                    break;
                }
            }
            long endTime = System.nanoTime();
            
            totalTime += (endTime - startTime);
        }
        
        return totalTime / queryCount; // 返回平均时间（纳秒）
    }
    
    /**
     * 使用索引从文件读取数据（使用 BTree 索引优化）
     */
    public long readDataFromFileWithIndex(String filename, int queryCount) throws IOException {
        Path filePath = Paths.get(dataDir, filename);
        if (!Files.exists(filePath)) {
            throw new IOException("文件不存在: " + filePath);
        }
        
        // 加载数据并构建索引
        BTreeIndex<Integer, PerformanceTest.TestRecord> index = new BTreeIndex<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            reader.readLine(); // 跳过表头
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 4) {
                    int id = Integer.parseInt(parts[0]);
                    String name = parts[1];
                    int value = Integer.parseInt(parts[2]);
                    String category = parts[3];
                    PerformanceTest.TestRecord record = new PerformanceTest.TestRecord(id, name, value, category);
                    index.put(id, record);
                }
            }
        }
        
        // 使用索引执行查询
        long totalTime = 0;
        java.util.Random random = new java.util.Random();
        
        for (int i = 0; i < queryCount; i++) {
            int randomId = random.nextInt(index.size()) + 1;
            
            long startTime = System.nanoTime();
            index.get(randomId);
            long endTime = System.nanoTime();
            
            totalTime += (endTime - startTime);
        }
        
        return totalTime / queryCount; // 返回平均时间（纳秒）
    }
    
    /**
     * 删除文件
     */
    public void deleteFile(String filename) throws IOException {
        Path filePath = Paths.get(dataDir, filename);
        Files.deleteIfExists(filePath);
    }
}

