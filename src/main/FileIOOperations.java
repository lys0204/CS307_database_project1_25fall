package main;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileIOOperations {
    private String dataDir;

    public FileIOOperations(String dataDir) {
        this.dataDir = dataDir;
    }

    public long writeDataToFile(List<PerformanceTest.TestRecord> data, String filename) throws IOException {
        long startTime = System.currentTimeMillis();

        Path dirPath = Paths.get(dataDir);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        Path filePath = Paths.get(dataDir, filename);
        try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
            writer.write("id,name,value,category");
            writer.newLine();

            for (PerformanceTest.TestRecord record : data) {
                writer.write(String.format("%d,%s,%d,%s",
                        record.id, record.name, record.value, record.category));
                writer.newLine();
            }
        }

        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public PerformanceTest.TestRecord searchFileForId(String filename, int idToFind) throws IOException {
        Path filePath = Paths.get(dataDir, filename);
        if (!Files.exists(filePath)) {
            throw new IOException("文件不存在: " + filePath);
        }

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 4) {
                    try {
                        int id = Integer.parseInt(parts[0]);
                        if (id == idToFind) {
                            String name = parts[1];
                            int value = Integer.parseInt(parts[2]);
                            String category = parts[3];
                            return new PerformanceTest.TestRecord(id, name, value, category);
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }
        return null;
    }

    public List<PerformanceTest.TestRecord> loadAllDataToMemory(String filename) throws IOException {
        Path filePath = Paths.get(dataDir, filename);
        if (!Files.exists(filePath)) {
            throw new IOException("文件不存在: " + filePath);
        }

        List<PerformanceTest.TestRecord> allData = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 4) {
                    try {
                        int id = Integer.parseInt(parts[0]);
                        String name = parts[1];
                        int value = Integer.parseInt(parts[2]);
                        String category = parts[3];
                        allData.add(new PerformanceTest.TestRecord(id, name, value, category));
                    } catch (NumberFormatException e) {
                        System.err.println("解析数据时出错: " + e.getMessage());
                    }
                }
            }
        }
        return allData;
    }

    public BTreeIndex<Integer, PerformanceTest.TestRecord> loadAllDataToBTree(String filename) throws IOException {
        Path filePath = Paths.get(dataDir, filename);
        if (!Files.exists(filePath)) {
            throw new IOException("文件不存在: " + filePath);
        }

        BTreeIndex<Integer, PerformanceTest.TestRecord> index = new BTreeIndex<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length >= 4) {
                    try {
                        int id = Integer.parseInt(parts[0]);
                        String name = parts[1];
                        int value = Integer.parseInt(parts[2]);
                        String category = parts[3];
                        PerformanceTest.TestRecord record = new PerformanceTest.TestRecord(id, name, value, category);
                        index.put(id, record);
                    } catch (NumberFormatException e) {
                        System.err.println("解析数据时出错: " + e.getMessage());
                    }
                }
            }
        }
        return index;
    }
}