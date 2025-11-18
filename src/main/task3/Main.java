package main.task3;

import main.common.ConnectionManager;
import main.common.DatabaseConfig;
import main.task4.PerformanceTest;

public class Main {

    public static void main(String[] args) {
        try {
            System.out.println("1. 数据库配置");
            DatabaseConfig config;

            try {
                config = DatabaseConfig.fromFile("db.properties");
                System.out.println("从配置文件加载: " + config);
            } catch (Exception e) {
                System.out.println("使用默认配置（端口12138）");
                config = DatabaseConfig.defaultConfig();
                System.out.println(config);
            }

            System.out.println();

            System.out.println("2. 建立数据库连接");
            ConnectionManager connMgr = new ConnectionManager(config, false);
            System.out.println("连接成功！");
            System.out.println("连接状态: " + (connMgr.isConnectionValid() ? "有效" : "无效"));
            System.out.println();

            try {
                System.out.println("3. CSV 数据导入");
                String dataDirectory = "final_data";
                
                CsvDataImporter importer = new CsvDataImporter(connMgr, dataDirectory);
                
                try {
                    importer.importAllCsvFiles();
                } catch (Exception e) {
                    System.out.println("导入失败: " + e.getMessage());
                }
                System.out.println();

                System.out.println("4. 性能测试");
                PerformanceTest perfTest = new PerformanceTest(connMgr, "test_performance", 10000);
                perfTest.createTestTable();
                
                if (args.length > 0 && args[0].equals("advanced")) {
                    perfTest.runAdvancedPerformanceTest();
                } else {
                perfTest.runFullPerformanceTest();
                    System.out.println("\n提示: 运行 'java -cp ... main.Main advanced' 可执行高级性能测试");
                }
                System.out.println();

            } finally {
                System.out.println("5.关闭连接");
                connMgr.close();
                System.out.println("连接已关闭");
            }

        } catch (Exception e) {
            System.err.println("程序执行出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}