# PostgreSQL 连接程序使用说明

这是一个完整的 Java PostgreSQL 连接程序，采用模块化设计，支持数据读取、批量写入、查询、性能测试等功能。

## 项目结构

```
src/
├── DatabaseConfig.java      # 数据库配置类（支持配置文件和常量）
├── ConnectionManager.java   # 连接管理类（打开/关闭连接，事务管理）
├── DataReader.java          # 数据读取/解析工具类（支持 CSV/JSON）
├── DataWriter.java          # 数据写入类（批量插入，清空表）
├── DataQuery.java           # 数据查询类（条件筛选、统计、更新/删除）
├── BTreeIndex.java          # 简易 BTree 索引实现
├── PerformanceTest.java     # 性能测试类（多线程对比、索引对比）
└── PostgresMain.java        # 主程序演示所有功能

db.properties                # 数据库配置文件示例
```

## 功能特性

### 1. 数据库配置
- 支持从配置文件 `db.properties` 读取配置
- 支持使用默认配置常量
- 支持手动指定配置参数

### 2. 连接管理
- 自动加载 PostgreSQL 驱动
- 支持手动事务管理（commit/rollback）
- 自动资源释放（实现 AutoCloseable）

### 3. 数据读取
- 支持从 CSV 文件读取数据
- 支持从 JSON 文件读取数据（简化版）
- 自动处理字段格式（清理空白、处理 null 值）
- 提供类型转换工具（parseInteger, parseLong, parseDouble）

### 4. 数据写入
- 支持清空目标表（TRUNCATE TABLE，可选 CASCADE）
- 批量插入数据（使用 PreparedStatement 和 addBatch/executeBatch）
- 可配置批次大小
- 自动处理各种数据类型

### 5. 数据查询
- 条件筛选查询
- 统计记录数量
- 更新记录
- 删除记录（必须提供条件，防止误删）
- 支持自定义 SQL 查询

### 6. 性能测试
- 生成随机测试数据
- 对比单线程 vs 多线程插入性能
- 对比数据库查询 vs 内存索引查询性能
- 输出详细的性能报告

### 7. BTree 索引
- 简易 BTree 索引实现
- 支持快速查找
- 支持范围查询

## 编译和运行

### 前置要求
- Java 8 或更高版本
- PostgreSQL 数据库
- PostgreSQL JDBC 驱动（postgresql-42.2.5.jar）

### 编译

```bash
# Windows PowerShell
javac -cp postgresql-42.2.5.jar src/*.java

# Linux/Mac
javac -cp postgresql-42.2.5.jar src/*.java
```

### 配置数据库

编辑 `db.properties` 文件：

```properties
db.host=localhost
db.port=5432
db.database=lab_project
db.user=postgres
db.password=your_password
```

### 运行主程序

```bash
# Windows PowerShell
java -cp postgresql-42.2.5.jar;src PostgresMain

# Linux/Mac
java -cp postgresql-42.2.5.jar:src PostgresMain
```

## 使用示例

### 1. 基本使用

```java
// 加载配置
DatabaseConfig config = DatabaseConfig.fromFile("db.properties");

// 建立连接（手动管理事务）
ConnectionManager connMgr = new ConnectionManager(config, false);

try {
    // 数据写入
    DataWriter writer = new DataWriter(connMgr.getConnection(), 1000);
    writer.truncateTable("test_table", false);
    writer.batchInsert("test_table", columns, data);
    connMgr.commit();
    
    // 数据查询
    DataQuery query = new DataQuery(connMgr.getConnection());
    long count = query.count("test_table", null);
    List<Map<String, Object>> results = query.selectByConditions("test_table", conditions, null);
    
} catch (Exception e) {
    connMgr.rollback();
} finally {
    connMgr.close();
}
```

### 2. 读取 CSV 文件

```java
List<Map<String, String>> csvData = DataReader.readCsv("data.csv");
for (Map<String, String> row : csvData) {
    String name = row.get("name");
    Integer age = DataReader.parseInteger(row.get("age"));
    // 处理数据...
}
```

### 3. 批量插入数据

```java
List<Map<String, Object>> data = new ArrayList<>();
Map<String, Object> record = new HashMap<>();
record.put("id", 1);
record.put("name", "Test");
record.put("value", 100);
data.add(record);

String[] columns = {"id", "name", "value"};
DataWriter writer = new DataWriter(connection, 1000); // 批次大小 1000
writer.batchInsert("test_table", columns, data);
```

### 4. 条件查询

```java
DataQuery query = new DataQuery(connection);

// 条件筛选
Map<String, Object> conditions = new HashMap<>();
conditions.put("category", "A");
conditions.put("value", 100);
List<Map<String, Object>> results = query.selectByConditions("test_table", conditions, null);

// 统计数量
long count = query.count("test_table", conditions);

// 更新记录
Map<String, Object> updates = new HashMap<>();
updates.put("value", 200);
Map<String, Object> updateConditions = new HashMap<>();
updateConditions.put("id", 1);
int updated = query.update("test_table", updates, updateConditions);
```

### 5. 性能测试

```java
PerformanceTest perfTest = new PerformanceTest(connMgr, "test_table", 10000);
perfTest.createTestTable();
perfTest.runFullPerformanceTest();
```

### 6. 使用 BTree 索引

```java
BTreeIndex<Integer, String> index = new BTreeIndex<>();
index.put(1, "value1");
index.put(2, "value2");
index.put(3, "value3");

// 查找
String value = index.get(2);

// 范围查询
List<String> results = index.rangeQuery(1, 3);
```

## 关键设计说明

### 1. 事务管理
- 默认关闭自动提交（`autoCommit = false`）
- 需要手动调用 `commit()` 提交事务
- 异常时调用 `rollback()` 回滚事务
- 使用 try-finally 确保资源释放

### 2. 批量插入
- 使用 `PreparedStatement.addBatch()` 添加批次
- 达到批次大小时执行 `executeBatch()`
- 默认批次大小为 1000（可配置）
- 自动处理剩余数据

### 3. 异常处理
- 所有数据库操作都可能抛出 `SQLException`
- 建议使用 try-catch-finally 确保资源释放
- 事务失败时自动回滚

### 4. 性能优化
- 批量插入减少网络往返
- 使用 PreparedStatement 提高性能
- 多线程插入可提升吞吐量
- 内存索引适合频繁查询的场景

## 注意事项

1. **连接管理**：确保在使用完毕后关闭连接，使用 try-with-resources 或 finally 块
2. **事务管理**：批量操作时建议关闭自动提交，统一提交以提高性能
3. **批次大小**：根据数据量和内存情况调整批次大小，通常 1000-5000 较合适
4. **异常处理**：数据库操作必须处理 SQLException，确保事务正确回滚
5. **SQL 注入防护**：所有查询都使用 PreparedStatement，避免 SQL 注入
6. **删除操作**：删除操作必须提供条件，防止误删所有数据

## 扩展建议

1. **连接池**：生产环境建议使用连接池（如 HikariCP）
2. **日志记录**：添加日志框架（如 Log4j）记录操作
3. **配置文件**：支持更多配置选项（如连接超时、最大连接数等）
4. **JSON 解析**：如需完整 JSON 支持，可引入 JSON 库（如 Jackson、Gson）
5. **数据验证**：添加数据验证逻辑，确保数据质量

## 许可证

本项目仅供学习和参考使用。

