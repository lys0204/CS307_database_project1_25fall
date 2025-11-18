# SUSTC Recipe Database Project

## 项目概述

本项目是 CS307 课程的数据库大作业，实现了 SUSTC（Sustainable Technology for Cook）食谱网站的数据库管理系统。  
项目涵盖：

- **Task 3：数据导入** – 从原始 CSV 文件自动清洗、拆分并导入到 PostgreSQL 中的 12 张表；
- **Task 4：性能测试** – 设计统一的实验框架，对比 DBMS 与 File I/O、多线程与单线程、不同查询方式的性能。

所有核心逻辑均使用 Java 和 JDBC 实现，并包含文件 I/O 与 BTree 内存索引等对比方案。

---

## 项目结构

项目源代码位于 `src/main/` 目录，主要文件如下：

src/main/
├── Main.java               # 主入口：导入 + 基础性能测试 / 高级测试（通过参数控制）
├── MainAdvanced.java       # 专用入口：导入 + 高级性能测试
├── DatabaseConfig.java     # 数据库配置管理（读取 db.properties）
├── ConnectionManager.java  # 数据库连接与事务管理（AutoCloseable）
├── DataReader.java         # CSV 数据读取与解析工具
├── DataWriter.java         # 批量写入工具（PreparedStatement + Batch）
├── DataQuery.java          # 通用查询与计数工具
├── CsvDataImporter.java    # CSV 数据导入管线（Task 3 核心）
├── PerformanceTest.java    # 性能测试与对比（Task 4 核心）
├── FileIOOperations.java   # 文件读写与基于文件的查询
├── BTreeIndex.java         # 简单 B-Tree 索引实现（内存搜索）
├── database_schema.sql     # 12 张表的建表 SQL
├── db.properties           # 数据库连接配置
└── README.md               # 项目说明文档---

## 数据库设计概览

### 主要实体表

- **`users`** – 用户表  
  - `authorid`：用户 ID（主键）  
  - `authorname`：用户名  
  - `gender`：性别  
  - `age`：年龄  

- **`recipes`** – 食谱表  
  - `recipeid`：食谱 ID（主键）  
  - `authorid`：作者 ID（外键，引用 `users(authorid)`）  
  - `name`：食谱名称  
  - `cooktime` / `preptime`：烹饪/准备时间  
  - `datepublished`：发布时间  
  - `description`：描述  
  - `recipecategory`：类别  
  - `recipeservings` / `recipeyield`：份量信息  

- **`reviews`** – 评论表  
  - `reviewid`：评论 ID（主键）  
  - `recipeid`：食谱 ID（外键）  
  - `authorid`：用户 ID（外键）  
  - `rating`：评分  
  - `review`：评论内容  
  - `datesubmitted` / `datemodified`：提交/修改时间  

- **其他实体表**  
  - `nutrition`：营养信息（calories、fatcontent、proteincontent 等）  
  - `instructions`：步骤信息（recipeid + stepnumber + instructiontext）  
  - `keywords`：关键词字典  
  - `ingredients`：原料字典  

### 多对多与行为表

- `recipe_keywords`：食谱–关键词多对多关系  
- `recipe_ingredients`：食谱–原料多对多关系  
- `user_favorite_recipes`：用户收藏的食谱  
- `user_liked_reviews`：用户点赞的评论  
- `user_follows`：用户关注关系  

所有表通过主键、外键和必要的联合唯一约束保持参照完整性，并在主键和常用查询字段上建立索引。

---

## 环境准备

### 依赖

- **Java**：JDK 8+（推荐 11+）  
- **数据库**：PostgreSQL 12+  
- **JDBC 驱动**：`postgresql-42.x.x.jar`（放在项目根目录）

### 数据库初始化

在 PostgreSQL 中执行：

-- 创建数据库（名称可自定义）
CREATE DATABASE sustc_recipe_db;

-- 切换到该数据库
\c sustc_recipe_db;

-- 执行建表脚本
\i database_schema.sql### 配置文件 `db.properties`

在项目根目录创建或修改 `db.properties`：

db.host=localhost
db.port=5432
db.database=sustc_recipe_db
db.user=postgres
db.password=your_password### 数据文件

将原始 CSV 放在项目根目录下的 `final_data/` 目录：

- `final_data/user.csv`
- `final_data/recipes.csv`
- `final_data/reviews.csv`

---

## Task 3：数据导入（Data Import）

### 功能说明

`CsvDataImporter` 实现从 CSV 到数据库的完整导入流程：

- 读取 `user.csv` / `recipes.csv` / `reviews.csv`；
- 清洗数据（去重、过滤关键字段为空的记录、类型转换）；
- 填充实体表：`users`、`recipes`、`reviews`、`nutrition`、`instructions`；
- 填充维度表：`keywords` 与 `ingredients`；
- 构建并写入多对多及用户行为表：`recipe_keywords`、`recipe_ingredients`、`user_favorite_recipes`、`user_liked_reviews`、`user_follows`；
- 导入结束后统计每个表的记录数，用于验证导入是否成功。

### 运行方式

在 Windows PowerShell 中（以 `D:\ideaprograms\DataBase` 为例）：
hell
cd D:\ideaprograms\DataBase

# 编译
javac -cp ".;postgresql-42.2.5.jar" src\main\*.java -d .

# 导入 + 基础性能测试
java -cp ".;postgresql-42.2.5.jar" main.Main

# 导入 + 高级性能测试（使用参数）
java -cp ".;postgresql-42.2.5.jar" main.Main advanced

# 导入 + 高级性能测试（专用入口）
java -cp ".;postgresql-42.2.5.jar" main.MainAdvanced### 技术要点

- 使用 `PreparedStatement + addBatch()` 做批量插入，默认 batch size 约为 1000。
- 对主键和唯一约束使用 `ON CONFLICT DO NOTHING`，支持幂等导入和重复执行。
- 在一个大事务中完成导入：出错时回滚、成功时统一提交。
- 使用内存缓存（`keywordCache`、`ingredientCache`）加速多对多关联表的构建。

---

## Task 4：DBMS vs File I/O 性能测试

### 功能说明

`PerformanceTest` 提供了一套可复现实验，用于比较不同存储和访问方式的性能，包括：

- 单线程 vs 多线程的批量插入；
- PostgreSQL 查询 vs 纯文件 I/O 查询；
- 内存线性搜索 vs BTree 索引搜索；
- 不同数据规模（5k / 10k / 50k）下性能变化；
- 不同线程数（2 / 4 / 8）下插入时间变化；
- 不同查询类型（点查询、范围查询、复杂聚合查询）的耗时差异。

### 测试数据与表结构

- 使用测试表 `test_performance`：
  - `id`：主键；
  - `value`：整数测试值；
  - `category`：类别标签（如 A~E）。
- 通过 `generateTestData(size)` 生成随机数据，并写入：
  - 数据库中的 `test_performance` 表；
  - 磁盘文件 `test_data/*.csv` 用于 File I/O 测试；
  - 内存结构（`ArrayList` 与 `BTreeIndex`）用于内存查询测试。

### 示例结果（摘要）

（取一次典型实验的数据）：

- **不同数据规模（插入与查询）**  
  - 5k：单线程 139ms，4 线程 197ms，DB 查询 0.3023ms，文件查询 1.0027ms  
  - 10k：单线程 132ms，4 线程 190ms，DB 查询 0.1226ms，文件查询 1.1718ms  
  - 50k：单线程 579ms，4 线程 331ms，DB 查询 0.1405ms，文件查询 4.8613ms  

- **不同线程数（10k 记录，插入时间）**  
  - 2 线程：158ms（约 1.66× 单线程）  
  - 4 线程：142ms（约 1.49× 单线程）  
  - 8 线程：164ms（约 1.73× 单线程）  

- **不同查询类型（1000 次查询平均耗时）**  
  - 点查询：约 0.0838ms  
  - 范围查询：约 2.0806ms  
  - 复杂聚合查询：约 1.6534ms  

### 结论简要

- 在小规模数据下，单线程批量插入已经很快，多线程并不一定更优；在 50k 级别数据时，多线程开始显示明显优势。
- 在相同数据规模下，数据库查询普遍比直接扫描文件快一个数量级以上，数据规模越大优势越明显。
- 将数据加载到内存并使用 BTree 索引，可以获得最高的查询性能，但需要额外的内存开销，适合读多写少、数据量适中且常驻内存的场景。

---

## 故障排查

- **连接失败**  
  - 检查 PostgreSQL 服务是否运行；  
  - 检查 `db.properties` 配置是否正确；  
  - 检查防火墙和端口占用情况。

- **CSV 导入失败**  
  - 确认 `final_data/` 目录存在且文件名正确；  
  - 检查 CSV 分隔符和编码（建议 UTF-8）；  
  - 若提示主键冲突，可确认是否在已有数据基础上重复导入（当前实现会忽略重复）。

- **性能异常**  
  - 确认测试前已清空 `test_performance` 表（脚本中会自动 TRUNCATE）；  
  - 避免在其他会话中对同一表执行长事务；  
  - 按需调整 batch size 或线程数。

---

## 说明

本项目仅用于 CS307 课程的学习与展示，代码和实验结果用于说明：

- 如何从原始文件构建关系模式并进行数据导入；
- 如何使用 Java + JDBC 操作数据库；
- 如何设计和实现简单的性能测试框架，对比 DBMS、文件系统和内存结构的表现。