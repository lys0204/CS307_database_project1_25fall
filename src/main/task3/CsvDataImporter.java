package main;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CsvDataImporter {
    private ConnectionManager connectionManager;
    private DataWriter dataWriter;
    private DataQuery dataQuery;
    private String dataDirectory;
    private Connection conn;

    private Map<String, Long> keywordCache = new HashMap<>();
    private Map<String, Long> ingredientCache = new HashMap<>();

    private List<Map<String, Object>> usersData = new ArrayList<>();
    private List<Map<String, Object>> recipesData = new ArrayList<>();
    private List<Map<String, Object>> reviewsData = new ArrayList<>();
    private List<Map<String, Object>> nutritionData = new ArrayList<>();
    private List<Map<String, Object>> instructionsData = new ArrayList<>();

    private List<Map<String, Object>> recipeKeywordsData = new ArrayList<>();
    private List<Map<String, Object>> recipeIngredientsData = new ArrayList<>();
    private List<Map<String, Object>> userFavoritesData = new ArrayList<>();
    private List<Map<String, Object>> userLikesData = new ArrayList<>();
    private List<Map<String, Object>> userFollowsData = new ArrayList<>();

    private Set<String> allKeywords = new HashSet<>();
    private Set<String> allIngredients = new HashSet<>();


    public CsvDataImporter(ConnectionManager connectionManager, String dataDirectory) {
        this.connectionManager = connectionManager;
        this.dataDirectory = dataDirectory;
        this.conn = connectionManager.getConnection();
        this.dataWriter = new DataWriter(conn, 1000);
        this.dataQuery = new DataQuery(conn);
    }

    public void importAllCsvFiles() throws Exception {
        try {
            readAndPrepareUsers(dataDirectory + "/user.csv");
            readAndPrepareRecipes(dataDirectory + "/recipes.csv");
            readAndPrepareReviews(dataDirectory + "/reviews.csv");
            populateM2MTables();
            insertAllData();
            connectionManager.commit();
            printTableStatistics();
        } catch (Exception e) {
            connectionManager.rollback();
            e.printStackTrace();
            throw new Exception("数据导入失败，已回滚", e);
        }
    }

    private void readAndPrepareUsers(String csvPath) throws Exception {
        List<Map<String, String>> csvData = DataReader.readCsv(csvPath);
        System.out.println("读取了 " + csvData.size() + " 条 user 记录");

        Set<Long> seenIds = new HashSet<>();
        for (Map<String, String> row : csvData) {
            Long authorId = DataReader.parseLong(row.get("AuthorId"));
            String authorName = DataReader.normalizeField(row.get("AuthorName"));

            if (authorId == null || !seenIds.add(authorId) || authorName == null) {
                continue;
            }

            Map<String, Object> userRow = new HashMap<>();
            userRow.put("authorid", authorId);
            userRow.put("authorname", authorName);
            userRow.put("gender", DataReader.normalizeField(row.get("Gender")));
            userRow.put("age", DataReader.parseInteger(row.get("Age")));
            usersData.add(userRow);

            List<Long> followerIds = DataReader.parseCsvIdList(row.get("FollowerUsers"));
            if (followerIds.isEmpty()) {
                followerIds = DataReader.parseCsvIdList(row.get("FollowerSystemList"));
            }
            if (followerIds.isEmpty()) {
                followerIds = DataReader.parseCsvIdList(row.get("FollowingUsers"));
            }
            for (Long followerId : followerIds) {
                if (followerId != null) {
                    Map<String, Object> followRow = new HashMap<>();
                    followRow.put("followerid", followerId);
                    followRow.put("followingid", authorId);
                    userFollowsData.add(followRow);
                }
            }
        }
    }

    private void readAndPrepareRecipes(String csvPath) throws Exception {
        List<Map<String, String>> csvData = DataReader.readCsv(csvPath);
        System.out.println("读取了 " + csvData.size() + " 条 recipe 记录");

        Set<Long> seenIds = new HashSet<>();
        for (Map<String, String> row : csvData) {
            Long recipeId = DataReader.parseLong(row.get("RecipeId"));
            String name = DataReader.normalizeField(row.get("Name"));
            Long authorId = DataReader.parseLong(row.get("AuthorId"));

            if (recipeId == null || !seenIds.add(recipeId) || name == null) {
                continue;
            }

            Map<String, Object> recipeRow = new HashMap<>();
            recipeRow.put("recipeid", recipeId);
            recipeRow.put("authorid", authorId);
            recipeRow.put("name", name);
            recipeRow.put("cooktime", DataReader.normalizeField(row.get("CookTime")));
            recipeRow.put("preptime", DataReader.normalizeField(row.get("PrepTime")));
            recipeRow.put("datepublished", DataReader.parseTimestamp(row.get("DatePublished")));
            recipeRow.put("description", DataReader.normalizeField(row.get("Description")));
            recipeRow.put("recipecategory", DataReader.normalizeField(row.get("RecipeCategory")));
            recipeRow.put("recipeservings", DataReader.parseInteger(row.get("RecipeServings")));
            recipeRow.put("recipeyield", DataReader.normalizeField(row.get("RecipeYield")));
            recipesData.add(recipeRow);

            Double calories = DataReader.parseDouble(row.get("Calories"));
            if (calories != null) {
                Map<String, Object> nutritionRow = new HashMap<>();
                nutritionRow.put("recipeid", recipeId);
                nutritionRow.put("calories", calories);
                nutritionRow.put("fatcontent", DataReader.parseDouble(row.get("FatContent")));
                nutritionRow.put("saturatedfatcontent", DataReader.parseDouble(row.get("SaturatedFatContent")));
                nutritionRow.put("cholesterolcontent", DataReader.parseDouble(row.get("CholesterolContent")));
                nutritionRow.put("sodiumcontent", DataReader.parseDouble(row.get("SodiumContent")));
                nutritionRow.put("carbohydratecontent", DataReader.parseDouble(row.get("CarbohydrateContent")));
                nutritionRow.put("fibercontent", DataReader.parseDouble(row.get("FiberContent")));
                nutritionRow.put("sugarcontent", DataReader.parseDouble(row.get("SugarContent")));
                nutritionRow.put("proteincontent", DataReader.parseDouble(row.get("ProteinContent")));
                nutritionData.add(nutritionRow);
            }

            List<String> steps = DataReader.parseCsvList(row.get("RecipeInstructions"));
            for (int i = 0; i < steps.size(); i++) {
                Map<String, Object> instructionRow = new HashMap<>();
                instructionRow.put("recipeid", recipeId);
                instructionRow.put("stepnumber", i + 1);
                instructionRow.put("instructiontext", steps.get(i));
                instructionsData.add(instructionRow);
            }

            List<String> keywords = DataReader.parseCsvList(row.get("Keywords"));
            allKeywords.addAll(keywords);

            List<String> ingredients = DataReader.parseCsvList(row.get("RecipeIngredientParts"));
            allIngredients.addAll(ingredients);

            List<Long> favoriteUserIds = DataReader.parseCsvIdList(row.get("FavoriteUsers"));
            if (favoriteUserIds.isEmpty()) {
                favoriteUserIds = DataReader.parseCsvIdList(row.get("FavoriteList"));
            }
            for (Long favAuthorId : favoriteUserIds) {
                if (favAuthorId != null) {
                    Map<String, Object> favRow = new HashMap<>();
                    favRow.put("authorid", favAuthorId);
                    favRow.put("recipeid", recipeId);
                    userFavoritesData.add(favRow);
                }
            }
        }
    }

    private void readAndPrepareReviews(String csvPath) throws Exception {
        List<Map<String, String>> csvData = DataReader.readCsv(csvPath);
        System.out.println("读取了 " + csvData.size() + " 条 review 记录");

        Set<Long> seenIds = new HashSet<>();
        for (Map<String, String> row : csvData) {
            Long reviewId = DataReader.parseLong(row.get("ReviewId"));
            Integer rating = DataReader.parseInteger(row.get("Rating"));

            if (reviewId == null || !seenIds.add(reviewId) || rating == null) {
                continue;
            }

            Map<String, Object> reviewRow = new HashMap<>();
            reviewRow.put("reviewid", reviewId);
            reviewRow.put("recipeid", DataReader.parseLong(row.get("RecipeId")));
            reviewRow.put("authorid", DataReader.parseLong(row.get("AuthorId")));
            reviewRow.put("rating", rating);
            reviewRow.put("review", DataReader.normalizeField(row.get("Review")));
            reviewRow.put("datesubmitted", DataReader.parseTimestamp(row.get("DateSubmitted")));
            reviewRow.put("datemodified", DataReader.parseTimestamp(row.get("DateModified")));
            reviewsData.add(reviewRow);

            List<Long> likedUserIds = DataReader.parseCsvIdList(row.get("Likes"));
            if (likedUserIds.isEmpty()) {
                likedUserIds = DataReader.parseCsvIdList(row.get("LikeList"));
            }
            for (Long likedAuthorId : likedUserIds) {
                if (likedAuthorId != null) {
                    Map<String, Object> likeRow = new HashMap<>();
                    likeRow.put("authorid", likedAuthorId);
                    likeRow.put("reviewid", reviewId);
                    userLikesData.add(likeRow);
                }
            }
        }
    }

    private void populateM2MTables() throws Exception {
        List<Map<String, Object>> keywordInsertData = new ArrayList<>();
        for (String keyword : allKeywords) {
            keywordInsertData.add(Map.of("keywordtext", keyword));
        }
        dataWriter.batchInsert("keywords", new String[]{"keywordtext"}, keywordInsertData);

        List<Map<String, Object>> ingredientInsertData = new ArrayList<>();
        for (String ingredient : allIngredients) {
            ingredientInsertData.add(Map.of("ingredientname", ingredient));
        }
        dataWriter.batchInsert("ingredients", new String[]{"ingredientname"}, ingredientInsertData);

        connectionManager.commit();

        try (PreparedStatement pstmt = conn.prepareStatement("SELECT keywordid, keywordtext FROM keywords")) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                keywordCache.put(rs.getString("keywordtext"), rs.getLong("keywordid"));
            }
        }
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT ingredientid, ingredientname FROM ingredients")) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                ingredientCache.put(rs.getString("ingredientname"), rs.getLong("ingredientid"));
            }
        }

        List<Map<String, String>> csvData = DataReader.readCsv(dataDirectory + "/recipes.csv");
        
        Set<String> recipeKeywordSet = new HashSet<>();
        Set<String> recipeIngredientSet = new HashSet<>();

        for (Map<String, String> row : csvData) {
            Long recipeId = DataReader.parseLong(row.get("RecipeId"));
            if (recipeId == null) continue;

            List<String> keywords = DataReader.parseCsvList(row.get("Keywords"));
            for (String keyword : keywords) {
                Long keywordId = keywordCache.get(keyword);
                if (keywordId != null) {
                    String key = recipeId + "," + keywordId;
                    if (recipeKeywordSet.add(key)) {
                        Map<String, Object> rkRow = new HashMap<>();
                        rkRow.put("recipeid", recipeId);
                        rkRow.put("keywordid", keywordId);
                        recipeKeywordsData.add(rkRow);
                    }
                }
            }

            List<String> ingredients = DataReader.parseCsvList(row.get("RecipeIngredientParts"));
            for (String ingredient : ingredients) {
                Long ingredientId = ingredientCache.get(ingredient);
                if (ingredientId != null) {
                    String key = recipeId + "," + ingredientId;
                    if (recipeIngredientSet.add(key)) {
                        Map<String, Object> riRow = new HashMap<>();
                        riRow.put("recipeid", recipeId);
                        riRow.put("ingredientid", ingredientId);
                        recipeIngredientsData.add(riRow);
                    }
                }
            }
        }
    }

    private void insertAllData() throws SQLException {
        insertWithConflict("users", new String[]{"authorid"}, new String[]{"authorid", "authorname", "gender", "age"}, usersData);
        insertWithConflict("recipes", new String[]{"recipeid"}, new String[]{"recipeid", "authorid", "name", "cooktime", "preptime", "datepublished", "description", "recipecategory", "recipeservings", "recipeyield"}, recipesData);
        insertWithConflict("reviews", new String[]{"reviewid"}, new String[]{"reviewid", "recipeid", "authorid", "rating", "review", "datesubmitted", "datemodified"}, reviewsData);

        String[] nutritionCols = {"recipeid", "calories", "fatcontent", "saturatedfatcontent", "cholesterolcontent", "sodiumcontent", "carbohydratecontent", "fibercontent", "sugarcontent", "proteincontent"};
        insertWithConflict("nutrition", new String[]{"recipeid"}, nutritionCols, nutritionData);

        insertWithConflict("instructions", new String[]{"recipeid", "stepnumber"}, new String[]{"recipeid", "stepnumber", "instructiontext"}, instructionsData);

        insertM2MTable("recipe_keywords", new String[]{"recipeid", "keywordid"}, recipeKeywordsData);
        insertM2MTable("recipe_ingredients", new String[]{"recipeid", "ingredientid"}, recipeIngredientsData);
        insertM2MTable("user_favorite_recipes", new String[]{"authorid", "recipeid"}, userFavoritesData);
        insertM2MTable("user_liked_reviews", new String[]{"authorid", "reviewid"}, userLikesData);
        insertM2MTable("user_follows", new String[]{"followerid", "followingid"}, userFollowsData);
    }
    
    /**
     * 插入主表，使用 ON CONFLICT 处理主键冲突
     */
    private void insertWithConflict(String tableName, String[] conflictColumns, String[] allColumns, List<Map<String, Object>> data) throws SQLException {
        if (data == null || data.isEmpty()) {
            return;
        }
        
        StringBuilder conflictClause = new StringBuilder("ON CONFLICT (");
        for (int i = 0; i < conflictColumns.length; i++) {
            if (i > 0) conflictClause.append(", ");
            conflictClause.append(conflictColumns[i]);
        }
        conflictClause.append(") DO NOTHING");
        
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append(tableName).append(" (");
        for (int i = 0; i < allColumns.length; i++) {
            if (i > 0) sqlBuilder.append(", ");
            sqlBuilder.append(allColumns[i]);
        }
        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < allColumns.length; i++) {
            if (i > 0) sqlBuilder.append(", ");
            sqlBuilder.append("?");
        }
        sqlBuilder.append(") ").append(conflictClause);
        
        String sql = sqlBuilder.toString();
        int totalInserted = 0;
        int batchCounter = 0;
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (Map<String, Object> row : data) {
                for (int i = 0; i < allColumns.length; i++) {
                    Object value = row.get(allColumns[i]);
                    setParameter(pstmt, i + 1, value);
                }
                
                pstmt.addBatch();
                batchCounter++;
                
                if (batchCounter % 1000 == 0) {
                    int[] results = pstmt.executeBatch();
                    totalInserted += countSuccess(results);
                    pstmt.clearBatch();
                }
            }
            
            if (batchCounter % 1000 != 0) {
                int[] results = pstmt.executeBatch();
                totalInserted += countSuccess(results);
            }
        }
        
        System.out.println("批量插入完成: " + totalInserted + " 条记录到表 " + tableName);
    }
    
    /**
     * 插入 M2M 关联表，使用 ON CONFLICT 处理重复键和外键约束
     */
    private void insertM2MTable(String tableName, String[] columns, List<Map<String, Object>> data) throws SQLException {
        if (data == null || data.isEmpty()) {
            return;
        }
        
（使用复合主键）
        StringBuilder conflictClause = new StringBuilder("ON CONFLICT (");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) conflictClause.append(", ");
            conflictClause.append(columns[i]);
        }
        conflictClause.append(") DO NOTHING");
        
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder.append(tableName).append(" (");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) sqlBuilder.append(", ");
            sqlBuilder.append(columns[i]);
        }
        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) sqlBuilder.append(", ");
            sqlBuilder.append("?");
        }
        sqlBuilder.append(") ").append(conflictClause);
        
        String sql = sqlBuilder.toString();
        int totalInserted = 0;
        int batchCounter = 0;
        
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (Map<String, Object> row : data) {
                for (int i = 0; i < columns.length; i++) {
                    Object value = row.get(columns[i]);
                    setParameter(pstmt, i + 1, value);
                }
                
                pstmt.addBatch();
                batchCounter++;
                
                if (batchCounter % 1000 == 0) {
                    try {
                        int[] results = pstmt.executeBatch();
                        totalInserted += countSuccess(results);
                    } catch (SQLException e) {
                        // 如果批量执行失败（可能是外键约束），逐条执行
                        pstmt.clearBatch();
                        // 重新执行当前批次，逐条插入
                        for (int j = batchCounter - 1000; j < batchCounter; j++) {
                            try {
                                Map<String, Object> singleRow = data.get(j);
                                for (int k = 0; k < columns.length; k++) {
                                    setParameter(pstmt, k + 1, singleRow.get(columns[k]));
                                }
                                pstmt.executeUpdate();
                                totalInserted++;
                            } catch (SQLException e2) {
                                String errorMsg = e2.getMessage();
                                if (errorMsg != null && 
                                    (errorMsg.contains("violates foreign key constraint") ||
                                     errorMsg.contains("violates unique constraint") ||
                                     errorMsg.contains("duplicate key"))) {
                                    continue;
                                }
                                // 其他错误重新抛出
                                throw e2;
                            }
                        }
                    }
                    pstmt.clearBatch();
                }
            }
            
            if (batchCounter % 1000 != 0) {
                try {
                    int[] results = pstmt.executeBatch();
                    totalInserted += countSuccess(results);
                } catch (SQLException e) {
                    // 如果批量执行失败，逐条执行剩余记录
                    pstmt.clearBatch();
                    int startIdx = (batchCounter / 1000) * 1000;
                    for (int j = startIdx; j < data.size(); j++) {
                        try {
                            Map<String, Object> singleRow = data.get(j);
                            for (int k = 0; k < columns.length; k++) {
                                setParameter(pstmt, k + 1, singleRow.get(columns[k]));
                            }
                            pstmt.executeUpdate();
                            totalInserted++;
                        } catch (SQLException e2) {
                            String errorMsg = e2.getMessage();
                            if (errorMsg != null && 
                                (errorMsg.contains("violates foreign key constraint") ||
                                 errorMsg.contains("violates unique constraint") ||
                                 errorMsg.contains("duplicate key"))) {
                                continue;
                            }
                            throw e2;
                        }
                    }
                }
            }
        }
        
        System.out.println("批量插入完成: " + totalInserted + " 条记录到表 " + tableName);
    }
    
    /**
     * 设置 PreparedStatement 参数
     */
    private void setParameter(PreparedStatement pstmt, int index, Object value) throws SQLException {
        if (value == null) {
            pstmt.setNull(index, java.sql.Types.NULL);
        } else if (value instanceof Integer) {
            pstmt.setInt(index, (Integer) value);
        } else if (value instanceof Long) {
            pstmt.setLong(index, (Long) value);
        } else if (value instanceof Double) {
            pstmt.setDouble(index, (Double) value);
        } else if (value instanceof Float) {
            pstmt.setFloat(index, (Float) value);
        } else if (value instanceof String) {
            pstmt.setString(index, (String) value);
        } else if (value instanceof Boolean) {
            pstmt.setBoolean(index, (Boolean) value);
        } else if (value instanceof java.sql.Timestamp) {
            pstmt.setTimestamp(index, (java.sql.Timestamp) value);
        } else if (value instanceof java.sql.Date) {
            pstmt.setDate(index, (java.sql.Date) value);
        } else {
            pstmt.setObject(index, value);
        }
    }

    private int countSuccess(int[] results) {
        int count = 0;
        for (int result : results) {
            if (result >= 0) {
                count++;
            }
        }
        return count;
    }


    private void printTableStatistics() throws SQLException {
        System.out.println("\n========== 各表记录统计 ==========");

        String[] tables = {
                "users", "recipes", "reviews", "nutrition", "instructions",
                "keywords", "recipe_keywords", "ingredients", "recipe_ingredients",
                "user_favorite_recipes", "user_liked_reviews", "user_follows"
        };

        for (String table : tables) {
            try {
                long count = dataQuery.count(table, null);
                System.out.println(String.format("%-25s : %d 条记录", table, count));
            } catch (SQLException e) {
                System.out.println(table + ": 表不存在或查询失败 - " + e.getMessage());
            }
        }
    }
}