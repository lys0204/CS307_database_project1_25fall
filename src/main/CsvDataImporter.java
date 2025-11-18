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

            String followerUsersField = row.get("FollowerUsers");
            if (followerUsersField == null || followerUsersField.trim().isEmpty()) {
                followerUsersField = row.get("FollowerSystemList");
            }
            if (followerUsersField == null || followerUsersField.trim().isEmpty()) {
                followerUsersField = row.get("FollowersList");
            }
            List<Long> followerIds = DataReader.parseCsvIdList(followerUsersField);
            for (Long followerId : followerIds) {
                if (followerId != null) {
                    Map<String, Object> followRow = new HashMap<>();
                    followRow.put("followerid", followerId);
                    followRow.put("followingid", authorId);
                    userFollowsData.add(followRow);
                }
            }
            
            String followingUsersField = row.get("FollowingUsers");
            if (followingUsersField == null || followingUsersField.trim().isEmpty()) {
                followingUsersField = row.get("FollowingSystemList");
            }
            if (followingUsersField == null || followingUsersField.trim().isEmpty()) {
                followingUsersField = row.get("FollowingList");
            }
            List<Long> followingIds = DataReader.parseCsvIdList(followingUsersField);
            for (Long followingId : followingIds) {
                if (followingId != null) {
                    Map<String, Object> followRow = new HashMap<>();
                    followRow.put("followerid", authorId);
                    followRow.put("followingid", followingId);
                    userFollowsData.add(followRow);
                }
            }
        }
    }

    private void readAndPrepareRecipes(String csvPath) throws Exception {
        List<Map<String, String>> csvData = DataReader.readCsv(csvPath);

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

            String favoriteUsersField = row.get("FavoriteUsers");
            if (favoriteUsersField == null || favoriteUsersField.trim().isEmpty()) {
                favoriteUsersField = row.get("FavoriteList");
            }
            if (favoriteUsersField == null || favoriteUsersField.trim().isEmpty()) {
                favoriteUsersField = row.get("FavoriteCount");
            }
            List<Long> favoriteUserIds = DataReader.parseCsvIdList(favoriteUsersField);
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

            String likesField = row.get("Likes");
            if (likesField == null || likesField.trim().isEmpty()) {
                likesField = row.get("LikeList");
            }
            if (likesField == null || likesField.trim().isEmpty()) {
                likesField = row.get("LikeCount");
            }
            List<Long> likedUserIds = DataReader.parseCsvIdList(likesField);
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
        String keywordSql = "INSERT INTO keywords (keywordtext) VALUES (?) ON CONFLICT (keywordtext) DO NOTHING";
        try (PreparedStatement pstmt = conn.prepareStatement(keywordSql)) {
            int count = 0;
            for (String keyword : allKeywords) {
                pstmt.setString(1, keyword);
                pstmt.addBatch();
                count++;
                if (count % 1000 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
        }

        String ingredientSql = "INSERT INTO ingredients (ingredientname) VALUES (?) ON CONFLICT (ingredientname) DO NOTHING";
        try (PreparedStatement pstmt = conn.prepareStatement(ingredientSql)) {
            int count = 0;
            for (String ingredient : allIngredients) {
                pstmt.setString(1, ingredient);
                pstmt.addBatch();
                count++;
                if (count % 1000 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
        }

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

        for (Map<String, String> row : csvData) {
            Long recipeId = DataReader.parseLong(row.get("RecipeId"));
            if (recipeId == null) continue;

            List<String> keywords = DataReader.parseCsvList(row.get("Keywords"));
            for (String keyword : keywords) {
                Long keywordId = keywordCache.get(keyword);
                if (keywordId != null) {
                    recipeKeywordsData.add(Map.of("recipeid", recipeId, "keywordid", keywordId));
                }
            }

            List<String> ingredients = DataReader.parseCsvList(row.get("RecipeIngredientParts"));
            for (String ingredient : ingredients) {
                Long ingredientId = ingredientCache.get(ingredient);
                if (ingredientId != null) {
                    recipeIngredientsData.add(Map.of("recipeid", recipeId, "ingredientid", ingredientId));
                }
            }
        }
    }

    private void insertAllData() throws SQLException {
        insertWithConflict("users", new String[]{"authorid", "authorname", "gender", "age"}, usersData, "authorid");
        insertWithConflict("recipes", new String[]{"recipeid", "authorid", "name", "cooktime", "preptime", "datepublished", "description", "recipecategory", "recipeservings", "recipeyield"}, recipesData, "recipeid");
        insertWithConflict("reviews", new String[]{"reviewid", "recipeid", "authorid", "rating", "review", "datesubmitted", "datemodified"}, reviewsData, "reviewid");

        String[] nutritionCols = {"recipeid", "calories", "fatcontent", "saturatedfatcontent", "cholesterolcontent", "sodiumcontent", "carbohydratecontent", "fibercontent", "sugarcontent", "proteincontent"};
        insertWithConflict("nutrition", nutritionCols, nutritionData, "recipeid");
        insertWithConflict("instructions", new String[]{"recipeid", "stepnumber", "instructiontext"}, instructionsData, "recipeid, stepnumber");

        insertM2MTable("recipe_keywords", recipeKeywordsData, "recipeid", "keywordid");
        insertM2MTable("recipe_ingredients", recipeIngredientsData, "recipeid", "ingredientid");
        insertM2MTable("user_favorite_recipes", userFavoritesData, "authorid", "recipeid");
        insertM2MTable("user_liked_reviews", userLikesData, "authorid", "reviewid");
        insertM2MTable("user_follows", userFollowsData, "followerid", "followingid");
    }

    private void insertWithConflict(String tableName, String[] columns, List<Map<String, Object>> data, String conflictKey) throws SQLException {
        if (data == null || data.isEmpty()) {
            return;
        }
        
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
        sqlBuilder.append(") ON CONFLICT (").append(conflictKey).append(") DO NOTHING");
        
        String sql = sqlBuilder.toString();
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int count = 0;
            for (Map<String, Object> row : data) {
                for (int i = 0; i < columns.length; i++) {
                    Object value = row.get(columns[i]);
                    setParameter(pstmt, i + 1, value);
                }
                pstmt.addBatch();
                count++;
                if (count % 1000 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
        }
    }
    
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

    private void insertM2MTable(String tableName, List<Map<String, Object>> data, String col1, String col2) throws SQLException {
        if (data == null || data.isEmpty()) {
            return;
        }
        String sql = String.format("INSERT INTO %s (%s, %s) VALUES (?, ?) ON CONFLICT (%s, %s) DO NOTHING",
                tableName, col1, col2, col1, col2);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            int count = 0;
            for (Map<String, Object> row : data) {
                Object val1 = row.get(col1);
                Object val2 = row.get(col2);
                if (val1 != null && val2 != null) {
                    if (val1 instanceof Long) {
                        pstmt.setLong(1, (Long) val1);
                    } else {
                        pstmt.setObject(1, val1);
                    }
                    if (val2 instanceof Long) {
                        pstmt.setLong(2, (Long) val2);
                    } else {
                        pstmt.setObject(2, val2);
                    }
                    pstmt.addBatch();
                    count++;
                    if (count % 1000 == 0) {
                        pstmt.executeBatch();
                    }
                }
            }
            pstmt.executeBatch();
        }
    }

    private void printTableStatistics() throws SQLException {
        String[] tables = {
                "users", "recipes", "reviews", "nutrition", "instructions",
                "keywords", "recipe_keywords", "ingredients", "recipe_ingredients",
                "user_favorite_recipes", "user_liked_reviews", "user_follows"
        };

        for (String table : tables) {
            try {
                long count = dataQuery.count(table, null);
                System.out.println(table + ": " + count);
            } catch (SQLException e) {
            }
        }
    }
}