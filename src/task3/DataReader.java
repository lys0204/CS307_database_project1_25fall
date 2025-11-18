package task3;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据读取/解析工具类
 * 支持从 CSV 和 JSON 文件读取数据（简化版 JSON 解析）
 */
public class DataReader {
    /**
     * 从 CSV 文件读取数据
     * @param filePath CSV 文件路径
     * @return 数据列表，每行是一个 Map，key 为列名，value 为字段值
     */
    public static List<Map<String, String>> readCsv(String filePath) throws IOException {
        List<Map<String, String>> data = new ArrayList<>();
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            throw new IOException("文件不存在: " + filePath);
        }

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            // 读取表头
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return data;
            }

            String[] headers = parseCsvLine(headerLine);

            // 读取数据行
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = parseCsvLine(line);
                Map<String, String> row = new HashMap<>();

                // 将数据映射到表头
                for (int i = 0; i < headers.length && i < values.length; i++) {
                    row.put(headers[i].trim(), values[i].trim());
                }

                data.add(row);
            }
        }

        return data;
    }

    /**
     * 解析 CSV 行（简单实现，处理引号和逗号）
     */
    private static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // 转义的引号
                    currentField.append('"');
                    i++;
                } else {
                    // 切换引号状态
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                // 字段分隔符
                fields.add(currentField.toString());
                currentField.setLength(0);
            } else {
                currentField.append(ch);
            }
        }

        // 添加最后一个字段
        fields.add(currentField.toString());

        return fields.toArray(new String[0]);
    }

    /**
     * 从 JSON 文件读取数据（简化版，仅支持简单的 JSON 对象和数组）
     * @param filePath JSON 文件路径
     * @return 数据列表，每个元素是一个 Map
     */
    public static List<Map<String, Object>> readJson(String filePath) throws IOException {
        List<Map<String, Object>> data = new ArrayList<>();
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            throw new IOException("文件不存在: " + filePath);
        }

        String content = new String(Files.readAllBytes(path));
        content = content.trim();

        // 简化版 JSON 解析：仅支持简单的对象数组格式
        // 格式示例: [{"key1":"value1","key2":"value2"}, {"key1":"value3","key2":"value4"}]
        if (content.startsWith("[") && content.endsWith("]")) {
            // 移除方括号
            content = content.substring(1, content.length() - 1).trim();

            // 简单分割对象（假设对象之间用 },{ 分隔）
            String[] objects = content.split("\\},\\s*\\{");
            for (int i = 0; i < objects.length; i++) {
                String objStr = objects[i];
                if (i == 0) {
                    objStr = objStr + "}";
                } else if (i == objects.length - 1) {
                    objStr = "{" + objStr;
                } else {
                    objStr = "{" + objStr + "}";
                }

                Map<String, Object> map = parseSimpleJsonObject(objStr);
                if (!map.isEmpty()) {
                    data.add(map);
                }
            }
        } else if (content.startsWith("{") && content.endsWith("}")) {
            // 单个 JSON 对象
            Map<String, Object> map = parseSimpleJsonObject(content);
            if (!map.isEmpty()) {
                data.add(map);
            }
        }

        return data;
    }

    /**
     * 解析简单的 JSON 对象（仅支持字符串值）
     * 格式: {"key1":"value1","key2":"value2"}
     */
    private static Map<String, Object> parseSimpleJsonObject(String jsonStr) {
        Map<String, Object> map = new HashMap<>();
        jsonStr = jsonStr.trim();

        if (!jsonStr.startsWith("{") || !jsonStr.endsWith("}")) {
            return map;
        }

        // 移除花括号
        jsonStr = jsonStr.substring(1, jsonStr.length() - 1).trim();

        // 简单解析键值对
        String[] pairs = jsonStr.split(",");
        for (String pair : pairs) {
            pair = pair.trim();
            int colonIndex = pair.indexOf(':');
            if (colonIndex > 0) {
                String key = pair.substring(0, colonIndex).trim();
                String value = pair.substring(colonIndex + 1).trim();

                // 移除引号
                if (key.startsWith("\"") && key.endsWith("\"")) {
                    key = key.substring(1, key.length() - 1);
                }
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }

                map.put(key, value);
            }
        }

        return map;
    }

    /**
     * 处理字段格式：清理空白字符，处理 null 值
     */
    public static String normalizeField(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() || trimmed.equalsIgnoreCase("null") ? null : trimmed;
    }

    /**
     * 解析整数
     */
    public static Integer parseInteger(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 解析长整数
     */
    public static Long parseLong(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 解析浮点数
     */
    public static Double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
