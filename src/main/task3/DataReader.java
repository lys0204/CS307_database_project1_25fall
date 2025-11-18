package main;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class DataReader {

    private static final Pattern CSV_LIST_PATTERN = Pattern.compile("\"(.*?)\"");


    public static List<Map<String, String>> readCsv(String filePath) throws IOException {
        List<Map<String, String>> data = new ArrayList<>();
        Path path = Paths.get(filePath);

        if (!Files.exists(path)) {
            throw new IOException("文件不存在: " + filePath);
        }

        try (BufferedReader reader = Files.newBufferedReader(path, java.nio.charset.StandardCharsets.UTF_8)) {
            // 读取表头
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return data;
            }

            String[] headers = parseCsvLine(headerLine);

            // 读取数据行
            String line;
            int lineNumber = 1; // 用于错误报告
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                try {
                    String[] values = parseCsvLine(line);
                    Map<String, String> row = new HashMap<>();

                    // 将数据映射到表头
                    for (int i = 0; i < headers.length && i < values.length; i++) {
                        row.put(headers[i].trim(), values[i].trim());
                    }

                    data.add(row);
                } catch (Exception e) {
                    System.err.println("解析第 " + lineNumber + " 行时出错: " + e.getMessage());
                    // 继续处理下一行，而不是中断整个导入过程
                }
            }
        }

        return data;
    }

    private static String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // 转义的引号 ""
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

    public static String normalizeField(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() || trimmed.equalsIgnoreCase("null") ? null : trimmed;
    }


    public static Integer parseInteger(String value) {
        value = normalizeField(value);
        if (value == null) return null;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public static Long parseLong(String value) {
        value = normalizeField(value);
        if (value == null) return null;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }


    public static Double parseDouble(String value) {
        value = normalizeField(value);
        if (value == null) return null;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }


    public static Timestamp parseTimestamp(String value) {
        value = normalizeField(value);
        if (value == null) return null;
        try {
            // "Z" 结尾表示 UTC 时间，Instant.parse 可以直接处理
            Instant instant = Instant.parse(value);
            return Timestamp.from(instant);
        } catch (DateTimeParseException e) {
            // 尝试使用 OffsetDateTime 作为备用，以防万一
            try {
                OffsetDateTime odt = OffsetDateTime.parse(value);
                return Timestamp.from(odt.toInstant());
            } catch (DateTimeParseException e2) {
                // System.err.println("无法解析时间戳: " + value + " - " + e2.getMessage());
                return null;
            }
        }
    }

    public static List<String> parseCsvList(String listString) {
        List<String> items = new ArrayList<>();
        listString = normalizeField(listString);
        if (listString == null || !listString.startsWith("c(") || !listString.endsWith(")")) {
            return items;
        }

        Matcher matcher = CSV_LIST_PATTERN.matcher(listString);
        while (matcher.find()) {
            String item = matcher.group(1);
            // 清理CSV内部转义的引号 ""
            item = item.replace("\"\"", "\"");
            if (!item.isEmpty()) {
                items.add(item);
            }
        }
        return items;
    }

    public static List<Long> parseCsvIdList(String idListString) {
        List<Long> ids = new ArrayList<>();
        idListString = normalizeField(idListString);
        if (idListString == null || idListString.isEmpty()) {
            return ids;
        }

        String trimmed = idListString;
        
        // 处理 """id1, id2, id3""" 格式
        if (trimmed.startsWith("\"\"\"") && trimmed.endsWith("\"\"\"")) {
            trimmed = trimmed.substring(3, trimmed.length() - 3);
        }
        // 处理 "id1, id2, id3" 格式
        else if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        
        // 分割并解析
        String[] idStrings = trimmed.split(",");
        for (String idStr : idStrings) {
            idStr = idStr.trim();
            if (!idStr.isEmpty()) {
                Long id = parseLong(idStr);
                if (id != null) {
                    ids.add(id);
                }
            }
        }
        return ids;
    }
}