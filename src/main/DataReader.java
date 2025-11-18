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
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return data;
            }

            String[] headers = parseCsvLine(headerLine);

            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                try {
                    String[] values = parseCsvLine(line);
                    Map<String, String> row = new HashMap<>();

                    for (int i = 0; i < headers.length && i < values.length; i++) {
                        row.put(headers[i].trim(), values[i].trim());
                    }

                    data.add(row);
                } catch (Exception e) {
                    System.err.println("解析第 " + lineNumber + " 行时出错: " + e.getMessage());
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
                    currentField.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                fields.add(currentField.toString());
                currentField.setLength(0);
            } else {
                currentField.append(ch);
            }
        }

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
            Instant instant = Instant.parse(value);
            return Timestamp.from(instant);
        } catch (DateTimeParseException e) {
            try {
                OffsetDateTime odt = OffsetDateTime.parse(value);
                return Timestamp.from(odt.toInstant());
            } catch (DateTimeParseException e2) {
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
        
        if (idListString.startsWith("\"\"\"") && idListString.endsWith("\"\"\"")) {
            String trimmed = idListString.substring(3, idListString.length() - 3);
            String[] idStrings = trimmed.split(",");
            for (String idStr : idStrings) {
                Long id = parseLong(idStr.trim());
                if (id != null) {
                    ids.add(id);
                }
            }
        } else if (idListString.contains(",")) {
            String[] idStrings = idListString.split(",");
            for (String idStr : idStrings) {
                Long id = parseLong(idStr.trim());
                if (id != null) {
                    ids.add(id);
                }
            }
        } else {
            Long id = parseLong(idListString.trim());
            if (id != null) {
                ids.add(id);
            }
        }
        return ids;
    }
}