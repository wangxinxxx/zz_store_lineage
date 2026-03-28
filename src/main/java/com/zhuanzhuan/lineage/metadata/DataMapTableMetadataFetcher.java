package com.zhuanzhuan.lineage.metadata;

import com.zhuanzhuan.lineage.model.TableRef;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DataMapTableMetadataFetcher {
    private static final Pattern COMMENT_PATTERN = Pattern.compile("(?is)\\bcomment\\b\\s+'((?:[^'\\\\]|\\\\.|'')*)'");
    private static final Pattern DATE_SUFFIX_PATTERN = Pattern.compile("^(.*?)(_20\\d{6,12})$");

    private final DataMapMetadataClient client;
    private final String sourceEndpoint;

    public DataMapTableMetadataFetcher(DataMapMetadataClient client, String sourceEndpoint) {
        this.client = client;
        this.sourceEndpoint = sourceEndpoint;
    }

    public TableMetadataSnapshot fetch(TableRef tableRef) throws IOException {
        String ddl = resolveCreateSql(tableRef);
        TableMetadataSnapshot snapshot = parseCreateSql(tableRef, ddl);
        return new TableMetadataSnapshot(
                snapshot.getCatalog(),
                snapshot.getDatabase(),
                snapshot.getTableName(),
                snapshot.getOwner(),
                snapshot.getLocation(),
                snapshot.getSourceType(),
                sourceEndpoint,
                snapshot.getFetchedAtEpochMs(),
                snapshot.getColumns(),
                snapshot.getPartitionKeys()
        );
    }

    private String resolveCreateSql(TableRef tableRef) throws IOException {
        IOException lastError = null;
        for (String candidate : candidateTableNames(tableRef)) {
            try {
                long tableId = client.findTableId(candidate);
                return client.fetchCreateSql(tableId);
            } catch (IOException error) {
                lastError = error;
            }
        }
        throw new IOException("Failed to fetch DataMap create_sql_hive for " + tableRef.normalizedName(), lastError);
    }

    private List<String> candidateTableNames(TableRef tableRef) {
        Set<String> candidates = new LinkedHashSet<String>();
        String fullName = tableRef.normalizedName();
        if (fullName != null && !fullName.trim().isEmpty()) {
            candidates.add(fullName);
        }

        for (String normalizedName : normalizeTableNames(fullName)) {
            if (normalizedName != null && !normalizedName.equals(fullName)) {
                candidates.add(normalizedName);
            }
        }
        return new ArrayList<String>(candidates);
    }

    private List<String> normalizeTableNames(String fullName) {
        if (fullName == null || fullName.trim().isEmpty()) {
            return Collections.emptyList();
        }
        int lastDot = fullName.lastIndexOf('.');
        String prefix = lastDot < 0 ? "" : fullName.substring(0, lastDot + 1);
        String tableName = lastDot < 0 ? fullName : fullName.substring(lastDot + 1);
        Set<String> candidates = new LinkedHashSet<String>();
        Matcher matcher = DATE_SUFFIX_PATTERN.matcher(tableName);
        if (matcher.matches()) {
            String base = matcher.group(1);
            candidates.add(prefix + base);
            if (!base.endsWith("_")) {
                candidates.add(prefix + base + "_");
            }
        }
        return new ArrayList<String>(candidates);
    }

    static TableMetadataSnapshot parseCreateSql(TableRef tableRef, String ddl) throws IOException {
        if (ddl == null || ddl.trim().isEmpty()) {
            throw new IOException("create_sql_hive returned empty DDL for " + tableRef.normalizedName());
        }

        List<FieldSchema> columns = parseFieldBlock(extractCreateColumnsBlock(ddl));
        List<FieldSchema> partitionKeys = parseFieldBlock(extractPartitionBlock(ddl));
        String location = extractSingleQuotedValue(ddl, "location");
        String sourceType = ddl.toLowerCase(Locale.ROOT).contains("create external table") ? "EXTERNAL_TABLE" : "MANAGED_TABLE";
        if (columns.isEmpty()) {
            throw new IOException("create_sql_hive did not expose columns for " + tableRef.normalizedName());
        }

        return new TableMetadataSnapshot(
                tableRef.getCatalog(),
                tableRef.getDatabase(),
                tableRef.getName(),
                null,
                location,
                sourceType,
                null,
                System.currentTimeMillis(),
                deduplicate(columns),
                deduplicate(partitionKeys)
        );
    }

    private static String extractCreateColumnsBlock(String ddl) {
        int createIndex = ddl.toLowerCase(Locale.ROOT).indexOf("create");
        if (createIndex < 0) {
            return null;
        }
        int firstOpen = ddl.indexOf('(', createIndex);
        if (firstOpen < 0) {
            return null;
        }
        return extractBalancedBlock(ddl, firstOpen);
    }

    private static String extractPartitionBlock(String ddl) {
        String lower = ddl.toLowerCase(Locale.ROOT);
        int partitionIndex = lower.indexOf("partitioned by");
        if (partitionIndex < 0) {
            return null;
        }
        int open = ddl.indexOf('(', partitionIndex);
        if (open < 0) {
            return null;
        }
        return extractBalancedBlock(ddl, open);
    }

    private static String extractBalancedBlock(String text, int openIndex) {
        int depth = 0;
        boolean singleQuote = false;
        boolean doubleQuote = false;
        boolean backtickQuote = false;
        int start = openIndex + 1;
        for (int i = openIndex; i < text.length(); i++) {
            char current = text.charAt(i);
            if (current == '\'' && !doubleQuote && !backtickQuote) {
                singleQuote = !singleQuote;
            } else if (current == '"' && !singleQuote && !backtickQuote) {
                doubleQuote = !doubleQuote;
            } else if (current == '`' && !singleQuote && !doubleQuote) {
                backtickQuote = !backtickQuote;
            }
            if (singleQuote || doubleQuote || backtickQuote) {
                continue;
            }
            if (current == '(') {
                depth++;
            } else if (current == ')') {
                depth--;
                if (depth == 0) {
                    return text.substring(start, i);
                }
            }
        }
        return null;
    }

    private static List<FieldSchema> parseFieldBlock(String block) {
        if (block == null || block.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<FieldSchema> fields = new ArrayList<FieldSchema>();
        for (String expression : splitTopLevel(block)) {
            FieldSchema field = parseFieldExpression(expression);
            if (field != null) {
                fields.add(field);
            }
        }
        return fields;
    }

    private static FieldSchema parseFieldExpression(String expression) {
        if (expression == null) {
            return null;
        }
        String trimmed = expression.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        if (trimmed.toLowerCase(Locale.ROOT).startsWith("primary key")
                || trimmed.toLowerCase(Locale.ROOT).startsWith("constraint")
                || trimmed.toLowerCase(Locale.ROOT).startsWith("index")) {
            return null;
        }

        int index = 0;
        while (index < trimmed.length() && Character.isWhitespace(trimmed.charAt(index))) {
            index++;
        }
        if (index >= trimmed.length()) {
            return null;
        }

        String name;
        if (trimmed.charAt(index) == '`') {
            int end = trimmed.indexOf('`', index + 1);
            if (end < 0) {
                return null;
            }
            name = trimmed.substring(index + 1, end);
            index = end + 1;
        } else {
            int end = index;
            while (end < trimmed.length() && !Character.isWhitespace(trimmed.charAt(end))) {
                end++;
            }
            name = trimmed.substring(index, end);
            index = end;
        }

        while (index < trimmed.length() && Character.isWhitespace(trimmed.charAt(index))) {
            index++;
        }
        if (name.trim().isEmpty() || index >= trimmed.length()) {
            return null;
        }

        int typeEnd = findTypeEnd(trimmed, index);
        String type = trimmed.substring(index, typeEnd).trim();
        if (type.isEmpty()) {
            return null;
        }
        return new FieldSchema(name.trim(), type, extractComment(trimmed.substring(typeEnd)));
    }

    private static int findTypeEnd(String expression, int start) {
        int depth = 0;
        boolean singleQuote = false;
        boolean doubleQuote = false;
        boolean backtickQuote = false;
        for (int i = start; i < expression.length(); i++) {
            char current = expression.charAt(i);
            if (current == '\'' && !doubleQuote && !backtickQuote) {
                singleQuote = !singleQuote;
            } else if (current == '"' && !singleQuote && !backtickQuote) {
                doubleQuote = !doubleQuote;
            } else if (current == '`' && !singleQuote && !doubleQuote) {
                backtickQuote = !backtickQuote;
            } else if (!singleQuote && !doubleQuote && !backtickQuote) {
                if (current == '(') {
                    depth++;
                } else if (current == ')' && depth > 0) {
                    depth--;
                } else if (Character.isWhitespace(current) && depth == 0) {
                    String remainder = expression.substring(i).trim().toLowerCase(Locale.ROOT);
                    if (remainder.startsWith("comment ")
                            || remainder.startsWith("constraint ")
                            || remainder.startsWith("not null")
                            || remainder.startsWith("default ")
                            || remainder.startsWith("encode ")
                            || remainder.startsWith("generated ")
                            || remainder.startsWith("primary key")) {
                        return i;
                    }
                }
            }
        }
        return expression.length();
    }

    private static List<String> splitTopLevel(String block) {
        List<String> expressions = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean singleQuote = false;
        boolean doubleQuote = false;
        boolean backtickQuote = false;
        for (int i = 0; i < block.length(); i++) {
            char currentChar = block.charAt(i);
            if (currentChar == '\'' && !doubleQuote && !backtickQuote) {
                singleQuote = !singleQuote;
            } else if (currentChar == '"' && !singleQuote && !backtickQuote) {
                doubleQuote = !doubleQuote;
            } else if (currentChar == '`' && !singleQuote && !doubleQuote) {
                backtickQuote = !backtickQuote;
            } else if (!singleQuote && !doubleQuote && !backtickQuote) {
                if (currentChar == '(') {
                    depth++;
                } else if (currentChar == ')' && depth > 0) {
                    depth--;
                } else if (currentChar == ',' && depth == 0) {
                    expressions.add(current.toString());
                    current.setLength(0);
                    continue;
                }
            }
            current.append(currentChar);
        }
        if (current.length() > 0) {
            expressions.add(current.toString());
        }
        return expressions;
    }

    private static String extractSingleQuotedValue(String ddl, String keyword) {
        String lower = ddl.toLowerCase(Locale.ROOT);
        int keywordIndex = lower.indexOf(keyword.toLowerCase(Locale.ROOT));
        if (keywordIndex < 0) {
            return null;
        }
        int firstQuote = ddl.indexOf('\'', keywordIndex);
        if (firstQuote < 0) {
            return null;
        }
        int secondQuote = ddl.indexOf('\'', firstQuote + 1);
        if (secondQuote < 0) {
            return null;
        }
        return ddl.substring(firstQuote + 1, secondQuote);
    }

    private static String extractComment(String tail) {
        if (tail == null || tail.trim().isEmpty()) {
            return null;
        }
        Matcher matcher = COMMENT_PATTERN.matcher(tail);
        if (!matcher.find()) {
            return null;
        }
        String raw = matcher.group(1);
        if (raw == null || raw.isEmpty()) {
            return null;
        }
        return raw.replace("\\'", "'").replace("''", "'").replace("\\\\", "\\");
    }

    private static List<FieldSchema> deduplicate(List<FieldSchema> fields) {
        if (fields == null || fields.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, FieldSchema> deduped = new LinkedHashMap<String, FieldSchema>();
        for (FieldSchema field : fields) {
            if (field == null || field.getName() == null || field.getName().trim().isEmpty()) {
                continue;
            }
            deduped.put(field.getName(), field);
        }
        return new ArrayList<FieldSchema>(deduped.values());
    }
}
