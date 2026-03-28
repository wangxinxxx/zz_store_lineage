package com.zhuanzhuan.lineage.model;

import org.apache.spark.sql.catalyst.TableIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class TableRef {
    private final String catalog;
    private final String database;
    private final String name;
    private final String sourceType;

    public TableRef(String catalog, String database, String name, String sourceType) {
        this.catalog = blankToNull(catalog);
        this.database = blankToNull(database);
        this.name = blankToNull(name);
        this.sourceType = blankToNull(sourceType) == null ? "table" : blankToNull(sourceType);
    }

    public static TableRef fromParts(List<String> parts) {
        return fromParts(parts, "table");
    }

    public static TableRef fromParts(List<String> parts, String sourceType) {
        List<String> cleaned = parts.stream()
                .map(TableRef::blankToNull)
                .filter(value -> value != null)
                .collect(Collectors.toList());

        if (cleaned.isEmpty()) {
            return new TableRef(null, null, "unknown", sourceType);
        }
        if (cleaned.size() == 1) {
            return new TableRef(null, null, cleaned.get(0), sourceType);
        }
        if (cleaned.size() == 2) {
            return new TableRef(null, cleaned.get(0), cleaned.get(1), sourceType);
        }

        String name = cleaned.get(cleaned.size() - 1);
        String database = cleaned.get(cleaned.size() - 2);
        String catalog = String.join(".", cleaned.subList(0, cleaned.size() - 2));
        return new TableRef(catalog, database, name, sourceType);
    }

    public static TableRef fromTableIdentifier(TableIdentifier identifier) {
        return new TableRef(null, identifier.database().isDefined() ? identifier.database().get() : null, identifier.table(), "table");
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String normalizedName() {
        List<String> parts = new ArrayList<>();
        if (catalog != null) {
            parts.add(catalog);
        }
        if (database != null) {
            parts.add(database);
        }
        if (name != null) {
            parts.add(name);
        }
        return String.join(".", parts);
    }

    private static String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
