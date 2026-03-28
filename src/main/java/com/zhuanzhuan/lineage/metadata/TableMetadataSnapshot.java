package com.zhuanzhuan.lineage.metadata;

import com.zhuanzhuan.lineage.model.TableRef;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class TableMetadataSnapshot {
    private final String catalog;
    private final String database;
    private final String tableName;
    private final String owner;
    private final String location;
    private final String sourceType;
    private final String metastoreUri;
    private final long fetchedAtEpochMs;
    private final List<FieldSchema> columns;
    private final List<FieldSchema> partitionKeys;

    public TableMetadataSnapshot(
            String catalog,
            String database,
            String tableName,
            String owner,
            String location,
            String sourceType,
            String metastoreUri,
            long fetchedAtEpochMs,
            List<FieldSchema> columns,
            List<FieldSchema> partitionKeys
    ) {
        this.catalog = blankToNull(catalog);
        this.database = blankToNull(database);
        this.tableName = blankToNull(tableName);
        this.owner = blankToNull(owner);
        this.location = blankToNull(location);
        this.sourceType = blankToNull(sourceType) == null ? "table" : blankToNull(sourceType);
        this.metastoreUri = blankToNull(metastoreUri);
        this.fetchedAtEpochMs = fetchedAtEpochMs;
        this.columns = copy(columns);
        this.partitionKeys = copy(partitionKeys);
    }

    public String getCatalog() {
        return catalog;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }

    public String getOwner() {
        return owner;
    }

    public String getLocation() {
        return location;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getMetastoreUri() {
        return metastoreUri;
    }

    public long getFetchedAtEpochMs() {
        return fetchedAtEpochMs;
    }

    public List<FieldSchema> getColumns() {
        return columns;
    }

    public List<FieldSchema> getPartitionKeys() {
        return partitionKeys;
    }

    public TableRef toTableRef() {
        List<String> parts = new ArrayList<String>();
        if (catalog != null) {
            parts.add(catalog);
        }
        if (database != null) {
            parts.add(database);
        }
        if (tableName != null) {
            parts.add(tableName);
        }
        return TableRef.fromParts(parts, sourceType);
    }

    private static List<FieldSchema> copy(List<FieldSchema> fields) {
        if (fields == null || fields.isEmpty()) {
            return Collections.emptyList();
        }
        List<FieldSchema> copy = new ArrayList<FieldSchema>(fields.size());
        for (FieldSchema field : fields) {
            if (field == null) {
                continue;
            }
            copy.add(new FieldSchema(field.getName(), field.getType(), field.getComment()));
        }
        return Collections.unmodifiableList(copy);
    }

    private static String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
