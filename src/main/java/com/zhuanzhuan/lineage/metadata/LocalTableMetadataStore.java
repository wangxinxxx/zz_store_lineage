package com.zhuanzhuan.lineage.metadata;

import com.zhuanzhuan.lineage.model.TableRef;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

public final class LocalTableMetadataStore {
    private static final String VERSION = "2";

    private final Path rootDir;

    public LocalTableMetadataStore(Path rootDir) {
        this.rootDir = rootDir;
    }

    public Path getRootDir() {
        return rootDir;
    }

    public boolean exists(TableRef tableRef) {
        return Files.exists(snapshotPath(tableRef));
    }

    public boolean needsReadableRewrite(TableRef tableRef) {
        Path path = snapshotPath(tableRef);
        if (!Files.exists(path)) {
            return false;
        }
        try {
            String raw = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
            return raw.contains("\\u");
        } catch (IOException ignored) {
            return false;
        }
    }

    public Optional<TableMetadataSnapshot> load(TableRef tableRef) throws IOException {
        Path path = snapshotPath(tableRef);
        if (!Files.exists(path)) {
            return Optional.empty();
        }

        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }

        List<FieldSchema> columns = loadFields(properties, "column");
        List<FieldSchema> partitionKeys = loadFields(properties, "partition");
        long fetchedAtEpochMs = 0L;
        String fetchedAtValue = properties.getProperty("fetched_at_epoch_ms");
        if (fetchedAtValue != null && !fetchedAtValue.trim().isEmpty()) {
            try {
                fetchedAtEpochMs = Long.parseLong(fetchedAtValue.trim());
            } catch (NumberFormatException ignored) {
            }
        }

        return Optional.of(new TableMetadataSnapshot(
                properties.getProperty("catalog"),
                properties.getProperty("database"),
                properties.getProperty("table_name"),
                properties.getProperty("owner"),
                properties.getProperty("location"),
                properties.getProperty("source_type"),
                properties.getProperty("metastore_uri"),
                fetchedAtEpochMs,
                columns,
                partitionKeys
        ));
    }

    public void save(TableMetadataSnapshot snapshot) throws IOException {
        Files.createDirectories(rootDir);
        Path path = snapshotPath(snapshot.toTableRef());
        Files.createDirectories(path.getParent());

        Properties properties = new Properties();
        properties.setProperty("version", VERSION);
        write(properties, "catalog", snapshot.getCatalog());
        write(properties, "database", snapshot.getDatabase());
        write(properties, "table_name", snapshot.getTableName());
        write(properties, "owner", snapshot.getOwner());
        write(properties, "location", snapshot.getLocation());
        write(properties, "source_type", snapshot.getSourceType());
        write(properties, "metastore_uri", snapshot.getMetastoreUri());
        properties.setProperty("fetched_at_epoch_ms", String.valueOf(snapshot.getFetchedAtEpochMs()));
        writeFields(properties, "column", snapshot.getColumns());
        writeFields(properties, "partition", snapshot.getPartitionKeys());

        Path tempFile = Files.createTempFile(rootDir, "table-metadata-", ".properties");
        try (Writer writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
            properties.store(writer, "table metadata snapshot");
        }
        Files.move(tempFile, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public Path saveBatchTableList(String batchName, List<TableRef> tableRefs) throws IOException {
        return writeBatchList(batchName, "tables", tableRefs == null
                ? Collections.<String>emptyList()
                : tableRefs.stream().map(TableRef::normalizedName).collect(Collectors.toList()));
    }

    public Path saveBatchFailures(String batchName, List<String> failures) throws IOException {
        return writeBatchList(batchName, "failures", failures == null ? Collections.<String>emptyList() : failures);
    }

    private Path snapshotPath(TableRef tableRef) {
        String database = tableRef.getDatabase() == null || tableRef.getDatabase().trim().isEmpty()
                ? "default"
                : tableRef.getDatabase();
        String fileName = sanitize(tableRef.getName()) + ".properties";
        Path directory = rootDir.resolve(sanitize(database));
        if (tableRef.getCatalog() != null && !tableRef.getCatalog().trim().isEmpty()) {
            directory = directory.resolve(sanitize(tableRef.getCatalog()));
        }
        return directory.resolve(fileName);
    }

    private void write(Properties properties, String key, String value) {
        if (value != null) {
            properties.setProperty(key, value);
        }
    }

    private void writeFields(Properties properties, String prefix, List<FieldSchema> fields) {
        int count = fields == null ? 0 : fields.size();
        properties.setProperty(prefix + ".count", String.valueOf(count));
        if (fields == null) {
            return;
        }
        for (int i = 0; i < fields.size(); i++) {
            FieldSchema field = fields.get(i);
            if (field == null) {
                continue;
            }
            write(properties, prefix + "." + i + ".name", field.getName());
            write(properties, prefix + "." + i + ".type", field.getType());
            write(properties, prefix + "." + i + ".comment", field.getComment());
        }
    }

    private List<FieldSchema> loadFields(Properties properties, String prefix) {
        int count = 0;
        String countValue = properties.getProperty(prefix + ".count");
        if (countValue != null && !countValue.trim().isEmpty()) {
            try {
                count = Integer.parseInt(countValue.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        List<FieldSchema> fields = new ArrayList<FieldSchema>(count);
        for (int i = 0; i < count; i++) {
            String name = properties.getProperty(prefix + "." + i + ".name");
            if (name == null || name.trim().isEmpty()) {
                continue;
            }
            fields.add(new FieldSchema(
                    name,
                    properties.getProperty(prefix + "." + i + ".type"),
                    properties.getProperty(prefix + "." + i + ".comment")
            ));
        }
        return fields;
    }

    private String sanitize(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "_unknown";
        }
        return value.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private Path writeBatchList(String batchName, String suffix, List<String> lines) throws IOException {
        Files.createDirectories(rootDir);
        Path batchDir = rootDir.resolve("_batches");
        Files.createDirectories(batchDir);
        Path output = batchDir.resolve(sanitize(batchName) + "." + sanitize(suffix) + ".txt");
        List<String> safeLines = lines == null ? Collections.<String>emptyList() : lines;
        Files.write(
                output,
                safeLines,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        );
        return output;
    }
}
