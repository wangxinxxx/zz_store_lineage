package com.zhuanzhuan.lineage.app;

import com.zhuanzhuan.lineage.common.HashUtils;
import com.zhuanzhuan.lineage.common.ScalaInterop;
import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExecutionStatus;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.RawPlanSnapshot;
import com.zhuanzhuan.lineage.model.SparkAppContext;
import com.zhuanzhuan.lineage.parser.DefaultSparkLineageParser;
import com.zhuanzhuan.lineage.storage.LineageStorage;
import com.zhuanzhuan.lineage.storage.LineageStorageFactory;
import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;
import com.zhuanzhuan.lineage.storage.nebula.NebulaImporterBundleWriter;
import com.zhuanzhuan.lineage.storage.nebula.SparkNebulaConnectorImporter;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SqlScriptImportService implements AutoCloseable {
    public static final String SPARK_CONF_PREFIX = "zz.lineage.spark.conf.";
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{([^}]+)}");
    private static final DateTimeFormatter DATE_TOKEN = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final String STORAGE_TYPE_NEBULA = "nebula";

    private SparkSession spark;
    private final DefaultSparkLineageParser parser;
    private final LineageStorage storage;
    private final String owner;
    private final String bizDateToken;
    private final NebulaImporterBundleWriter bundleWriter;
    private final SparkNebulaConnectorImporter connectorImporter;

    public SqlScriptImportService(
            SparkSession spark,
            DefaultSparkLineageParser parser,
            LineageStorage storage,
            String owner,
            String bizDateToken,
            NebulaImporterBundleWriter bundleWriter,
            SparkNebulaConnectorImporter connectorImporter
    ) {
        this.spark = spark;
        this.parser = parser;
        this.storage = storage;
        this.owner = owner;
        this.bizDateToken = bizDateToken;
        this.bundleWriter = bundleWriter;
        this.connectorImporter = connectorImporter;
    }

    public static SqlScriptImportService createDefault() throws IOException {
        ensureNebulaMode();
        Path warehouseDir = Files.createTempDirectory("spark-script-import-warehouse-");
        SparkSession spark = createSparkSession(warehouseDir);
        spark.sparkContext().setLogLevel("WARN");
        return new SqlScriptImportService(
                spark,
                new DefaultSparkLineageParser(),
                LineageStorageFactory.createDefault(),
                "sql_script_importer",
                LocalDate.now().format(DATE_TOKEN),
                new NebulaImporterBundleWriter(
                        NebulaImporterBundleWriter.resolveBundleDirFromSystem(),
                        NebulaGraphConfig.fromSystem(),
                        NebulaImporterBundleWriter.ImporterOptions.fromSystem()
                ),
                new SparkNebulaConnectorImporter(
                        NebulaGraphConfig.fromSystem(),
                        SparkNebulaConnectorImporter.ImportOptions.fromSystem()
                )
        );
    }

    public static SqlScriptImportService createParserOnly() throws IOException {
        Path warehouseDir = Files.createTempDirectory("spark-script-parse-warehouse-");
        SparkSession spark = createSparkSession(warehouseDir);
        spark.sparkContext().setLogLevel("ERROR");
        return new SqlScriptImportService(
                spark,
                new DefaultSparkLineageParser(),
                null,
                "sql_script_parser",
                LocalDate.now().format(DATE_TOKEN),
                new NebulaImporterBundleWriter(
                        NebulaImporterBundleWriter.resolveBundleDirFromSystem(),
                        NebulaGraphConfig.fromSystem(),
                        NebulaImporterBundleWriter.ImporterOptions.fromSystem()
                ),
                null
        );
    }

    private static SparkSession createSparkSession(Path warehouseDir) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName("sql-script-import-service")
                .master("local[1]")
                .config("spark.ui.enabled", "true")
                .config("spark.sql.warehouse.dir", warehouseDir.toAbsolutePath().toString())
                .config("hive.metastore.uris", "thrift://hive-metsatore2.58dns.org:9083");

        applySparkOverrides(builder);
        return builder.enableHiveSupport().getOrCreate();
    }

    private void stopActiveSparkSession() {
        if (spark == null) {
            return;
        }
        Path warehousePath = null;
        try {
            warehousePath = Paths.get(spark.conf().get("spark.sql.warehouse.dir"));
        } catch (Exception ignored) {
        }
        try {
            spark.stop();
        } finally {
            spark = null;
            if (warehousePath != null) {
                deleteRecursively(warehousePath);
            }
        }
    }

    public void clearStorage() {
        if (storage != null) {
            storage.clear();
        }
    }

    public NebulaImporterBundleWriter.BundleSummary exportImporterBundle(Path scriptPath) throws IOException {
        if (bundleWriter == null) {
            throw new IllegalStateException("Nebula bundle writer is not configured. Use createDefault() for Nebula export.");
        }
        Path absolutePath = scriptPath.toAbsolutePath().normalize();
        if (Files.isDirectory(absolutePath)) {
            return exportImporterBundleDirectory(absolutePath);
        }
        try (NebulaImporterBundleWriter.BundleSession bundleSession = bundleWriter.openSession(buildBatchName(absolutePath))) {
            PreparedImport prepared = prepareImport(absolutePath);
            bundleSession.appendScript(absolutePath, prepared.events, prepared.results);
            return bundleSession.finish();
        }
    }

    private NebulaImporterBundleWriter.BundleSummary exportImporterBundleDirectory(Path directoryPath) throws IOException {
        if (!Files.isDirectory(directoryPath)) {
            throw new IllegalArgumentException("Path is not a directory: " + directoryPath);
        }
        List<Path> sqlFiles = listSqlFiles(directoryPath);
        try (NebulaImporterBundleWriter.BundleSession bundleSession = bundleWriter.openSession(buildBatchName(directoryPath))) {
            for (Path sqlFile : sqlFiles) {
                PreparedImport prepared = prepareImport(sqlFile);
                bundleSession.appendScript(sqlFile, prepared.events, prepared.results);
            }
            return bundleSession.finish();
        }
    }

    private PreparedImport prepareImport(Path scriptPath) throws IOException {
        Path absolutePath = scriptPath.toAbsolutePath().normalize();
        String sqlText = new String(Files.readAllBytes(absolutePath), StandardCharsets.UTF_8);
        Map<String, String> variables = defaultVariables();
        List<String> statements = splitStatements(sqlText);

        List<ExecutionCaptureEvent> bufferedEvents = new ArrayList<ExecutionCaptureEvent>();
        List<NormalizedLineageResult> bufferedResults = new ArrayList<NormalizedLineageResult>();

        int lineageStatementIndex = 0;
        for (String rawStatement : statements) {
            String statement = rawStatement.trim();
            if (statement.isEmpty()) {
                continue;
            }

            if (isSetStatement(statement)) {
                registerSetVariable(variables, statement);
                continue;
            }

            String renderedStatement = replacePlaceholders(statement, variables);
            if (!isLineageStatement(renderedStatement)) {
                continue;
            }

            lineageStatementIndex++;
            try {
                AnalysisBundle analysisBundle = analyzeStatement(renderedStatement);
                ExecutionCaptureEvent event = buildEvent(absolutePath, lineageStatementIndex, renderedStatement, analysisBundle);
                NormalizedLineageResult result = parser.parse(
                        event,
                        analysisBundle.logicalPlan,
                        analysisBundle.analyzedPlan,
                        analysisBundle.optimizedPlan
                );
                bufferedEvents.add(event);
                bufferedResults.add(result);
            } catch (Exception error) {
                throw buildStatementFailure(absolutePath, lineageStatementIndex, renderedStatement, error);
            }
        }
        return new PreparedImport(bufferedEvents, bufferedResults);
    }

    @Override
    public void close() {
        try {
            stopActiveSparkSession();
        } finally {
            if (storage instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) storage).close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static void ensureNebulaMode() {
        if (System.getProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY) == null
                && System.getenv(LineageStorageFactory.STORAGE_TYPE_ENV) == null) {
            System.setProperty(LineageStorageFactory.STORAGE_TYPE_PROPERTY, STORAGE_TYPE_NEBULA);
        }
    }

    private LogicalPlan parsePlan(String sql) {
        Object sessionState = invokeNoArg(spark, "sessionState");
        Object sqlParser = invokeNoArg(sessionState, "sqlParser");
        Object logicalPlan = invokeOneArg(sqlParser, "parsePlan", String.class, sql);
        if (!(logicalPlan instanceof LogicalPlan)) {
            throw new IllegalStateException("Spark sqlParser did not return a LogicalPlan.");
        }
        return (LogicalPlan) logicalPlan;
    }

    private AnalysisBundle analyzeStatement(String sql) {
        LogicalPlan logicalPlan = parsePlan(sql);
        InsertIntoStatement insert = extractInsertStatement(logicalPlan);
        LogicalPlan analysisPlan = insert == null
                ? logicalPlan
                : parsePlan(stripInsertClause(sql));
        LogicalPlan analyzedPlan = analyzePlan(analysisPlan);
        return new AnalysisBundle(logicalPlan, analyzedPlan, analyzedPlan);
    }

    private LogicalPlan analyzePlan(LogicalPlan planToExecute) {
        Object sessionState = invokeNoArg(spark, "sessionState");
        Object queryExecution = invokeCompatibleOneArg(sessionState, "executePlan", planToExecute);

        if (queryExecution != null) {
            LogicalPlan analyzedPlan = asLogicalPlan(invokeNoArg(queryExecution, "analyzed"));
            if (analyzedPlan != null) {
                return analyzedPlan;
            }
        }

        Object analyzer = invokeNoArg(sessionState, "analyzer");
        Object analyzed = invokeCompatibleOneArg(analyzer, "execute", planToExecute);
        LogicalPlan analyzedPlan = asLogicalPlan(analyzed);
        if (analyzedPlan == null) {
            analyzedPlan = planToExecute;
        }
        return analyzedPlan;
    }

    private String stripInsertClause(String sql) {
        return sql.replaceFirst(
                "(?is)insert\\s+(?:overwrite|into)\\s+table\\s+[a-zA-Z_][\\w]*(?:\\.[a-zA-Z_][\\w]*)+\\s*(?:partition\\s*\\([^)]*\\))?\\s*",
                ""
        );
    }

    private InsertIntoStatement extractInsertStatement(LogicalPlan logicalPlan) {
        if (logicalPlan == null) {
            return null;
        }
        if (logicalPlan instanceof InsertIntoStatement) {
            return (InsertIntoStatement) logicalPlan;
        }
        LogicalPlan transparentChild = extractTransparentChildPlan(logicalPlan);
        if (transparentChild != null && transparentChild != logicalPlan) {
            InsertIntoStatement nested = extractInsertStatement(transparentChild);
            if (nested != null) {
                return nested;
            }
        }
        for (LogicalPlan child : ScalaInterop.toJavaList(logicalPlan.children())) {
            InsertIntoStatement nested = extractInsertStatement(child);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    private LogicalPlan asLogicalPlan(Object value) {
        if (value instanceof LogicalPlan) {
            return (LogicalPlan) value;
        }
        return null;
    }

    private ExecutionCaptureEvent buildEvent(Path scriptPath, int statementIndex, String statement, AnalysisBundle analysisBundle) {
        long captureTimeMs = System.currentTimeMillis();
        String scriptLabel = scriptLabel(scriptPath);
        String taskId = sanitizeTaskId(scriptLabel) + "_stmt_" + statementIndex;
        String runId = sanitizeTaskId(scriptLabel) + "_import_run";
        String eventId = "offline:" + HashUtils.sha1(scriptPath + "#" + statementIndex + "#" + statement);

        return new ExecutionCaptureEvent(
                eventId,
                ExecutionStatus.SUCCESS,
                new LineageTaskContext(
                        taskId,
                        scriptLabel,
                        runId,
                        bizDateToken,
                        owner,
                        scriptLabel
                ),
                new SparkAppContext(
                        spark.sparkContext().applicationId(),
                        spark.sparkContext().appName(),
                        spark.sparkContext().sparkUser(),
                        spark.sparkContext().master()
                ),
                "offline_parse",
                null,
                captureTimeMs,
                null,
                new RawPlanSnapshot(
                        planText(analysisBundle.logicalPlan),
                        planText(analysisBundle.analyzedPlan),
                        planText(analysisBundle.optimizedPlan),
                        ""
                )
        );
    }

    private String planText(LogicalPlan logicalPlan) {
        if (logicalPlan == null) {
            return "";
        }
        try {
            return logicalPlan.toString();
        } catch (Exception ignored) {
            return "";
        }
    }

    private Map<String, String> defaultVariables() {
        LinkedHashMap<String, String> variables = new LinkedHashMap<>();
        LocalDate current = LocalDate.now();
        LocalDate previousDay = current.minusDays(1);
        variables.put("biz_date", bizDateToken);
        variables.put("outfilesuffix", previousDay.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        variables.put("datesuffix", bizDateToken);
        variables.put("today", bizDateToken);
        variables.put("end_date", bizDateToken);
        variables.put("sevendaysbeforesuffix", current.minusDays(7).format(DATE_TOKEN));
        variables.put("start_date", "20230101");
        return variables;
    }

    private boolean isSetStatement(String statement) {
        return statement.toLowerCase(Locale.ROOT).startsWith("set ");
    }

    private void registerSetVariable(Map<String, String> variables, String statement) {
        String content = statement.trim().substring(4).trim();
        int index = content.indexOf('=');
        if (index <= 0) {
            return;
        }
        String key = content.substring(0, index).trim().toLowerCase(Locale.ROOT);
        String value = content.substring(index + 1).trim();
        if (value.endsWith(";")) {
            value = value.substring(0, value.length() - 1).trim();
        }
        value = stripQuotes(value);
        if (!key.isEmpty() && !value.isEmpty()) {
            variables.put(key, value.replace("-", ""));
        }
    }

    private String replacePlaceholders(String statement, Map<String, String> variables) {
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(statement);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String key = matcher.group(1).trim().toLowerCase(Locale.ROOT);
            String replacement = variables.get(key);
            if (replacement == null) {
                replacement = defaultPlaceholderValue(key);
            }
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String defaultPlaceholderValue(String key) {
        if (key.contains("date") || key.contains("suffix")) {
            return bizDateToken;
        }
        return "0";
    }

    private boolean isLineageStatement(String statement) {
        String normalized = statement.trim().toLowerCase(Locale.ROOT);
        String collapsed = normalized.replaceAll("\\s+", " ");
        if (collapsed.startsWith("add jar")
                || collapsed.startsWith("create temporary function")
                || collapsed.startsWith("create temp function")
                || collapsed.startsWith("drop table")
                || collapsed.startsWith("drop view")
                || collapsed.startsWith("msck")
                || collapsed.startsWith("analyze")) {
            return false;
        }
        if (collapsed.startsWith("insert ")) {
            return true;
        }
        if (collapsed.startsWith("create table") && collapsed.contains(" as ")) {
            return true;
        }
        if (collapsed.startsWith("with ")) {
            return collapsed.contains(" insert ") || collapsed.contains("create table");
        }
        return false;
    }

    private List<String> splitStatements(String sqlText) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean singleQuote = false;
        boolean doubleQuote = false;
        boolean backtickQuote = false;
        boolean lineComment = false;
        boolean blockComment = false;

        for (int i = 0; i < sqlText.length(); i++) {
            char currentChar = sqlText.charAt(i);
            char nextChar = i + 1 < sqlText.length() ? sqlText.charAt(i + 1) : '\0';

            if (lineComment) {
                if (currentChar == '\n') {
                    lineComment = false;
                    current.append(currentChar);
                }
                continue;
            }
            if (blockComment) {
                if (currentChar == '*' && nextChar == '/') {
                    blockComment = false;
                    i++;
                }
                continue;
            }
            if (!singleQuote && !doubleQuote && !backtickQuote) {
                if (currentChar == '-' && nextChar == '-') {
                    lineComment = true;
                    i++;
                    continue;
                }
                if (currentChar == '/' && nextChar == '*') {
                    blockComment = true;
                    i++;
                    continue;
                }
            }

            if (currentChar == '\'' && !doubleQuote && !backtickQuote) {
                singleQuote = !singleQuote;
            } else if (currentChar == '"' && !singleQuote && !backtickQuote) {
                doubleQuote = !doubleQuote;
            } else if (currentChar == '`' && !singleQuote && !doubleQuote) {
                backtickQuote = !backtickQuote;
            }

            if (currentChar == ';' && !singleQuote && !doubleQuote && !backtickQuote) {
                statements.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(currentChar);
        }

        if (current.length() > 0) {
            statements.add(current.toString());
        }
        return statements;
    }

    private String sanitizeTaskId(String fileName) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        StringBuilder builder = new StringBuilder(lower.length());
        for (int i = 0; i < lower.length(); i++) {
            char ch = lower.charAt(i);
            if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                builder.append(ch);
            } else {
                builder.append('_');
            }
        }
        return builder.toString().replaceAll("_+", "_");
    }

    private String stripQuotes(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return value.substring(1, value.length() - 1);
            }
        }
        return value;
    }

    private String buildBatchName(Path path) {
        String label = sanitizeTaskId(scriptLabel(path));
        String digest = HashUtils.sha1(path.toAbsolutePath().normalize().toString()).substring(0, 8);
        if (label.length() > 32) {
            label = label.substring(0, 32);
        }
        return label + "_" + digest;
    }

    private String scriptLabel(Path path) {
        if (path == null) {
            return "unknown";
        }
        Path fileName = path.getFileName();
        if (fileName == null) {
            return "unknown";
        }
        String label = fileName.toString().trim();
        return label.isEmpty() ? "unknown" : label;
    }

    private IllegalStateException buildStatementFailure(Path scriptPath, int statementIndex, String statement, Throwable error) {
        Throwable rootCause = rootCause(error);
        Throwable displayError = rootCause == null ? error : rootCause;
        String message = "Failed to analyze SQL statement #"
                + statementIndex
                + " in "
                + scriptLabel(scriptPath)
                + ": "
                + displayError
                + System.lineSeparator()
                + "SQL:"
                + System.lineSeparator()
                + statement;
        return new IllegalStateException(message, rootCause == null ? error : rootCause);
    }

    private Object invokeNoArg(Object target, String methodName) {
        if (target == null) {
            throw new IllegalStateException("Target is null for method " + methodName);
        }
        try {
            return invokeMethod(target, target.getClass().getMethod(methodName));
        } catch (NoSuchMethodException ignored) {
        }
        try {
            return invokeMethod(target, target.getClass().getDeclaredMethod(methodName));
        } catch (Exception error) {
            throw reflectionFailure(target, methodName, error);
        }
    }

    private Object invokeOneArg(Object target, String methodName, Class<?> argumentType, Object argumentValue) {
        if (target == null) {
            throw new IllegalStateException("Target is null for method " + methodName);
        }
        try {
            return invokeMethod(target, target.getClass().getMethod(methodName, argumentType), argumentValue);
        } catch (NoSuchMethodException ignored) {
        }
        try {
            return invokeMethod(target, target.getClass().getDeclaredMethod(methodName, argumentType), argumentValue);
        } catch (Exception error) {
            throw reflectionFailure(target, methodName, error);
        }
    }

    private Object invokeCompatibleOneArg(Object target, String methodName, Object argumentValue) {
        if (target == null) {
            throw new IllegalStateException("Target is null for method " + methodName);
        }
        Method[] methods = target.getClass().getMethods();
        Object result = invokeCompatibleOneArg(methods, target, methodName, argumentValue);
        if (result != null) {
            return result;
        }
        methods = target.getClass().getDeclaredMethods();
        result = invokeCompatibleOneArg(methods, target, methodName, argumentValue);
        if (result != null) {
            return result;
        }
        throw new IllegalStateException("Failed to invoke compatible method `" + methodName + "` on " + target.getClass().getName());
    }

    private LogicalPlan extractTransparentChildPlan(Object target) {
        for (String methodName : new String[]{"child", "plan", "queryPlan", "inputPlan"}) {
            try {
                Object value = invokeNoArg(target, methodName);
                if (value instanceof LogicalPlan) {
                    return (LogicalPlan) value;
                }
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    private Object invokeCompatibleOneArg(Method[] methods, Object target, String methodName, Object argumentValue) {
        for (Method method : methods) {
            if (!methodName.equals(method.getName()) || method.getParameterTypes().length != 1) {
                continue;
            }
            Class<?> parameterType = method.getParameterTypes()[0];
            if (argumentValue != null && !parameterType.isInstance(argumentValue) && !parameterType.isAssignableFrom(argumentValue.getClass())) {
                continue;
            }
            return invokeMethod(target, method, argumentValue);
        }
        return null;
    }

    private Object invokeMethod(Object target, Method method, Object... arguments) {
        try {
            method.setAccessible(true);
            return method.invoke(target, arguments);
        } catch (InvocationTargetException error) {
            throw reflectionFailure(target, method.getName(), error.getCause() == null ? error : error.getCause());
        } catch (Exception error) {
            throw reflectionFailure(target, method.getName(), error);
        }
    }

    private IllegalStateException reflectionFailure(Object target, String methodName, Throwable error) {
        Throwable cause = error instanceof InvocationTargetException && ((InvocationTargetException) error).getCause() != null
                ? ((InvocationTargetException) error).getCause()
                : error;
        return new IllegalStateException(
                "Failed to invoke method `"
                        + methodName
                        + "` on "
                        + target.getClass().getName()
                        + (cause == null || cause.getMessage() == null || cause.getMessage().trim().isEmpty()
                        ? ""
                        : ": " + cause.getMessage()),
                cause == null ? error : cause
        );
    }

    private static void applySparkOverrides(SparkSession.Builder builder) {
        for (Map.Entry<String, String> entry : prefixedProperties(SPARK_CONF_PREFIX).entrySet()) {
            builder.config(entry.getKey(), entry.getValue());
        }
    }

    private static Map<String, String> prefixedProperties(String prefix) {
        LinkedHashMap<String, String> values = new LinkedHashMap<String, String>();
        for (Map.Entry<Object, Object> entry : System.getProperties().entrySet()) {
            String key = String.valueOf(entry.getKey());
            if (!key.startsWith(prefix)) {
                continue;
            }
            String value = entry.getValue() == null ? null : String.valueOf(entry.getValue()).trim();
            if (value == null || value.isEmpty()) {
                continue;
            }
            values.put(key.substring(prefix.length()), value);
        }
        return values;
    }

    private void deleteRecursively(Path root) {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try {
            Files.walk(root)
                    .sorted(Collections.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        } catch (IOException ignored) {
        }
    }

    private List<Path> listSqlFiles(Path directoryPath) throws IOException {
        try (Stream<Path> stream = Files.walk(directoryPath)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".sql"))
                    .sorted(Comparator.comparing(Path::toString))
                    .collect(Collectors.toList());
        }
    }

    private Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current != null && current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static final class AnalysisBundle {
        private final LogicalPlan logicalPlan;
        private final LogicalPlan analyzedPlan;
        private final LogicalPlan optimizedPlan;

        private AnalysisBundle(LogicalPlan logicalPlan, LogicalPlan analyzedPlan, LogicalPlan optimizedPlan) {
            this.logicalPlan = logicalPlan;
            this.analyzedPlan = analyzedPlan;
            this.optimizedPlan = optimizedPlan;
        }
    }

    private static final class PreparedImport {
        private final List<ExecutionCaptureEvent> events;
        private final List<NormalizedLineageResult> results;

        private PreparedImport(
                List<ExecutionCaptureEvent> events,
                List<NormalizedLineageResult> results
        ) {
            this.events = events;
            this.results = results;
        }
    }
}
