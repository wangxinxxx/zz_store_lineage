package com.zhuanzhuan.lineage.app;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class NebulaFieldCaliberQueryTool {
    private NebulaFieldCaliberQueryTool() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
            return;
        }

        String tableName = args[0].trim();
        String columnName = args[1].trim();
        String tableVid = "table:" + tableName;

        NebulaGraphConfig config = NebulaGraphConfig.fromSystem();
        NebulaPool pool = new NebulaPool();
        initPool(pool, config);

        Session session = null;
        try {
            session = pool.getSession(config.getUsername(), config.getPassword(), false);
            useSpace(session, config.getSpace());

            TableInfo tableInfo = fetchTableInfo(session, tableVid);
            if (tableInfo == null) {
                throw new IllegalArgumentException("Table not found in Nebula: " + tableName);
            }

            ColumnInfo targetColumn = findColumnInTable(session, tableVid, columnName);
            if (targetColumn == null) {
                throw new IllegalArgumentException("Column not found under table " + tableName + ": " + columnName);
            }

            printTarget(tableInfo, targetColumn);

            List<ExpressionInfo> expressions = fetchExpressionsForColumn(session, targetColumn.vid);
            List<ColumnInfo> upstreamColumns = fetchUpstreamColumnsForColumn(session, targetColumn.vid);
            List<PredicateInfo> predicates = fetchPredicatesForColumn(session, targetColumn.vid);

            printExpressions(session, expressions);
            printUpstreamColumns(upstreamColumns);
            printPredicates(predicates);
            printLineageTree(session, targetColumn);
        } finally {
            if (session != null) {
                session.release();
            }
            pool.close();
        }
    }

    private static void printTarget(TableInfo tableInfo, ColumnInfo targetColumn) {
        System.out.println("Target field");
        System.out.println("Table       : " + tableInfo.normalizedName);
        System.out.println("Column      : " + targetColumn.columnName);
        System.out.println("Column VID  : " + targetColumn.vid);
        System.out.println("Owner       : " + targetColumn.ownerId);
        System.out.println("Owner Type  : " + targetColumn.ownerType);
        System.out.println("Data Type   : " + targetColumn.dataType);
        System.out.println("Qualifier   : " + targetColumn.qualifier);
        System.out.println();
    }

    private static void printExpressions(Session session, List<ExpressionInfo> expressions) throws Exception {
        System.out.println("Expressions");
        if (expressions.isEmpty()) {
            System.out.println("(none)");
            System.out.println();
            return;
        }

        for (int i = 0; i < expressions.size(); i++) {
            ExpressionInfo expression = expressions.get(i);
            System.out.println((i + 1) + ". Type       : " + expression.expressionType);
            System.out.println("   SQL        : " + expression.expressionSql);
            System.out.println("   Normalized : " + expression.normalizedExpression);

            List<ColumnInfo> sourceColumns = fetchColumnsForExpression(session, expression.vid);
            List<LiteralInfo> literals = fetchLiteralsForExpression(session, expression.vid);

            System.out.println("   Source Cols: " + joinColumnRefs(sourceColumns));
            System.out.println("   Literals   : " + joinLiteralValues(literals));
        }
        System.out.println();
    }

    private static void printUpstreamColumns(List<ColumnInfo> upstreamColumns) {
        System.out.println("Direct upstream columns");
        if (upstreamColumns.isEmpty()) {
            System.out.println("(none)");
            System.out.println();
            return;
        }

        for (int i = 0; i < upstreamColumns.size(); i++) {
            ColumnInfo column = upstreamColumns.get(i);
            System.out.println((i + 1) + ". " + formatColumnRef(column) + " | type=" + column.dataType);
        }
        System.out.println();
    }

    private static void printPredicates(List<PredicateInfo> predicates) {
        System.out.println("Related predicates");
        if (predicates.isEmpty()) {
            System.out.println("(none)");
            return;
        }

        for (int i = 0; i < predicates.size(); i++) {
            PredicateInfo predicate = predicates.get(i);
            System.out.println((i + 1) + ". Type       : " + predicate.predicateType);
            System.out.println("   SQL        : " + predicate.predicateSql);
            System.out.println("   Normalized : " + predicate.normalizedPredicate);
            System.out.println("   Scope      : " + predicate.scopeId);
        }
    }

    private static void printLineageTree(Session session, ColumnInfo targetColumn) throws Exception {
        System.out.println();
        System.out.println("Lineage tree");
        printColumnTree(session, targetColumn, 0, new HashSet<String>(), new HashSet<String>());
    }

    private static void printColumnTree(
            Session session,
            ColumnInfo column,
            int depth,
            Set<String> visitedColumns,
            Set<String> visitedExpressions
    ) throws Exception {
        String prefix = indent(depth);
        System.out.println(prefix + "COLUMN " + formatColumnRef(column));
        if (!visitedColumns.add(column.vid)) {
            return;
        }

        List<ExpressionInfo> expressions = fetchExpressionsForColumn(session, column.vid);
        for (ExpressionInfo expression : expressions) {
            printExpressionTree(session, expression, depth + 1, visitedColumns, visitedExpressions);
        }

        List<ColumnInfo> upstreamColumns = fetchUpstreamColumnsForColumn(session, column.vid);
        for (ColumnInfo upstreamColumn : upstreamColumns) {
            printColumnTree(session, upstreamColumn, depth + 1, visitedColumns, visitedExpressions);
        }
    }

    private static void printExpressionTree(
            Session session,
            ExpressionInfo expression,
            int depth,
            Set<String> visitedColumns,
            Set<String> visitedExpressions
    ) throws Exception {
        String prefix = indent(depth);
        System.out.println(prefix + "EXPR   " + expression.expressionSql);
        if (!visitedExpressions.add(expression.vid)) {
            return;
        }

        List<LiteralInfo> literals = fetchLiteralsForExpression(session, expression.vid);
        for (LiteralInfo literal : literals) {
            System.out.println(indent(depth + 1) + "LITERAL " + literalDisplay(literal));
        }

        List<ExpressionInfo> nestedExpressions = fetchExpressionsForExpression(session, expression.vid);
        for (ExpressionInfo nestedExpression : nestedExpressions) {
            printExpressionTree(session, nestedExpression, depth + 1, visitedColumns, visitedExpressions);
        }

        List<ColumnInfo> sourceColumns = fetchColumnsForExpression(session, expression.vid);
        for (ColumnInfo sourceColumn : sourceColumns) {
            printColumnTree(session, sourceColumn, depth + 1, visitedColumns, visitedExpressions);
        }
    }

    private static TableInfo fetchTableInfo(Session session, String tableVid) throws Exception {
        ResultSet resultSet = execute(session,
                "FETCH PROP ON `table_node` " + quote(tableVid)
                        + " YIELD `table_node`.`normalized_name` AS normalized_name,"
                        + "`table_node`.`database_name` AS database_name,"
                        + "`table_node`.`table_name` AS table_name;");
        if (resultSet.isEmpty()) {
            return null;
        }
        ResultSet.Record record = resultSet.rowValues(0);
        return new TableInfo(
                tableVid,
                value(record, "normalized_name"),
                value(record, "database_name"),
                value(record, "table_name")
        );
    }

    private static ColumnInfo findColumnInTable(Session session, String tableVid, String columnName) throws Exception {
        ResultSet resultSet = execute(session,
                "GO FROM " + quote(tableVid) + " OVER `table_has_column` YIELD dst(edge) AS column_vid;");
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            String columnVid = value(resultSet.rowValues(i), "column_vid");
            ColumnInfo columnInfo = fetchColumnInfo(session, columnVid);
            if (columnInfo != null && columnName.equalsIgnoreCase(columnInfo.columnName)) {
                return columnInfo;
            }
        }
        return null;
    }

    private static List<ExpressionInfo> fetchExpressionsForColumn(Session session, String columnVid) throws Exception {
        ResultSet resultSet = execute(session,
                "GO FROM " + quote(columnVid) + " OVER `latest_column_uses_expression` YIELD dst(edge) AS expression_vid;");
        List<ExpressionInfo> expressions = new ArrayList<ExpressionInfo>();
        Set<String> seen = new LinkedHashSet<String>();
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            String expressionVid = value(resultSet.rowValues(i), "expression_vid");
            if (!seen.add(expressionVid)) {
                continue;
            }
            ExpressionInfo expression = fetchExpressionInfo(session, expressionVid);
            if (expression != null) {
                expressions.add(expression);
            }
        }
        return expressions;
    }

    private static List<ColumnInfo> fetchUpstreamColumnsForColumn(Session session, String columnVid) throws Exception {
        ResultSet resultSet = execute(session,
                "GO FROM " + quote(columnVid) + " OVER `latest_column_depends_on_column` YIELD dst(edge) AS column_vid;");
        return fetchColumnsByVid(session, resultSet, "column_vid");
    }

    private static List<PredicateInfo> fetchPredicatesForColumn(Session session, String columnVid) throws Exception {
        ResultSet resultSet = execute(session,
                "GO FROM " + quote(columnVid) + " OVER `latest_column_flows_to_predicate` YIELD dst(edge) AS predicate_vid;");
        List<PredicateInfo> predicates = new ArrayList<PredicateInfo>();
        Set<String> seen = new LinkedHashSet<String>();
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            String predicateVid = value(resultSet.rowValues(i), "predicate_vid");
            if (!seen.add(predicateVid)) {
                continue;
            }
            PredicateInfo predicate = fetchPredicateInfo(session, predicateVid);
            if (predicate != null) {
                predicates.add(predicate);
            }
        }
        return predicates;
    }

    private static List<ColumnInfo> fetchColumnsForExpression(Session session, String expressionVid) throws Exception {
        ResultSet resultSet = execute(session,
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_depends_on_column` YIELD dst(edge) AS column_vid;");
        return fetchColumnsByVid(session, resultSet, "column_vid");
    }

    private static List<ExpressionInfo> fetchExpressionsForExpression(Session session, String expressionVid) throws Exception {
        ResultSet resultSet = execute(session,
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_depends_on_expression` YIELD dst(edge) AS expression_vid;");
        List<ExpressionInfo> expressions = new ArrayList<ExpressionInfo>();
        Set<String> seen = new LinkedHashSet<String>();
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            String nestedExpressionVid = value(resultSet.rowValues(i), "expression_vid");
            if (!seen.add(nestedExpressionVid)) {
                continue;
            }
            ExpressionInfo expression = fetchExpressionInfo(session, nestedExpressionVid);
            if (expression != null) {
                expressions.add(expression);
            }
        }
        return expressions;
    }

    private static List<LiteralInfo> fetchLiteralsForExpression(Session session, String expressionVid) throws Exception {
        ResultSet resultSet = execute(session,
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_depends_on_literal` YIELD dst(edge) AS literal_vid;");
        List<LiteralInfo> literals = new ArrayList<LiteralInfo>();
        Set<String> seen = new LinkedHashSet<String>();
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            String literalVid = value(resultSet.rowValues(i), "literal_vid");
            if (!seen.add(literalVid)) {
                continue;
            }
            LiteralInfo literal = fetchLiteralInfo(session, literalVid);
            if (literal != null) {
                literals.add(literal);
            }
        }
        return literals;
    }

    private static List<ColumnInfo> fetchColumnsByVid(Session session, ResultSet resultSet, String key) throws Exception {
        List<ColumnInfo> columns = new ArrayList<ColumnInfo>();
        Set<String> seen = new LinkedHashSet<String>();
        for (int i = 0; i < resultSet.rowsSize(); i++) {
            String columnVid = value(resultSet.rowValues(i), key);
            if (!seen.add(columnVid)) {
                continue;
            }
            ColumnInfo column = fetchColumnInfo(session, columnVid);
            if (column != null) {
                columns.add(column);
            }
        }
        return columns;
    }

    private static ColumnInfo fetchColumnInfo(Session session, String columnVid) throws Exception {
        ResultSet resultSet = execute(session,
                "FETCH PROP ON `column_node` " + quote(columnVid)
                        + " YIELD `column_node`.`column_name` AS column_name,"
                        + "`column_node`.`owner_id` AS owner_id,"
                        + "`column_node`.`owner_type` AS owner_type,"
                        + "`column_node`.`data_type` AS data_type,"
                        + "`column_node`.`qualifier` AS qualifier;");
        if (resultSet.isEmpty()) {
            return null;
        }
        ResultSet.Record record = resultSet.rowValues(0);
        return new ColumnInfo(
                columnVid,
                value(record, "column_name"),
                value(record, "owner_id"),
                value(record, "owner_type"),
                value(record, "data_type"),
                value(record, "qualifier")
        );
    }

    private static ExpressionInfo fetchExpressionInfo(Session session, String expressionVid) throws Exception {
        ResultSet resultSet = execute(session,
                "FETCH PROP ON `expression_node` " + quote(expressionVid)
                        + " YIELD `expression_node`.`expression_type` AS expression_type,"
                        + "`expression_node`.`expression_sql` AS expression_sql,"
                        + "`expression_node`.`normalized_expression` AS normalized_expression;");
        if (resultSet.isEmpty()) {
            return null;
        }
        ResultSet.Record record = resultSet.rowValues(0);
        return new ExpressionInfo(
                expressionVid,
                value(record, "expression_type"),
                value(record, "expression_sql"),
                value(record, "normalized_expression")
        );
    }

    private static LiteralInfo fetchLiteralInfo(Session session, String literalVid) throws Exception {
        ResultSet resultSet = execute(session,
                "FETCH PROP ON `literal_node` " + quote(literalVid)
                        + " YIELD `literal_node`.`literal_type` AS literal_type,"
                        + "`literal_node`.`literal_value` AS literal_value,"
                        + "`literal_node`.`normalized_value` AS normalized_value;");
        if (resultSet.isEmpty()) {
            return null;
        }
        ResultSet.Record record = resultSet.rowValues(0);
        return new LiteralInfo(
                literalVid,
                value(record, "literal_type"),
                value(record, "literal_value"),
                value(record, "normalized_value")
        );
    }

    private static PredicateInfo fetchPredicateInfo(Session session, String predicateVid) throws Exception {
        ResultSet resultSet = execute(session,
                "FETCH PROP ON `predicate_node` " + quote(predicateVid)
                        + " YIELD `predicate_node`.`predicate_type` AS predicate_type,"
                        + "`predicate_node`.`predicate_sql` AS predicate_sql,"
                        + "`predicate_node`.`normalized_predicate` AS normalized_predicate,"
                        + "`predicate_node`.`scope_id` AS scope_id;");
        if (resultSet.isEmpty()) {
            return null;
        }
        ResultSet.Record record = resultSet.rowValues(0);
        return new PredicateInfo(
                predicateVid,
                value(record, "predicate_type"),
                value(record, "predicate_sql"),
                value(record, "normalized_predicate"),
                value(record, "scope_id")
        );
    }

    private static ResultSet execute(Session session, String gql) throws Exception {
        ResultSet resultSet = session.execute(gql);
        if (!resultSet.isSucceeded()) {
            throw new IllegalStateException("Failed nGQL: " + gql + ", error=" + resultSet.getErrorMessage());
        }
        return resultSet;
    }

    private static void useSpace(Session session, String space) throws Exception {
        execute(session, "USE `" + space + "`;");
    }

    private static void initPool(NebulaPool pool, NebulaGraphConfig config) {
        NebulaPoolConfig poolConfig = new NebulaPoolConfig()
                .setMinConnSize(config.getMinConnSize())
                .setMaxConnSize(config.getMaxConnSize())
                .setTimeout(config.getTimeoutMs())
                .setMinClusterHealthRate(0.0d);
        try {
            pool.init(Arrays.asList(new HostAddress(config.getHost(), config.getPort())), poolConfig);
        } catch (Exception error) {
            throw new IllegalStateException(
                    "Failed to initialize NebulaGraph pool for " + config.getHost() + ":" + config.getPort(),
                    error
            );
        }
    }

    private static String value(ResultSet.Record record, String key) throws Exception {
        if (!record.contains(key)) {
            return "";
        }
        return stringify(record.get(key));
    }

    private static String stringify(ValueWrapper valueWrapper) throws Exception {
        if (valueWrapper == null || valueWrapper.isEmpty() || valueWrapper.isNull()) {
            return "";
        }
        if (valueWrapper.isString()) {
            return valueWrapper.asString();
        }
        if (valueWrapper.isLong()) {
            return String.valueOf(valueWrapper.asLong());
        }
        if (valueWrapper.isDouble()) {
            return String.valueOf(valueWrapper.asDouble());
        }
        if (valueWrapper.isBoolean()) {
            return String.valueOf(valueWrapper.asBoolean());
        }
        return valueWrapper.toString();
    }

    private static String formatColumnRef(ColumnInfo column) {
        if (column == null) {
            return "";
        }
        return column.ownerId + "." + column.columnName;
    }

    private static String joinColumnRefs(List<ColumnInfo> columns) {
        if (columns.isEmpty()) {
            return "(none)";
        }
        List<String> parts = new ArrayList<String>();
        for (ColumnInfo column : columns) {
            parts.add(formatColumnRef(column));
        }
        return String.join(", ", parts);
    }

    private static String joinLiteralValues(List<LiteralInfo> literals) {
        if (literals.isEmpty()) {
            return "(none)";
        }
        List<String> parts = new ArrayList<String>();
        for (LiteralInfo literal : literals) {
            if (literal.literalValue != null && !literal.literalValue.isEmpty()) {
                parts.add(literal.literalValue);
            } else {
                parts.add(literal.normalizedValue);
            }
        }
        return String.join(", ", parts);
    }

    private static String literalDisplay(LiteralInfo literal) {
        if (literal.literalValue != null && !literal.literalValue.isEmpty()) {
            return literal.literalValue;
        }
        return literal.normalizedValue;
    }

    private static String indent(int depth) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            builder.append("  ");
        }
        return builder.toString();
    }

    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static void printUsage() {
        System.out.println("Usage: NebulaFieldCaliberQueryTool <database.table> <column_name>");
        System.out.println("Example: NebulaFieldCaliberQueryTool hdp_ubu_zhuanzhuan_ads_c2b.ads_bi_offline_store_operating_data_center_v3_full_1d sm_rec_order_subsidy_rate");
    }

    private static final class TableInfo {
        private final String vid;
        private final String normalizedName;
        private final String databaseName;
        private final String tableName;

        private TableInfo(String vid, String normalizedName, String databaseName, String tableName) {
            this.vid = vid;
            this.normalizedName = normalizedName;
            this.databaseName = databaseName;
            this.tableName = tableName;
        }
    }

    private static final class ColumnInfo {
        private final String vid;
        private final String columnName;
        private final String ownerId;
        private final String ownerType;
        private final String dataType;
        private final String qualifier;

        private ColumnInfo(String vid, String columnName, String ownerId, String ownerType, String dataType, String qualifier) {
            this.vid = vid;
            this.columnName = columnName;
            this.ownerId = ownerId;
            this.ownerType = ownerType;
            this.dataType = dataType;
            this.qualifier = qualifier;
        }
    }

    private static final class ExpressionInfo {
        private final String vid;
        private final String expressionType;
        private final String expressionSql;
        private final String normalizedExpression;

        private ExpressionInfo(String vid, String expressionType, String expressionSql, String normalizedExpression) {
            this.vid = vid;
            this.expressionType = expressionType;
            this.expressionSql = expressionSql;
            this.normalizedExpression = normalizedExpression;
        }
    }

    private static final class LiteralInfo {
        private final String vid;
        private final String literalType;
        private final String literalValue;
        private final String normalizedValue;

        private LiteralInfo(String vid, String literalType, String literalValue, String normalizedValue) {
            this.vid = vid;
            this.literalType = literalType;
            this.literalValue = literalValue;
            this.normalizedValue = normalizedValue;
        }
    }

    private static final class PredicateInfo {
        private final String vid;
        private final String predicateType;
        private final String predicateSql;
        private final String normalizedPredicate;
        private final String scopeId;

        private PredicateInfo(String vid, String predicateType, String predicateSql, String normalizedPredicate, String scopeId) {
            this.vid = vid;
            this.predicateType = predicateType;
            this.predicateSql = predicateSql;
            this.normalizedPredicate = normalizedPredicate;
            this.scopeId = scopeId;
        }
    }
}
