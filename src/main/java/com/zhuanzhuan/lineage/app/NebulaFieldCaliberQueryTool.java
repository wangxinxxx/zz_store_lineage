package com.zhuanzhuan.lineage.app;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.zhuanzhuan.lineage.storage.nebula.NebulaGraphConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NebulaFieldCaliberQueryTool {
    private static final String DEFAULT_TABLE_NAME = "hdp_ubu_zhuanzhuan_dw_c2b.dw_trade_sale_store_pro_retail_offline_data_full_1d";
    private static final String DEFAULT_COLUMN_NAME = "afs_create_time";
    private static final String DEFAULT_OUTPUT_FORMAT = "text";
    private static final Pattern BACKTICK_ALIAS_COLUMN_PATTERN =
            Pattern.compile("`([A-Za-z_][A-Za-z0-9_]*)`\\s*\\.\\s*`[A-Za-z_][A-Za-z0-9_]*`");
    private static final Pattern PLAIN_ALIAS_COLUMN_PATTERN =
            Pattern.compile("(?<![`\\w])([A-Za-z_][A-Za-z0-9_]*)\\s*\\.\\s*[A-Za-z_][A-Za-z0-9_]*");

    private NebulaFieldCaliberQueryTool() {
    }

    public static void main(String[] args) throws Exception {
        String tableName = readArg(args, 0, DEFAULT_TABLE_NAME);
        String columnName = readArg(args, 1, DEFAULT_COLUMN_NAME);
        String outputFormat = readArg(args, 2, DEFAULT_OUTPUT_FORMAT).toLowerCase(Locale.ROOT);
        String tableVid = "table:" + tableName;

        System.out.println("Query input");
        System.out.println("Table        : " + tableName);
        System.out.println("Column       : " + columnName);
        System.out.println("Output format: " + outputFormat);
        System.out.println();

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

            if ("json".equals(outputFormat)) {
                System.out.println(toJson(tableInfo, targetColumn, expressions, upstreamColumns, predicates));
            } else {
                printTarget(tableInfo, targetColumn);
                printExpressions(session, expressions);
                printUpstreamColumns(upstreamColumns);
                printPredicates(predicates);
                printLineageTree(session, targetColumn);
            }
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
            List<ColumnInstanceResolution> columnInstanceResolutions = fetchColumnInstanceResolutionsForExpression(session, expression.vid);
            List<LiteralInfo> literals = fetchLiteralsForExpression(session, expression.vid);
            List<RelationJoinInfo> joins = fetchJoinRelationsForExpression(session, expression.vid);
            List<PredicateUsageInfo> filters = fetchFilterPredicatesForExpression(session, expression.vid);

            System.out.println("   Source Cols: " + joinColumnRefs(sourceColumns));
            System.out.println("   Source Inst: " + joinColumnInstanceResolutions(columnInstanceResolutions));
            System.out.println("   Literals   : " + joinLiteralValues(literals));
            System.out.println("   Joins      : " + joinRelationJoinSummaries(joins));
            System.out.println("   Filters    : " + joinPredicateUsageSummaries(filters));
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

        List<ColumnInstanceResolution> columnInstanceResolutions = fetchColumnInstanceResolutionsForExpression(session, expression.vid);
        for (ColumnInstanceResolution resolution : columnInstanceResolutions) {
            System.out.println(indent(depth + 1) + "COL_INST " + resolution.instanceRef + " -> " + resolution.resolvedRef);
        }

        List<RelationJoinInfo> joins = fetchJoinRelationsForExpression(session, expression.vid);
        for (RelationJoinInfo join : joins) {
            String roleSuffix = isBlank(join.role) ? "" : " | role=" + join.role;
            System.out.println(indent(depth + 1) + "JOIN   " + join.leftRef + " <-> " + join.rightRef + roleSuffix);
        }

        List<PredicateUsageInfo> filters = fetchFilterPredicatesForExpression(session, expression.vid);
        for (PredicateUsageInfo usage : filters) {
            String sql = firstNonEmpty(usage.predicate.predicateSql, usage.predicate.normalizedPredicate, usage.predicate.vid);
            String sourceRef = isBlank(usage.sourceRef) ? "unknown" : usage.sourceRef;
            String roleSuffix = isBlank(usage.role) ? "" : " | role=" + usage.role;
            System.out.println(indent(depth + 1) + "FILTER " + sql + " | source=" + sourceRef + roleSuffix);
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
        Set<String> vids = collectUniqueVids(
                session,
                "expression_vid",
                "GO FROM " + quote(columnVid) + " OVER `latest_column_uses_expression` YIELD dst(edge) AS expression_vid;",
                "GO FROM " + quote(columnVid) + " OVER `column_uses_expression` YIELD dst(edge) AS expression_vid;"
        );
        return fetchExpressionsByVids(session, vids);
    }

    private static List<ColumnInfo> fetchUpstreamColumnsForColumn(Session session, String columnVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "column_vid",
                "GO FROM " + quote(columnVid) + " OVER `latest_column_depends_on_column` YIELD dst(edge) AS column_vid;",
                "GO FROM " + quote(columnVid) + " OVER `column_depends_on_column` YIELD dst(edge) AS column_vid;"
        );
        return fetchColumnsByVids(session, vids);
    }

    private static List<PredicateInfo> fetchPredicatesForColumn(Session session, String columnVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "predicate_vid",
                "GO FROM " + quote(columnVid) + " OVER `latest_column_flows_to_predicate` YIELD dst(edge) AS predicate_vid;",
                "GO FROM " + quote(columnVid) + " OVER `latest_predicate_depends_on_column` REVERSELY YIELD src(edge) AS predicate_vid;",
                "GO FROM " + quote(columnVid) + " OVER `predicate_depends_on_column` REVERSELY YIELD src(edge) AS predicate_vid;"
        );
        return fetchPredicatesByVids(session, vids);
    }

    private static List<ColumnInfo> fetchColumnsForExpression(Session session, String expressionVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "column_vid",
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_depends_on_column` YIELD dst(edge) AS column_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `expression_depends_on_column` YIELD dst(edge) AS column_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `latest_column_flows_to_expression` REVERSELY YIELD src(edge) AS column_vid;"
        );
        return fetchColumnsByVids(session, vids);
    }

    private static List<ColumnInstanceResolution> fetchColumnInstanceResolutionsForExpression(Session session, String expressionVid) throws Exception {
        List<ColumnInstanceInfo> columnInstances = fetchColumnInstancesForExpression(session, expressionVid);
        List<ColumnInstanceResolution> resolutions = new ArrayList<ColumnInstanceResolution>();
        Set<String> seen = new LinkedHashSet<String>();
        for (ColumnInstanceInfo columnInstance : columnInstances) {
            String instanceRef = formatColumnInstanceRef(session, columnInstance);
            List<ColumnInfo> resolvedColumns = resolveColumnsForColumnInstance(session, columnInstance, new HashSet<String>());
            if (resolvedColumns.isEmpty()) {
                String key = instanceRef + "->(unresolved)";
                if (seen.add(key)) {
                    resolutions.add(new ColumnInstanceResolution(instanceRef, "(unresolved)"));
                }
                continue;
            }
            for (ColumnInfo resolvedColumn : resolvedColumns) {
                String resolvedRef = formatColumnRef(resolvedColumn);
                String key = instanceRef + "->" + resolvedRef;
                if (seen.add(key)) {
                    resolutions.add(new ColumnInstanceResolution(instanceRef, resolvedRef));
                }
            }
        }
        return resolutions;
    }

    private static List<ColumnInstanceInfo> fetchColumnInstancesForExpression(Session session, String expressionVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "column_instance_vid",
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_depends_on_column_instance` YIELD dst(edge) AS column_instance_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `expression_depends_on_column_instance` YIELD dst(edge) AS column_instance_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `latest_column_instance_flows_to_expression` REVERSELY YIELD src(edge) AS column_instance_vid;"
        );
        return fetchColumnInstancesByVids(session, vids);
    }

    private static List<ColumnInstanceInfo> fetchSinkColumnInstancesForExpression(Session session, String expressionVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "column_instance_vid",
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_flows_to_column_instance` YIELD dst(edge) AS column_instance_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `column_instance_uses_expression` REVERSELY YIELD src(edge) AS column_instance_vid;"
        );
        return fetchColumnInstancesByVids(session, vids);
    }

    private static List<RelationInstanceInfo> fetchDirectRelationInstancesForDerivedColumnInstance(Session session, String columnInstanceVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "relation_instance_vid",
                "GO FROM " + quote(columnInstanceVid) + " OVER `latest_column_instance_depends_on_relation_instance` YIELD dst(edge) AS relation_instance_vid;",
                "GO FROM " + quote(columnInstanceVid) + " OVER `column_instance_depends_on_relation_instance` YIELD dst(edge) AS relation_instance_vid;",
                "GO FROM " + quote(columnInstanceVid) + " OVER `latest_relation_instance_flows_to_column_instance` REVERSELY YIELD src(edge) AS relation_instance_vid;"
        );
        return fetchRelationInstancesByVids(session, vids);
    }

    private static List<RelationInstanceInfo> fetchRelationInstancesForExpression(Session session, String expressionVid) throws Exception {
        List<ColumnInstanceInfo> sinkColumnInstances = fetchSinkColumnInstancesForExpression(session, expressionVid);
        Set<String> relationVids = new LinkedHashSet<String>();
        for (ColumnInstanceInfo sinkColumnInstance : sinkColumnInstances) {
            List<RelationInstanceInfo> directRelations = fetchDirectRelationInstancesForDerivedColumnInstance(session, sinkColumnInstance.vid);
            for (RelationInstanceInfo relation : directRelations) {
                relationVids.add(relation.vid);
            }
        }
        return fetchRelationInstancesByVids(session, relationVids);
    }

    private static List<RelationJoinInfo> fetchJoinRelationsForExpression(Session session, String expressionVid) throws Exception {
        List<RelationInstanceInfo> relationInstances = fetchRelationInstancesForExpression(session, expressionVid);
        List<RelationJoinInfo> joins = new ArrayList<RelationJoinInfo>();
        Set<String> seen = new LinkedHashSet<String>();
        Map<String, RelationInstanceInfo> relationCache = new HashMap<String, RelationInstanceInfo>();
        for (RelationInstanceInfo relationInstance : relationInstances) {
            relationCache.put(relationInstance.vid, relationInstance);
        }
        for (RelationInstanceInfo relationInstance : relationInstances) {
            List<VidRoleRef> joinedRelations = collectVidRoles(
                    session,
                    "relation_instance_vid",
                    "role",
                    "GO FROM " + quote(relationInstance.vid) + " OVER `latest_relation_instance_joins_relation_instance` YIELD dst(edge) AS relation_instance_vid, properties(edge).role AS role;",
                    "GO FROM " + quote(relationInstance.vid) + " OVER `relation_instance_joins_relation_instance` YIELD dst(edge) AS relation_instance_vid, properties(edge).role AS role;",
                    "GO FROM " + quote(relationInstance.vid) + " OVER `latest_relation_instance_joins_relation_instance` REVERSELY YIELD src(edge) AS relation_instance_vid, properties(edge).role AS role;",
                    "GO FROM " + quote(relationInstance.vid) + " OVER `relation_instance_joins_relation_instance` REVERSELY YIELD src(edge) AS relation_instance_vid, properties(edge).role AS role;"
            );
            for (VidRoleRef joinedRelation : joinedRelations) {
                RelationInstanceInfo target = relationCache.get(joinedRelation.vid);
                if (target == null) {
                    target = fetchRelationInstanceInfo(session, joinedRelation.vid);
                    if (target != null) {
                        relationCache.put(target.vid, target);
                    }
                }
                if (target == null) {
                    continue;
                }
                if (!relationCache.containsKey(target.vid)) {
                    continue;
                }
                String leftRef = formatRelationInstanceRef(session, relationInstance);
                String rightRef = formatRelationInstanceRef(session, target);
                if (isBlank(leftRef) || isBlank(rightRef) || leftRef.equals(rightRef)) {
                    continue;
                }
                String a = leftRef.compareTo(rightRef) <= 0 ? leftRef : rightRef;
                String b = leftRef.compareTo(rightRef) <= 0 ? rightRef : leftRef;
                String role = firstNonEmpty(joinedRelation.role, "join");
                String vidA = relationInstance.vid.compareTo(target.vid) <= 0 ? relationInstance.vid : target.vid;
                String vidB = relationInstance.vid.compareTo(target.vid) <= 0 ? target.vid : relationInstance.vid;
                String key = vidA + "|" + vidB + "|" + role;
                if (seen.add(key)) {
                    joins.add(new RelationJoinInfo(a, b, role));
                }
            }
        }
        return joins;
    }

    private static List<PredicateUsageInfo> fetchFilterPredicatesForExpression(Session session, String expressionVid) throws Exception {
        List<PredicateUsageInfo> usages = new ArrayList<PredicateUsageInfo>();
        Set<String> seen = new LinkedHashSet<String>();
        Map<String, PredicateInfo> predicateCache = new HashMap<String, PredicateInfo>();

        List<ColumnInstanceInfo> sinkColumnInstances = fetchSinkColumnInstancesForExpression(session, expressionVid);
        for (ColumnInstanceInfo sinkColumnInstance : sinkColumnInstances) {
            String sourceRef = formatColumnInstanceRef(session, sinkColumnInstance);
            List<VidRoleRef> predicateRefs = collectVidRoles(
                    session,
                    "predicate_vid",
                    "role",
                    "GO FROM " + quote(sinkColumnInstance.vid) + " OVER `latest_column_instance_filtered_by_predicate` YIELD dst(edge) AS predicate_vid, properties(edge).role AS role;",
                    "GO FROM " + quote(sinkColumnInstance.vid) + " OVER `column_instance_filtered_by_predicate` YIELD dst(edge) AS predicate_vid, properties(edge).role AS role;",
                    "GO FROM " + quote(sinkColumnInstance.vid) + " OVER `latest_predicate_flows_to_column_instance` REVERSELY YIELD src(edge) AS predicate_vid, properties(edge).role AS role;"
            );
            for (VidRoleRef predicateRef : predicateRefs) {
                addPredicateUsage(usages, seen, predicateCache, session, predicateRef.vid, sourceRef, predicateRef.role);
            }
        }

        return usages;
    }

    private static void addPredicateUsage(
            List<PredicateUsageInfo> usages,
            Set<String> seen,
            Map<String, PredicateInfo> predicateCache,
            Session session,
            String predicateVid,
            String sourceRef,
            String role
    ) throws Exception {
        PredicateInfo predicate = predicateCache.get(predicateVid);
        if (predicate == null) {
            predicate = fetchPredicateInfo(session, predicateVid);
            if (predicate != null) {
                predicateCache.put(predicateVid, predicate);
            }
        }
        if (predicate == null) {
            return;
        }
        String key = predicate.vid + "|" + firstNonEmpty(sourceRef, "") + "|" + firstNonEmpty(role, "");
        if (seen.add(key)) {
            usages.add(new PredicateUsageInfo(predicate, sourceRef, role));
        }
    }

    private static List<ColumnInfo> resolveColumnsForColumnInstance(
            Session session,
            ColumnInstanceInfo columnInstance,
            Set<String> visitedColumnInstances
    ) throws Exception {
        if (columnInstance == null || !visitedColumnInstances.add(columnInstance.vid)) {
            return new ArrayList<ColumnInfo>();
        }

        List<ColumnInfo> columns = new ArrayList<ColumnInfo>();
        Set<String> seenRefs = new LinkedHashSet<String>();

        if (!isBlank(columnInstance.columnId)) {
            ColumnInfo directColumn = fetchColumnInfo(session, columnInstance.columnId);
            if (directColumn != null) {
                addColumnIfAbsent(columns, seenRefs, directColumn);
            }
        }

        List<ColumnInstanceInfo> upstreamColumnInstances = fetchUpstreamColumnInstancesForColumnInstance(session, columnInstance.vid);
        for (ColumnInstanceInfo upstreamColumnInstance : upstreamColumnInstances) {
            List<ColumnInfo> upstreamColumns = resolveColumnsForColumnInstance(session, upstreamColumnInstance, visitedColumnInstances);
            for (ColumnInfo upstreamColumn : upstreamColumns) {
                addColumnIfAbsent(columns, seenRefs, upstreamColumn);
            }
        }

        if (!columns.isEmpty()) {
            return columns;
        }

        List<RelationInstanceInfo> relationInstances = fetchUpstreamRelationInstancesForColumnInstance(session, columnInstance.vid);
        if (relationInstances.isEmpty()) {
            RelationInstanceInfo primaryRelation = fetchPrimaryRelationForColumnInstance(session, columnInstance);
            if (primaryRelation != null) {
                relationInstances.add(primaryRelation);
            }
        }

        for (RelationInstanceInfo relationInstance : relationInstances) {
            String ownerRef = resolveRelationOwnerRef(session, relationInstance);
            if (isBlank(ownerRef) || isBlank(columnInstance.columnName)) {
                continue;
            }
            ColumnInfo pseudoColumn = new ColumnInfo(
                    "pseudo:" + relationInstance.vid + ":" + columnInstance.columnName,
                    columnInstance.columnName,
                    ownerRef,
                    "COLUMN_INSTANCE_RESOLVED",
                    columnInstance.dataType,
                    ""
            );
            addColumnIfAbsent(columns, seenRefs, pseudoColumn);
        }

        return columns;
    }

    private static List<ColumnInstanceInfo> fetchUpstreamColumnInstancesForColumnInstance(Session session, String columnInstanceVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "column_instance_vid",
                "GO FROM " + quote(columnInstanceVid) + " OVER `latest_column_instance_depends_on_column_instance` YIELD dst(edge) AS column_instance_vid;",
                "GO FROM " + quote(columnInstanceVid) + " OVER `column_instance_depends_on_column_instance` YIELD dst(edge) AS column_instance_vid;",
                "GO FROM " + quote(columnInstanceVid) + " OVER `latest_column_instance_flows_to_column_instance` REVERSELY YIELD src(edge) AS column_instance_vid;"
        );
        return fetchColumnInstancesByVids(session, vids);
    }

    private static List<RelationInstanceInfo> fetchUpstreamRelationInstancesForColumnInstance(Session session, String columnInstanceVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "relation_instance_vid",
                "GO FROM " + quote(columnInstanceVid) + " OVER `latest_column_instance_depends_on_relation_instance` YIELD dst(edge) AS relation_instance_vid;",
                "GO FROM " + quote(columnInstanceVid) + " OVER `column_instance_depends_on_relation_instance` YIELD dst(edge) AS relation_instance_vid;",
                "GO FROM " + quote(columnInstanceVid) + " OVER `latest_relation_instance_flows_to_column_instance` REVERSELY YIELD src(edge) AS relation_instance_vid;"
        );
        return fetchRelationInstancesByVids(session, vids);
    }

    private static ColumnInstanceInfo fetchColumnInstanceInfo(Session session, String columnInstanceVid) throws Exception {
        ResultSet resultSet = execute(session,
                "FETCH PROP ON `column_instance_node` " + quote(columnInstanceVid)
                        + " YIELD `column_instance_node`.`column_id` AS column_id,"
                        + "`column_instance_node`.`column_name` AS column_name,"
                        + "`column_instance_node`.`scope_id` AS scope_id,"
                        + "`column_instance_node`.`relation_instance_id` AS relation_instance_id,"
                        + "`column_instance_node`.`instance_type` AS instance_type,"
                        + "`column_instance_node`.`data_type` AS data_type,"
                        + "`column_instance_node`.`ordinal` AS ordinal;");
        if (resultSet.isEmpty()) {
            return null;
        }
        ResultSet.Record record = resultSet.rowValues(0);
        return new ColumnInstanceInfo(
                columnInstanceVid,
                value(record, "column_id"),
                value(record, "column_name"),
                value(record, "scope_id"),
                value(record, "relation_instance_id"),
                value(record, "instance_type"),
                value(record, "data_type"),
                value(record, "ordinal")
        );
    }

    private static RelationInstanceInfo fetchRelationInstanceInfo(Session session, String relationInstanceVid) throws Exception {
        if (isBlank(relationInstanceVid)) {
            return null;
        }
        ResultSet resultSet = execute(session,
                "FETCH PROP ON `relation_instance_node` " + quote(relationInstanceVid)
                        + " YIELD `relation_instance_node`.`instance_name` AS instance_name,"
                        + "`relation_instance_node`.`scope_id` AS scope_id,"
                        + "`relation_instance_node`.`source_table_id` AS source_table_id,"
                        + "`relation_instance_node`.`source_type` AS source_type,"
                        + "`relation_instance_node`.`alias_name` AS alias_name,"
                        + "`relation_instance_node`.`plan_node_name` AS plan_node_name;");
        if (resultSet.isEmpty()) {
            return null;
        }
        ResultSet.Record record = resultSet.rowValues(0);
        return new RelationInstanceInfo(
                relationInstanceVid,
                value(record, "instance_name"),
                value(record, "scope_id"),
                value(record, "source_table_id"),
                value(record, "source_type"),
                value(record, "alias_name"),
                value(record, "plan_node_name")
        );
    }

    private static RelationInstanceInfo fetchPrimaryRelationForColumnInstance(Session session, ColumnInstanceInfo columnInstance) throws Exception {
        if (columnInstance == null) {
            return null;
        }
        if (!isBlank(columnInstance.relationInstanceId)) {
            RelationInstanceInfo relationInstance = fetchRelationInstanceInfo(session, columnInstance.relationInstanceId);
            if (relationInstance != null) {
                return relationInstance;
            }
        }

        ResultSet resultSet = execute(session,
                "GO FROM " + quote(columnInstance.vid) + " OVER `relation_instance_exposes_column_instance` REVERSELY YIELD src(edge) AS relation_instance_vid;");
        if (resultSet.isEmpty()) {
            return null;
        }
        String relationInstanceVid = value(resultSet.rowValues(0), "relation_instance_vid");
        return fetchRelationInstanceInfo(session, relationInstanceVid);
    }

    private static String formatColumnInstanceRef(Session session, ColumnInstanceInfo columnInstance) throws Exception {
        RelationInstanceInfo relationInstance = fetchPrimaryRelationForColumnInstance(session, columnInstance);
        String owner = null;
        if (relationInstance != null) {
            owner = firstNonEmpty(
                    relationInstance.aliasName,
                    relationInstance.instanceName,
                    stripTableVid(relationInstance.sourceTableId),
                    relationInstance.vid
            );
        }
        if (isBlank(owner)) {
            owner = "unresolved";
        }
        String columnName = firstNonEmpty(columnInstance.columnName, stripColumnNameFromColumnId(columnInstance.columnId), "unknown_column");
        return owner + "." + columnName;
    }

    private static String formatRelationInstanceRef(Session session, RelationInstanceInfo relationInstance) throws Exception {
        if (relationInstance == null) {
            return "";
        }
        String alias = firstNonEmpty(relationInstance.aliasName, relationInstance.instanceName, relationInstance.vid);
        String owner = resolveRelationOwnerRef(session, relationInstance);
        if (isBlank(owner)) {
            return alias;
        }
        if (alias.equals(owner)) {
            return owner;
        }
        return alias + "(" + owner + ")";
    }

    private static Set<String> fetchScopeIdsForExpression(Session session, String expressionVid) throws Exception {
        return collectUniqueVids(
                session,
                "scope_vid",
                "GO FROM " + quote(expressionVid) + " OVER `scope_uses_expression` REVERSELY YIELD src(edge) AS scope_vid;"
        );
    }

    private static Set<String> extractAliasHints(ExpressionInfo expression) {
        Set<String> hints = new LinkedHashSet<String>();
        if (expression == null) {
            return hints;
        }
        collectAliasesFromText(expression.expressionSql, hints);
        collectAliasesFromText(expression.normalizedExpression, hints);
        return hints;
    }

    private static void collectAliasesFromText(String text, Set<String> hints) {
        if (isBlank(text) || hints == null) {
            return;
        }
        Matcher backtickMatcher = BACKTICK_ALIAS_COLUMN_PATTERN.matcher(text);
        while (backtickMatcher.find()) {
            String alias = backtickMatcher.group(1);
            if (!isBlank(alias)) {
                hints.add(alias.toLowerCase(Locale.ROOT));
            }
        }
        Matcher plainMatcher = PLAIN_ALIAS_COLUMN_PATTERN.matcher(text);
        while (plainMatcher.find()) {
            String alias = plainMatcher.group(1);
            if (!isBlank(alias)) {
                hints.add(alias.toLowerCase(Locale.ROOT));
            }
        }
    }

    private static boolean matchesAliasHints(RelationInstanceInfo relationInstance, Set<String> aliasHints) {
        if (relationInstance == null || aliasHints == null || aliasHints.isEmpty()) {
            return true;
        }
        String relationAlias = firstNonEmpty(relationInstance.aliasName, relationInstance.instanceName, "");
        return aliasHints.contains(relationAlias.toLowerCase(Locale.ROOT));
    }

    private static boolean isSameScopeRelationPair(
            RelationInstanceInfo left,
            RelationInstanceInfo right,
            Set<String> expressionScopeIds
    ) {
        if (left == null || right == null) {
            return false;
        }
        if (!isBlank(left.scopeId) && !isBlank(right.scopeId) && !left.scopeId.equals(right.scopeId)) {
            return false;
        }
        if (expressionScopeIds == null || expressionScopeIds.isEmpty()) {
            return true;
        }
        return expressionScopeIds.contains(left.scopeId) && expressionScopeIds.contains(right.scopeId);
    }

    private static List<ExpressionInfo> fetchExpressionsForExpression(Session session, String expressionVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "expression_vid",
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_depends_on_expression` YIELD dst(edge) AS expression_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `expression_depends_on_expression` YIELD dst(edge) AS expression_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_flows_to_expression` REVERSELY YIELD src(edge) AS expression_vid;"
        );
        return fetchExpressionsByVids(session, vids);
    }

    private static List<LiteralInfo> fetchLiteralsForExpression(Session session, String expressionVid) throws Exception {
        Set<String> vids = collectUniqueVids(
                session,
                "literal_vid",
                "GO FROM " + quote(expressionVid) + " OVER `latest_expression_depends_on_literal` YIELD dst(edge) AS literal_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `expression_depends_on_literal` YIELD dst(edge) AS literal_vid;",
                "GO FROM " + quote(expressionVid) + " OVER `latest_literal_flows_to_expression` REVERSELY YIELD src(edge) AS literal_vid;"
        );
        return fetchLiteralsByVids(session, vids);
    }

    private static List<ColumnInfo> fetchColumnsByVids(Session session, Set<String> columnVids) throws Exception {
        List<ColumnInfo> columns = new ArrayList<ColumnInfo>();
        for (String columnVid : columnVids) {
            ColumnInfo column = fetchColumnInfo(session, columnVid);
            if (column != null) {
                columns.add(column);
            }
        }
        return columns;
    }

    private static List<ExpressionInfo> fetchExpressionsByVids(Session session, Set<String> expressionVids) throws Exception {
        List<ExpressionInfo> expressions = new ArrayList<ExpressionInfo>();
        for (String expressionVid : expressionVids) {
            ExpressionInfo expression = fetchExpressionInfo(session, expressionVid);
            if (expression != null) {
                expressions.add(expression);
            }
        }
        return expressions;
    }

    private static List<LiteralInfo> fetchLiteralsByVids(Session session, Set<String> literalVids) throws Exception {
        List<LiteralInfo> literals = new ArrayList<LiteralInfo>();
        for (String literalVid : literalVids) {
            LiteralInfo literal = fetchLiteralInfo(session, literalVid);
            if (literal != null) {
                literals.add(literal);
            }
        }
        return literals;
    }

    private static List<PredicateInfo> fetchPredicatesByVids(Session session, Set<String> predicateVids) throws Exception {
        List<PredicateInfo> predicates = new ArrayList<PredicateInfo>();
        for (String predicateVid : predicateVids) {
            PredicateInfo predicate = fetchPredicateInfo(session, predicateVid);
            if (predicate != null) {
                predicates.add(predicate);
            }
        }
        return predicates;
    }

    private static List<ColumnInstanceInfo> fetchColumnInstancesByVids(Session session, Set<String> columnInstanceVids) throws Exception {
        List<ColumnInstanceInfo> instances = new ArrayList<ColumnInstanceInfo>();
        for (String columnInstanceVid : columnInstanceVids) {
            ColumnInstanceInfo info = fetchColumnInstanceInfo(session, columnInstanceVid);
            if (info != null) {
                instances.add(info);
            }
        }
        return instances;
    }

    private static List<RelationInstanceInfo> fetchRelationInstancesByVids(Session session, Set<String> relationInstanceVids) throws Exception {
        List<RelationInstanceInfo> relations = new ArrayList<RelationInstanceInfo>();
        for (String relationInstanceVid : relationInstanceVids) {
            RelationInstanceInfo info = fetchRelationInstanceInfo(session, relationInstanceVid);
            if (info != null) {
                relations.add(info);
            }
        }
        return relations;
    }

    private static Set<String> collectUniqueVids(Session session, String key, String... gqls) throws Exception {
        Set<String> vids = new LinkedHashSet<String>();
        if (gqls == null) {
            return vids;
        }
        for (String gql : gqls) {
            ResultSet resultSet = tryExecuteAllowMissingEdge(session, gql);
            if (resultSet == null) {
                continue;
            }
            for (int i = 0; i < resultSet.rowsSize(); i++) {
                String vid = value(resultSet.rowValues(i), key);
                if (!isBlank(vid)) {
                    vids.add(vid);
                }
            }
        }
        return vids;
    }

    private static List<VidRoleRef> collectVidRoles(Session session, String vidKey, String roleKey, String... gqls) throws Exception {
        List<VidRoleRef> refs = new ArrayList<VidRoleRef>();
        Set<String> seen = new LinkedHashSet<String>();
        if (gqls == null) {
            return refs;
        }
        for (String gql : gqls) {
            ResultSet resultSet = tryExecuteAllowMissingEdge(session, gql);
            if (resultSet == null) {
                continue;
            }
            for (int i = 0; i < resultSet.rowsSize(); i++) {
                ResultSet.Record record = resultSet.rowValues(i);
                String vid = value(record, vidKey);
                String role = value(record, roleKey);
                if (isBlank(vid)) {
                    continue;
                }
                String key = vid + "|" + firstNonEmpty(role, "");
                if (seen.add(key)) {
                    refs.add(new VidRoleRef(vid, role));
                }
            }
        }
        return refs;
    }

    private static ResultSet tryExecuteAllowMissingEdge(Session session, String gql) throws Exception {
        try {
            return execute(session, gql);
        } catch (IllegalStateException error) {
            if (isSchemaEdgeMissingError(error.getMessage())) {
                return null;
            }
            throw error;
        }
    }

    private static boolean isSchemaEdgeMissingError(String message) {
        if (isBlank(message)) {
            return false;
        }
        String lower = message.toLowerCase(Locale.ROOT);
        return lower.contains("unknown edge type")
                || lower.contains("edge not found")
                || lower.contains("edge doesn't exist")
                || lower.contains("unknown edge")
                || lower.contains("no schema found");
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

    private static String joinColumnInstanceResolutions(List<ColumnInstanceResolution> resolutions) {
        if (resolutions.isEmpty()) {
            return "(none)";
        }
        List<String> parts = new ArrayList<String>();
        for (ColumnInstanceResolution resolution : resolutions) {
            parts.add(resolution.instanceRef + " -> " + resolution.resolvedRef);
        }
        return String.join(", ", parts);
    }

    private static String joinRelationJoinSummaries(List<RelationJoinInfo> joins) {
        if (joins.isEmpty()) {
            return "(none)";
        }
        List<String> parts = new ArrayList<String>();
        for (RelationJoinInfo join : joins) {
            String roleSuffix = isBlank(join.role) ? "" : " | role=" + join.role;
            parts.add(join.leftRef + " <-> " + join.rightRef + roleSuffix);
        }
        return String.join("; ", parts);
    }

    private static String joinPredicateUsageSummaries(List<PredicateUsageInfo> usages) {
        if (usages.isEmpty()) {
            return "(none)";
        }
        List<String> parts = new ArrayList<String>();
        for (PredicateUsageInfo usage : usages) {
            String sql = firstNonEmpty(usage.predicate.predicateSql, usage.predicate.normalizedPredicate, usage.predicate.vid);
            String sourceRef = isBlank(usage.sourceRef) ? "unknown" : usage.sourceRef;
            String roleSuffix = isBlank(usage.role) ? "" : " | role=" + usage.role;
            parts.add(sql + " | source=" + sourceRef + roleSuffix);
        }
        return String.join("; ", parts);
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

    private static void addColumnIfAbsent(List<ColumnInfo> columns, Set<String> seenRefs, ColumnInfo columnInfo) {
        if (columnInfo == null) {
            return;
        }
        String ref = formatColumnRef(columnInfo);
        if (seenRefs.add(ref)) {
            columns.add(columnInfo);
        }
    }

    private static String resolveRelationOwnerRef(Session session, RelationInstanceInfo relationInstance) throws Exception {
        if (relationInstance == null) {
            return "";
        }
        if (!isBlank(relationInstance.sourceTableId)) {
            TableInfo tableInfo = fetchTableInfo(session, relationInstance.sourceTableId);
            if (tableInfo != null && !isBlank(tableInfo.normalizedName)) {
                return tableInfo.normalizedName;
            }
            return stripTableVid(relationInstance.sourceTableId);
        }
        return firstNonEmpty(relationInstance.aliasName, relationInstance.instanceName);
    }

    private static String stripTableVid(String sourceTableId) {
        if (isBlank(sourceTableId)) {
            return "";
        }
        if (sourceTableId.startsWith("table:")) {
            return sourceTableId.substring("table:".length());
        }
        return sourceTableId;
    }

    private static String stripColumnNameFromColumnId(String columnId) {
        if (isBlank(columnId)) {
            return "";
        }
        int splitIndex = columnId.lastIndexOf('.');
        if (splitIndex >= 0 && splitIndex < columnId.length() - 1) {
            return columnId.substring(splitIndex + 1);
        }
        return columnId;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String firstNonEmpty(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value;
            }
        }
        return "";
    }

    private static void printUsage() {
        System.out.println("Usage: NebulaFieldCaliberQueryTool [database.table] [column_name] [text|json]");
        System.out.println("Example: NebulaFieldCaliberQueryTool dw_trade_sale_store_pro_retail_offline_data_full_1d is_old_change json");
        System.out.println("Default table : " + DEFAULT_TABLE_NAME);
        System.out.println("Default column: " + DEFAULT_COLUMN_NAME);
        System.out.println("Default format: " + DEFAULT_OUTPUT_FORMAT);
    }

    private static String readArg(String[] args, int index, String defaultValue) {
        if (args == null || args.length <= index) {
            return defaultValue;
        }
        String value = args[index] == null ? "" : args[index].trim();
        if (value.isEmpty()) {
            return defaultValue;
        }
        return value;
    }

    private static String toJson(
            TableInfo tableInfo,
            ColumnInfo targetColumn,
            List<ExpressionInfo> expressions,
            List<ColumnInfo> upstreamColumns,
            List<PredicateInfo> predicates
    ) {
        StringBuilder builder = new StringBuilder();
        builder.append("{\n");
        builder.append("  \"target\": {\n");
        builder.append("    \"table\": \"").append(json(tableInfo.normalizedName)).append("\",\n");
        builder.append("    \"column\": \"").append(json(targetColumn.columnName)).append("\",\n");
        builder.append("    \"columnVid\": \"").append(json(targetColumn.vid)).append("\",\n");
        builder.append("    \"ownerId\": \"").append(json(targetColumn.ownerId)).append("\",\n");
        builder.append("    \"ownerType\": \"").append(json(targetColumn.ownerType)).append("\",\n");
        builder.append("    \"dataType\": \"").append(json(targetColumn.dataType)).append("\",\n");
        builder.append("    \"qualifier\": \"").append(json(targetColumn.qualifier)).append("\"\n");
        builder.append("  },\n");

        builder.append("  \"expressions\": [\n");
        for (int i = 0; i < expressions.size(); i++) {
            ExpressionInfo expression = expressions.get(i);
            builder.append("    {\n");
            builder.append("      \"type\": \"").append(json(expression.expressionType)).append("\",\n");
            builder.append("      \"sql\": \"").append(json(expression.expressionSql)).append("\",\n");
            builder.append("      \"normalized\": \"").append(json(expression.normalizedExpression)).append("\"\n");
            builder.append("    }");
            if (i < expressions.size() - 1) {
                builder.append(",");
            }
            builder.append("\n");
        }
        builder.append("  ],\n");

        builder.append("  \"upstreamColumns\": [\n");
        for (int i = 0; i < upstreamColumns.size(); i++) {
            ColumnInfo column = upstreamColumns.get(i);
            builder.append("    {\n");
            builder.append("      \"ref\": \"").append(json(formatColumnRef(column))).append("\",\n");
            builder.append("      \"dataType\": \"").append(json(column.dataType)).append("\",\n");
            builder.append("      \"vid\": \"").append(json(column.vid)).append("\"\n");
            builder.append("    }");
            if (i < upstreamColumns.size() - 1) {
                builder.append(",");
            }
            builder.append("\n");
        }
        builder.append("  ],\n");

        builder.append("  \"predicates\": [\n");
        for (int i = 0; i < predicates.size(); i++) {
            PredicateInfo predicate = predicates.get(i);
            builder.append("    {\n");
            builder.append("      \"type\": \"").append(json(predicate.predicateType)).append("\",\n");
            builder.append("      \"sql\": \"").append(json(predicate.predicateSql)).append("\",\n");
            builder.append("      \"normalized\": \"").append(json(predicate.normalizedPredicate)).append("\",\n");
            builder.append("      \"scopeId\": \"").append(json(predicate.scopeId)).append("\"\n");
            builder.append("    }");
            if (i < predicates.size() - 1) {
                builder.append(",");
            }
            builder.append("\n");
        }
        builder.append("  ]\n");
        builder.append("}");
        return builder.toString();
    }

    private static String json(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
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

    private static final class VidRoleRef {
        private final String vid;
        private final String role;

        private VidRoleRef(String vid, String role) {
            this.vid = vid;
            this.role = role;
        }
    }

    private static final class RelationJoinInfo {
        private final String leftRef;
        private final String rightRef;
        private final String role;

        private RelationJoinInfo(String leftRef, String rightRef, String role) {
            this.leftRef = leftRef;
            this.rightRef = rightRef;
            this.role = role;
        }
    }

    private static final class PredicateUsageInfo {
        private final PredicateInfo predicate;
        private final String sourceRef;
        private final String role;

        private PredicateUsageInfo(PredicateInfo predicate, String sourceRef, String role) {
            this.predicate = predicate;
            this.sourceRef = sourceRef;
            this.role = role;
        }
    }

    private static final class ColumnInstanceInfo {
        private final String vid;
        private final String columnId;
        private final String columnName;
        private final String scopeId;
        private final String relationInstanceId;
        private final String instanceType;
        private final String dataType;
        private final String ordinal;

        private ColumnInstanceInfo(
                String vid,
                String columnId,
                String columnName,
                String scopeId,
                String relationInstanceId,
                String instanceType,
                String dataType,
                String ordinal
        ) {
            this.vid = vid;
            this.columnId = columnId;
            this.columnName = columnName;
            this.scopeId = scopeId;
            this.relationInstanceId = relationInstanceId;
            this.instanceType = instanceType;
            this.dataType = dataType;
            this.ordinal = ordinal;
        }
    }

    private static final class RelationInstanceInfo {
        private final String vid;
        private final String instanceName;
        private final String scopeId;
        private final String sourceTableId;
        private final String sourceType;
        private final String aliasName;
        private final String planNodeName;

        private RelationInstanceInfo(
                String vid,
                String instanceName,
                String scopeId,
                String sourceTableId,
                String sourceType,
                String aliasName,
                String planNodeName
        ) {
            this.vid = vid;
            this.instanceName = instanceName;
            this.scopeId = scopeId;
            this.sourceTableId = sourceTableId;
            this.sourceType = sourceType;
            this.aliasName = aliasName;
            this.planNodeName = planNodeName;
        }
    }

    private static final class ColumnInstanceResolution {
        private final String instanceRef;
        private final String resolvedRef;

        private ColumnInstanceResolution(String instanceRef, String resolvedRef) {
            this.instanceRef = instanceRef;
            this.resolvedRef = resolvedRef;
        }
    }
}
