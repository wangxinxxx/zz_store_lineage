package com.zhuanzhuan.lineage.demo;

import com.zhuanzhuan.lineage.app.SqlScriptImportService;
import com.zhuanzhuan.lineage.model.ColumnInstanceNode;
import com.zhuanzhuan.lineage.model.ColumnNode;
import com.zhuanzhuan.lineage.model.ExecutionCaptureEvent;
import com.zhuanzhuan.lineage.model.ExecutionStatus;
import com.zhuanzhuan.lineage.model.ExpressionNode;
import com.zhuanzhuan.lineage.model.LineageGraphEdge;
import com.zhuanzhuan.lineage.model.LineageTaskContext;
import com.zhuanzhuan.lineage.model.LiteralNode;
import com.zhuanzhuan.lineage.model.NormalizedLineageResult;
import com.zhuanzhuan.lineage.model.PredicateNode;
import com.zhuanzhuan.lineage.model.RawPlanSnapshot;
import com.zhuanzhuan.lineage.model.RelationInstanceNode;
import com.zhuanzhuan.lineage.model.ScopeNode;
import com.zhuanzhuan.lineage.model.SparkAppContext;
import com.zhuanzhuan.lineage.parser.SparkLineageParser;
import com.zhuanzhuan.lineage.parser.DefaultSparkLineageParser;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import scala.collection.Seq;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class OfflineScriptLineageDebug {
    private OfflineScriptLineageDebug() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: OfflineScriptLineageDebug <sql-path> <target-column-node-id>");
            return;
        }
        boolean compact = Boolean.parseBoolean(System.getProperty("zz.lineage.debug.compact", "false"));

        Path sqlPath = Paths.get(args[0]).toAbsolutePath().normalize();
        String targetNodeId = args[1];
        String targetKeyword = targetNodeId.substring(targetNodeId.lastIndexOf('.') + 1);
        SparkSession spark = SparkSession.builder()
                .appName("offline-script-lineage-debug")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.catalogImplementation", "in-memory")
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        try {
            String sqlText = new String(Files.readAllBytes(sqlPath), StandardCharsets.UTF_8)
                    .replace("${outFileSuffix}", "20260325")
                    .replace("${today}", "20260325")
                    .replace("${start_date}", "20230101");
            String sql = extractLineageStatement(sqlText);
            LogicalPlan plan = parsePlan(spark, sql);
            if (!compact) {
                printPlanTree(plan, 0, 5);
                printCteRelations(plan);
            }
            DefaultSparkLineageParser parser = new DefaultSparkLineageParser();
            if (!compact) {
                debugCollectTargets(parser, plan);
                debugCteTarget(parser, plan, "t_gmv_n_profit_result", "zonghe_online_rec_net_gmv");
                debugCteTarget(parser, plan, "t_income", "online_rec_net_gmv");
                debugCteTarget(parser, plan, "t_result", "online_rec_net_gmv");
                debugCteTarget(parser, plan, "t_recycle_net", "online_rec_net_gmv");
                debugResultAliasT2Target(parser, plan, "online_rec_net_gmv");
                debugResultAliasT2FirstChildTarget(parser, plan, "online_rec_net_gmv");
            }
            NormalizedLineageResult result = parser.parse(buildEvent(sqlPath, spark, sql, plan), plan);

            System.out.println("statementType=" + result.getStatementType());
            System.out.println("inputTables=" + result.getInputTables().size());
            System.out.println("outputTables=" + result.getOutputTables().size());
            System.out.println("columns=" + result.getColumnNodes().size());
            System.out.println("expressions=" + result.getExpressionNodes().size());
            System.out.println("scopes=" + result.getScopeNodes().size());
            System.out.println("literals=" + result.getLiteralNodes().size());
            System.out.println("columnInstances=" + result.getColumnInstanceNodes().size());
            System.out.println("relationInstances=" + result.getRelationInstanceNodes().size());
            System.out.println("predicates=" + result.getPredicateNodes().size());
            System.out.println("edges=" + result.getGraphEdges().size());

            for (ScopeNode scopeNode : compact ? new ArrayList<ScopeNode>() : result.getScopeNodes()) {
                System.out.println("SCOPE nodeId=" + scopeNode.getNodeId()
                        + " type=" + scopeNode.getScopeType()
                        + " name=" + scopeNode.getScopeName()
                        + " parent=" + scopeNode.getParentScopeId());
            }
            for (RelationInstanceNode relationInstanceNode : compact ? new ArrayList<RelationInstanceNode>() : result.getRelationInstanceNodes()) {
                System.out.println("RELATION_INSTANCE nodeId=" + relationInstanceNode.getNodeId()
                        + " scopeId=" + relationInstanceNode.getScopeId()
                        + " sourceTableId=" + relationInstanceNode.getSourceTableId()
                        + " sourceType=" + relationInstanceNode.getSourceType());
            }
            for (ColumnInstanceNode columnInstanceNode : result.getColumnInstanceNodes()) {
                if (columnInstanceNode.getColumnName() != null
                        && (columnInstanceNode.getColumnName().contains(targetKeyword)
                        || "order_type".equalsIgnoreCase(columnInstanceNode.getColumnName()))) {
                    System.out.println("COLUMN_INSTANCE nodeId=" + columnInstanceNode.getNodeId()
                            + " scopeId=" + columnInstanceNode.getScopeId()
                            + " relationInstanceId=" + columnInstanceNode.getRelationInstanceId()
                            + " columnId=" + columnInstanceNode.getColumnId()
                            + " type=" + columnInstanceNode.getInstanceType());
                }
            }
            for (PredicateNode predicateNode : compact ? new ArrayList<PredicateNode>() : result.getPredicateNodes()) {
                System.out.println("PREDICATE nodeId=" + predicateNode.getNodeId()
                        + " scopeId=" + predicateNode.getScopeId()
                        + " type=" + predicateNode.getPredicateType()
                        + " sql=" + predicateNode.getPredicateSql());
            }
            for (LiteralNode literalNode : compact ? new ArrayList<LiteralNode>() : result.getLiteralNodes()) {
                if ("'pay'".equalsIgnoreCase(literalNode.getLiteralValue())
                        || "'refund'".equalsIgnoreCase(literalNode.getLiteralValue())
                        || "'线上导流'".equals(literalNode.getLiteralValue())) {
                    System.out.println("LITERAL nodeId=" + literalNode.getNodeId()
                            + " type=" + literalNode.getLiteralType()
                            + " value=" + literalNode.getLiteralValue());
                }
            }

            for (ColumnNode columnNode : result.getColumnNodes()) {
                if (targetNodeId.equals(columnNode.getNodeId()) || columnNode.getNodeId().contains(targetKeyword)) {
                    System.out.println("TARGET_COLUMN owner=" + columnNode.getOwnerId() + ", ownerType=" + columnNode.getOwnerType());
                    System.out.println("TARGET_COLUMN nodeId=" + columnNode.getNodeId());
                }
            }

            Set<String> targetColumnInstanceIds = new HashSet<>();
            for (ColumnInstanceNode columnInstanceNode : result.getColumnInstanceNodes()) {
                if (columnInstanceNode.getColumnId() != null
                        && (targetNodeId.equals(columnInstanceNode.getColumnId())
                        || columnInstanceNode.getColumnId().contains(targetKeyword))) {
                    targetColumnInstanceIds.add(columnInstanceNode.getNodeId());
                    System.out.println("TARGET_COLUMN_INSTANCE nodeId=" + columnInstanceNode.getNodeId()
                            + " columnId=" + columnInstanceNode.getColumnId()
                            + " scopeId=" + columnInstanceNode.getScopeId()
                            + " relationInstanceId=" + columnInstanceNode.getRelationInstanceId()
                            + " type=" + columnInstanceNode.getInstanceType());
                }
            }

            for (LineageGraphEdge edge : result.getGraphEdges()) {
                if (edge.getTargetNodeId().contains(targetKeyword) || edge.getSourceNodeId().contains(targetKeyword)) {
                    System.out.println("EDGE " + edge.getEdgeType() + " : " + edge.getSourceNodeId() + " -> " + edge.getTargetNodeId() + " role=" + edge.getRole());
                }
                if (targetColumnInstanceIds.contains(edge.getSourceNodeId()) || targetColumnInstanceIds.contains(edge.getTargetNodeId())) {
                    System.out.println("TARGET_INSTANCE_EDGE " + edge.getEdgeType() + " : " + edge.getSourceNodeId() + " -> " + edge.getTargetNodeId() + " role=" + edge.getRole());
                }
            }

            for (ColumnNode columnNode : compact ? new ArrayList<ColumnNode>() : result.getColumnNodes()) {
                if (columnNode.getNodeId().contains("online_rec_net_gmv")) {
                    System.out.println("ONLINE_COLUMN owner=" + columnNode.getOwnerId() + ", ownerType=" + columnNode.getOwnerType());
                    System.out.println("ONLINE_COLUMN nodeId=" + columnNode.getNodeId());
                }
            }

            for (LineageGraphEdge edge : result.getGraphEdges()) {
                if (edge.getSourceNodeId().contains("online_rec_net_gmv") || edge.getTargetNodeId().contains("online_rec_net_gmv")) {
                    System.out.println("ONLINE_EDGE " + edge.getEdgeType() + " : " + edge.getSourceNodeId() + " -> " + edge.getTargetNodeId() + " role=" + edge.getRole());
                }
            }

            if (!compact) {
                printReverseBacktrace(result, targetNodeId, 4);
            }
        } finally {
            spark.stop();
        }
    }

    private static String extractLineageStatement(String sqlText) {
        String lowerCase = sqlText.toLowerCase();
        int withIndex = lowerCase.indexOf("with ");
        int insertIndex = lowerCase.indexOf("insert ");
        int startIndex = -1;
        if (withIndex >= 0) {
            startIndex = withIndex;
        }
        if (insertIndex >= 0 && (startIndex < 0 || insertIndex < startIndex)) {
            startIndex = insertIndex;
        }
        if (startIndex >= 0) {
            return sqlText.substring(startIndex).trim();
        }

        String[] statements = sqlText.split(";");
        String selected = null;
        for (String rawStatement : statements) {
            String statement = rawStatement == null ? "" : rawStatement.trim();
            if (statement.isEmpty()) {
                continue;
            }
            String normalized = statement.toLowerCase();
            if (normalized.startsWith("set ")) {
                continue;
            }
            if (normalized.startsWith("--")) {
                continue;
            }
            selected = statement;
        }
        return selected == null ? sqlText : selected;
    }

    private static LogicalPlan parsePlan(SparkSession spark, String sql) throws Exception {
        Object sessionState = invokeNoArg(spark, "sessionState");
        Object sqlParser = invokeNoArg(sessionState, "sqlParser");
        Object logicalPlan = invokeOneArg(sqlParser, "parsePlan", String.class, sql);
        return (LogicalPlan) logicalPlan;
    }

    private static Object invokeNoArg(Object target, String methodName) throws Exception {
        try {
            Method method = target.getClass().getMethod(methodName);
            method.setAccessible(true);
            return method.invoke(target);
        } catch (Exception ignored) {
        }
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }

    private static Object invokeOneArg(Object target, String methodName, Class<?> argumentType, Object argumentValue) throws Exception {
        try {
            Method method = target.getClass().getMethod(methodName, argumentType);
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        } catch (Exception ignored) {
        }
        try {
            Method method = target.getClass().getDeclaredMethod(methodName, argumentType);
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        } catch (Exception ignored) {
        }

        for (Method method : target.getClass().getMethods()) {
            if (!methodName.equals(method.getName()) || method.getParameterCount() != 1) {
                continue;
            }
            Class<?> parameterType = method.getParameterTypes()[0];
            if (!parameterType.isAssignableFrom(argumentType) && !argumentType.isAssignableFrom(parameterType)) {
                continue;
            }
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        }

        for (Method method : target.getClass().getDeclaredMethods()) {
            if (!methodName.equals(method.getName()) || method.getParameterCount() != 1) {
                continue;
            }
            Class<?> parameterType = method.getParameterTypes()[0];
            if (!parameterType.isAssignableFrom(argumentType) && !argumentType.isAssignableFrom(parameterType)) {
                continue;
            }
            method.setAccessible(true);
            return method.invoke(target, argumentValue);
        }

        throw new NoSuchMethodException(target.getClass().getName() + "." + methodName + "(" + argumentType.getName() + ")");
    }

    private static ExecutionCaptureEvent buildEvent(Path sqlPath, SparkSession spark, String statement, LogicalPlan logicalPlan) {
        return new ExecutionCaptureEvent(
                "debug-event",
                ExecutionStatus.SUCCESS,
                new LineageTaskContext("debug-task", sqlPath.getFileName().toString(), "debug-run", "20260325", "debug", sqlPath.toString()),
                new SparkAppContext(spark.sparkContext().applicationId(), spark.sparkContext().appName(), spark.sparkContext().sparkUser(), spark.sparkContext().master()),
                "offline_debug",
                null,
                System.currentTimeMillis(),
                null,
                new RawPlanSnapshot(logicalPlan.toString(), logicalPlan.toString(), logicalPlan.toString(), statement)
        );
    }

    private static void printPlanTree(LogicalPlan plan, int depth, int maxDepth) throws Exception {
        if (plan == null || depth > maxDepth) {
            return;
        }
        StringBuilder prefix = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            prefix.append("  ");
        }
        String aliasText = "";
        if (plan instanceof SubqueryAlias) {
            aliasText = " alias=" + ((SubqueryAlias) plan).alias();
        }
        System.out.println(prefix + "PLAN depth=" + depth + " node=" + plan.nodeName() + " class=" + plan.getClass().getName() + aliasText);
        if (plan instanceof Project) {
            List<?> projectList = scalaInteropToJavaList(((Project) plan).projectList());
            for (int i = 0; i < projectList.size() && i < 8; i++) {
                Object item = projectList.get(i);
                System.out.println(prefix + "  project[" + i + "]=" + item.getClass().getName() + " text=" + String.valueOf(item));
            }
        }
        List<LogicalPlan> children = childrenOf(plan);
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan child = children.get(i);
            String childAliasText = "";
            if (child instanceof SubqueryAlias) {
                childAliasText = " alias=" + ((SubqueryAlias) child).alias();
            }
            System.out.println(prefix + "  child[" + i + "]=" + child.nodeName() + " class=" + child.getClass().getName() + childAliasText);
            printPlanTree(child, depth + 1, maxDepth);
        }
    }

    private static List<LogicalPlan> childrenOf(LogicalPlan plan) throws Exception {
        Method method = plan.getClass().getMethod("children");
        Object result = method.invoke(plan);
        List<LogicalPlan> children = new ArrayList<>();
        if (result instanceof Seq) {
            scala.collection.Iterator<?> iterator = ((Seq<?>) result).iterator();
            while (iterator.hasNext()) {
                Object next = iterator.next();
                if (next instanceof LogicalPlan) {
                    children.add((LogicalPlan) next);
                }
            }
        }
        return children;
    }

    private static List<?> scalaInteropToJavaList(Seq<?> seq) {
        List<Object> values = new ArrayList<>();
        scala.collection.Iterator<?> iterator = seq.iterator();
        while (iterator.hasNext()) {
            values.add(iterator.next());
        }
        return values;
    }

    private static void debugCollectTargets(DefaultSparkLineageParser parser, LogicalPlan plan) throws Exception {
        Method buildResolutionContext = DefaultSparkLineageParser.class.getDeclaredMethod(
                "buildResolutionContext",
                LogicalPlan.class,
                LogicalPlan.class,
                LogicalPlan.class
        );
        buildResolutionContext.setAccessible(true);
        Object resolutionContext = buildResolutionContext.invoke(parser, plan, plan, plan);

        Method method = DefaultSparkLineageParser.class.getDeclaredMethod(
                "collectTargetExpressions",
                LogicalPlan.class,
                resolutionContext.getClass()
        );
        method.setAccessible(true);
        Object rootResult = tryInvokeCollect(method, parser, "root", plan, resolutionContext);
        List<LogicalPlan> children = childrenOf(plan);
        for (int i = 0; i < children.size() && i < 2; i++) {
            tryInvokeCollect(method, parser, "child[" + i + "]", children.get(i), resolutionContext);
            List<LogicalPlan> subChildren = childrenOf(children.get(i));
            for (int j = 0; j < subChildren.size() && j < 2; j++) {
                tryInvokeCollect(method, parser, "child[" + i + "].child[" + j + "]", subChildren.get(j), resolutionContext);
            }
        }
        if (rootResult instanceof List) {
            dumpTargetsWithKeyword((List<?>) rootResult, "zonghe_online_rec_net_gmv");
        }
    }

    private static void printCteRelations(LogicalPlan plan) throws Exception {
        Object result = invokeNoArg(plan, "cteRelations");
        if (!(result instanceof Seq)) {
            System.out.println("CTE none");
            return;
        }
        List<?> cteRelations = scalaInteropToJavaList((Seq<?>) result);
        System.out.println("CTE count=" + cteRelations.size());
        for (int i = 0; i < cteRelations.size() && i < 12; i++) {
            Object relation = cteRelations.get(i);
            System.out.println("CTE[" + i + "]=" + String.valueOf(relation));
        }
    }

    private static void debugCteTarget(DefaultSparkLineageParser parser, LogicalPlan plan, String cteName, String targetName) throws Exception {
        Object cteRelations = invokeNoArg(plan, "cteRelations");
        if (!(cteRelations instanceof Seq)) {
            return;
        }
        List<?> relations = scalaInteropToJavaList((Seq<?>) cteRelations);
        Object targetPlan = null;
        for (Object relation : relations) {
            String text = String.valueOf(relation);
            if (text.startsWith("(" + cteName + ",")) {
                if (relation instanceof scala.Tuple2) {
                    Object planValue = ((scala.Tuple2<?, ?>) relation)._2();
                    if (planValue instanceof LogicalPlan) {
                        targetPlan = planValue;
                        break;
                    }
                }
            }
        }
        if (!(targetPlan instanceof LogicalPlan)) {
            System.out.println("CTE_TARGET missing cte=" + cteName);
            return;
        }

        Method buildResolutionContext = DefaultSparkLineageParser.class.getDeclaredMethod(
                "buildResolutionContext",
                LogicalPlan.class,
                LogicalPlan.class,
                LogicalPlan.class
        );
        buildResolutionContext.setAccessible(true);
        Object resolutionContext = buildResolutionContext.invoke(parser, plan, plan, plan);

        Method collectTargets = DefaultSparkLineageParser.class.getDeclaredMethod(
                "collectTargetExpressions",
                LogicalPlan.class,
                resolutionContext.getClass()
        );
        collectTargets.setAccessible(true);
        Object targets = collectTargets.invoke(parser, targetPlan, resolutionContext);
        if (!(targets instanceof List)) {
            return;
        }
        for (Object target : (List<?>) targets) {
            Method getName = target.getClass().getDeclaredMethod("getName");
            Method getExpression = target.getClass().getDeclaredMethod("getExpression");
            Method getSourceColumns = target.getClass().getDeclaredMethod("getSourceColumns");
            Method getDependentTargets = target.getClass().getDeclaredMethod("getDependentTargets");
            getName.setAccessible(true);
            getExpression.setAccessible(true);
            getSourceColumns.setAccessible(true);
            getDependentTargets.setAccessible(true);
            Object name = getName.invoke(target);
            if (name != null && targetName.equals(String.valueOf(name))) {
                System.out.println("CTE_TARGET cte=" + cteName + " name=" + name + " expr=" + getExpression.invoke(target));
                Object sourceColumns = getSourceColumns.invoke(target);
                if (sourceColumns instanceof List) {
                    for (Object sourceColumn : (List<?>) sourceColumns) {
                        Method getNodeId = sourceColumn.getClass().getMethod("getNodeId");
                        Method getOwnerId = sourceColumn.getClass().getMethod("getOwnerId");
                        Method getOwnerType = sourceColumn.getClass().getMethod("getOwnerType");
                        System.out.println("CTE_SOURCE cte=" + cteName
                                + " nodeId=" + getNodeId.invoke(sourceColumn)
                                + " owner=" + getOwnerId.invoke(sourceColumn)
                                + " ownerType=" + getOwnerType.invoke(sourceColumn));
                    }
                }
                Object dependentTargets = getDependentTargets.invoke(target);
                if (dependentTargets instanceof List) {
                    for (Object dependentTarget : (List<?>) dependentTargets) {
                        Method depGetName = dependentTarget.getClass().getDeclaredMethod("getName");
                        Method depGetExpression = dependentTarget.getClass().getDeclaredMethod("getExpression");
                        depGetName.setAccessible(true);
                        depGetExpression.setAccessible(true);
                        System.out.println("CTE_DEPENDENCY cte=" + cteName
                                + " target=" + name
                                + " depName=" + depGetName.invoke(dependentTarget)
                                + " depExpr=" + depGetExpression.invoke(dependentTarget));
                    }
                }
            }
        }

        if ("t_gmv_n_profit_result".equals(cteName)) {
            debugTgmvJoinTargets(parser, (LogicalPlan) targetPlan, resolutionContext, collectTargets);
        }
    }

    private static void debugTgmvJoinTargets(
            DefaultSparkLineageParser parser,
            LogicalPlan targetPlan,
            Object resolutionContext,
            Method collectTargets
    ) throws Exception {
        List<LogicalPlan> level1 = childrenOf(targetPlan);
        if (level1.isEmpty()) {
            return;
        }
        LogicalPlan project = level1.get(0);
        List<LogicalPlan> level2 = childrenOf(project);
        if (level2.isEmpty()) {
            return;
        }
        LogicalPlan subquery = level2.get(0);
        List<LogicalPlan> level3 = childrenOf(subquery);
        if (level3.isEmpty()) {
            return;
        }
        LogicalPlan aggregate = level3.get(0);
        List<LogicalPlan> level4 = childrenOf(aggregate);
        if (level4.isEmpty()) {
            return;
        }
        LogicalPlan join = level4.get(0);
        Object joinTargets = collectTargets.invoke(parser, join, resolutionContext);
        if (!(joinTargets instanceof List)) {
            return;
        }
        System.out.println("TGMV_JOIN_TARGETS size=" + ((List<?>) joinTargets).size());
        int printed = 0;
        for (Object target : (List<?>) joinTargets) {
            Method getName = target.getClass().getDeclaredMethod("getName");
            Method getSourceColumns = target.getClass().getDeclaredMethod("getSourceColumns");
            getName.setAccessible(true);
            getSourceColumns.setAccessible(true);
            Object targetName = getName.invoke(target);
            System.out.println("TGMV_JOIN_TARGET name=" + targetName);
            if ("online_rec_net_gmv".equals(String.valueOf(targetName))) {
                Object sourceColumns = getSourceColumns.invoke(target);
                if (sourceColumns instanceof List) {
                    for (Object sourceColumn : (List<?>) sourceColumns) {
                        Method getNodeId = sourceColumn.getClass().getMethod("getNodeId");
                        Method getOwnerId = sourceColumn.getClass().getMethod("getOwnerId");
                        System.out.println("TGMV_JOIN_SOURCE nodeId=" + getNodeId.invoke(sourceColumn) + " owner=" + getOwnerId.invoke(sourceColumn));
                    }
                }
            }
            if (++printed >= 20) {
                break;
            }
        }
    }

    private static void debugResultAliasT2Target(
            DefaultSparkLineageParser parser,
            LogicalPlan plan,
            String targetName
    ) throws Exception {
        Object cteRelations = invokeNoArg(plan, "cteRelations");
        if (!(cteRelations instanceof Seq)) {
            return;
        }
        List<?> relations = scalaInteropToJavaList((Seq<?>) cteRelations);
        LogicalPlan resultPlan = null;
        for (Object relation : relations) {
            String text = String.valueOf(relation);
            if (!text.startsWith("(t_result,")) {
                continue;
            }
            if (relation instanceof scala.Tuple2) {
                Object planValue = ((scala.Tuple2<?, ?>) relation)._2();
                if (planValue instanceof LogicalPlan) {
                    resultPlan = (LogicalPlan) planValue;
                    break;
                }
            }
        }
        if (resultPlan == null) {
            return;
        }

        Method buildResolutionContext = DefaultSparkLineageParser.class.getDeclaredMethod(
                "buildResolutionContext",
                LogicalPlan.class,
                LogicalPlan.class,
                LogicalPlan.class
        );
        buildResolutionContext.setAccessible(true);
        Object resolutionContext = buildResolutionContext.invoke(parser, plan, plan, plan);

        Method collectTargets = DefaultSparkLineageParser.class.getDeclaredMethod(
                "collectTargetExpressions",
                LogicalPlan.class,
                resolutionContext.getClass()
        );
        collectTargets.setAccessible(true);
        LogicalPlan aliasPlan = findSubqueryAlias(resultPlan, "t2");
        if (aliasPlan == null) {
            System.out.println("ALIAS_T2_TARGET missing alias=t2");
            return;
        }
        Object aliasTargets = collectTargets.invoke(parser, aliasPlan, resolutionContext);
        if (!(aliasTargets instanceof List)) {
            return;
        }
        for (Object target : (List<?>) aliasTargets) {
            Method getName = target.getClass().getDeclaredMethod("getName");
            Method getSourceColumns = target.getClass().getDeclaredMethod("getSourceColumns");
            Method getDependentTargets = target.getClass().getDeclaredMethod("getDependentTargets");
            getName.setAccessible(true);
            getSourceColumns.setAccessible(true);
            getDependentTargets.setAccessible(true);
            Object name = getName.invoke(target);
            if (!targetName.equals(String.valueOf(name))) {
                continue;
            }
            System.out.println("ALIAS_T2_TARGET name=" + name);
            Object sourceColumns = getSourceColumns.invoke(target);
            if (sourceColumns instanceof List) {
                for (Object sourceColumn : (List<?>) sourceColumns) {
                    Method getNodeId = sourceColumn.getClass().getMethod("getNodeId");
                    Method getOwnerId = sourceColumn.getClass().getMethod("getOwnerId");
                    System.out.println("ALIAS_T2_SOURCE nodeId=" + getNodeId.invoke(sourceColumn) + " owner=" + getOwnerId.invoke(sourceColumn));
                }
            }
            Object dependentTargets = getDependentTargets.invoke(target);
            if (dependentTargets instanceof List) {
                for (Object dependentTarget : (List<?>) dependentTargets) {
                    Method depGetName = dependentTarget.getClass().getDeclaredMethod("getName");
                    Method depGetExpression = dependentTarget.getClass().getDeclaredMethod("getExpression");
                    depGetName.setAccessible(true);
                    depGetExpression.setAccessible(true);
                    System.out.println("ALIAS_T2_DEPENDENCY name=" + depGetName.invoke(dependentTarget)
                            + " expr=" + depGetExpression.invoke(dependentTarget));
                }
            }
        }
    }

    private static void debugResultAliasT2FirstChildTarget(
            DefaultSparkLineageParser parser,
            LogicalPlan plan,
            String targetName
    ) throws Exception {
        Object cteRelations = invokeNoArg(plan, "cteRelations");
        if (!(cteRelations instanceof Seq)) {
            return;
        }
        List<?> relations = scalaInteropToJavaList((Seq<?>) cteRelations);
        LogicalPlan resultPlan = null;
        for (Object relation : relations) {
            String text = String.valueOf(relation);
            if (!text.startsWith("(t_result,")) {
                continue;
            }
            if (relation instanceof scala.Tuple2) {
                Object planValue = ((scala.Tuple2<?, ?>) relation)._2();
                if (planValue instanceof LogicalPlan) {
                    resultPlan = (LogicalPlan) planValue;
                    break;
                }
            }
        }
        if (resultPlan == null) {
            return;
        }

        Method buildResolutionContext = DefaultSparkLineageParser.class.getDeclaredMethod(
                "buildResolutionContext",
                LogicalPlan.class,
                LogicalPlan.class,
                LogicalPlan.class
        );
        buildResolutionContext.setAccessible(true);
        Object resolutionContext = buildResolutionContext.invoke(parser, plan, plan, plan);

        Method collectTargets = DefaultSparkLineageParser.class.getDeclaredMethod(
                "collectTargetExpressions",
                LogicalPlan.class,
                resolutionContext.getClass()
        );
        collectTargets.setAccessible(true);

        LogicalPlan aliasPlan = findSubqueryAlias(resultPlan, "t2");
        if (aliasPlan == null) {
            return;
        }
        List<LogicalPlan> aliasChildren = childrenOf(aliasPlan);
        if (aliasChildren.isEmpty()) {
            return;
        }
        LogicalPlan unionPlan = aliasChildren.get(0);
        List<LogicalPlan> unionChildren = childrenOf(unionPlan);
        if (unionChildren.isEmpty()) {
            return;
        }
        Object branchTargets = collectTargets.invoke(parser, unionChildren.get(0), resolutionContext);
        if (!(branchTargets instanceof List)) {
            return;
        }
        for (Object target : (List<?>) branchTargets) {
            Method getName = target.getClass().getDeclaredMethod("getName");
            Method getDependentTargets = target.getClass().getDeclaredMethod("getDependentTargets");
            getName.setAccessible(true);
            getDependentTargets.setAccessible(true);
            Object name = getName.invoke(target);
            if (!targetName.equals(String.valueOf(name))) {
                continue;
            }
            System.out.println("ALIAS_T2_BRANCH_TARGET name=" + name);
            Object dependentTargets = getDependentTargets.invoke(target);
            if (dependentTargets instanceof List) {
                for (Object dependentTarget : (List<?>) dependentTargets) {
                    Method depGetName = dependentTarget.getClass().getDeclaredMethod("getName");
                    Method depGetExpression = dependentTarget.getClass().getDeclaredMethod("getExpression");
                    depGetName.setAccessible(true);
                    depGetExpression.setAccessible(true);
                    System.out.println("ALIAS_T2_BRANCH_DEPENDENCY name=" + depGetName.invoke(dependentTarget)
                            + " expr=" + depGetExpression.invoke(dependentTarget));
                }
            }
        }
    }

    private static LogicalPlan findSubqueryAlias(LogicalPlan plan, String aliasName) throws Exception {
        if (plan == null || aliasName == null) {
            return null;
        }
        if (plan instanceof SubqueryAlias && aliasName.equalsIgnoreCase(((SubqueryAlias) plan).alias())) {
            return plan;
        }
        for (LogicalPlan child : childrenOf(plan)) {
            LogicalPlan match = findSubqueryAlias(child, aliasName);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private static Object tryInvokeCollect(Method method, DefaultSparkLineageParser parser, String label, LogicalPlan plan, Object resolutionContext) {
        try {
            Object result = method.invoke(parser, plan, resolutionContext);
            int size = result instanceof List ? ((List<?>) result).size() : -1;
            System.out.println("collectTargetExpressions " + label + " node=" + plan.nodeName() + " size=" + size);
            return result;
        } catch (Exception error) {
            Throwable cause = error.getCause() == null ? error : error.getCause();
            System.out.println("collectTargetExpressions " + label + " node=" + plan.nodeName() + " failed=" + cause);
            return null;
        }
    }

    private static void dumpTargetsWithKeyword(List<?> targets, String keyword) throws Exception {
        for (Object target : targets) {
            Method getName = target.getClass().getDeclaredMethod("getName");
            Method getExpression = target.getClass().getDeclaredMethod("getExpression");
            getName.setAccessible(true);
            getExpression.setAccessible(true);
            Object name = getName.invoke(target);
            if (name != null && String.valueOf(name).contains(keyword)) {
                Object expression = getExpression.invoke(target);
                System.out.println("TARGET_EXPR name=" + name
                        + " exprClass=" + (expression == null ? "null" : expression.getClass().getName())
                        + " exprText=" + String.valueOf(expression));
            }
        }
    }

    private static void printReverseBacktrace(NormalizedLineageResult result, String targetNodeId, int maxDepth) {
        Map<String, ExpressionNode> expressionById = new LinkedHashMap<>();
        for (ExpressionNode expressionNode : result.getExpressionNodes()) {
            expressionById.put(expressionNode.getNodeId(), expressionNode);
        }
        System.out.println("BACKTRACE start=" + targetNodeId);
        printReverseBacktrace(result.getGraphEdges(), expressionById, targetNodeId, 0, maxDepth, new HashSet<String>());
    }

    private static void printReverseBacktrace(
            List<LineageGraphEdge> edges,
            Map<String, ExpressionNode> expressionById,
            String targetNodeId,
            int depth,
            int maxDepth,
            Set<String> visiting
    ) {
        if (depth > maxDepth || !visiting.add(targetNodeId + "@" + depth)) {
            return;
        }
        try {
            for (LineageGraphEdge edge : edges) {
                if (!targetNodeId.equals(edge.getTargetNodeId())) {
                    continue;
                }
                String sourceNodeId = edge.getSourceNodeId();
                String sql = expressionById.containsKey(sourceNodeId)
                        ? expressionById.get(sourceNodeId).getExpressionSql()
                        : "";
                System.out.println("BACKTRACE_EDGE depth=" + depth
                        + " type=" + edge.getEdgeType()
                        + " role=" + edge.getRole()
                        + " source=" + sourceNodeId
                        + " target=" + edge.getTargetNodeId()
                        + (sql.isEmpty() ? "" : " sql=" + sql));
                printReverseBacktrace(edges, expressionById, sourceNodeId, depth + 1, maxDepth, visiting);
            }
        } finally {
            visiting.remove(targetNodeId + "@" + depth);
        }
    }
}
