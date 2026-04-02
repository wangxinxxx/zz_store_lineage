package com.zhuanzhuan.lineage.app


import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{NebulaConnectionConfig, WriteNebulaEdgeConfig, WriteNebulaVertexConfig}
import com.zhuanzhuan.lineage.storage.nebula.{NebulaGraphConfig, NebulaImporterBundleWriter, SparkNebulaConnectorImporter}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import java.nio.file.{Files, Path, Paths}
import scala.collection.JavaConverters._

object ImportNebulaDemo {
  private val bundleDir = Paths.get(".nebula-importer-bundles/1775130710409-dw_trade_sale_store_pro_retail_o_94b34f7a")
  private val importVertices = true
  private val importEdges = true

  def main(args: Array[String]): Unit = {
    SparkConnectorImportSupport.ensureHadoopHome()

    if (!Files.isDirectory(bundleDir)) {
      throw new IllegalArgumentException(s"Bundle directory does not exist: $bundleDir")
    }

    val graphConfig = NebulaGraphConfig.fromSystem()
    val timeoutMs = math.max(graphConfig.getTimeoutMs, 10000)
    val batchSize = math.max(SparkConnectorImportSupport.readInt(SparkNebulaConnectorImporter.BATCH_PROPERTY, 1024), 1)
    val repartition = math.max(SparkConnectorImportSupport.readInt(SparkNebulaConnectorImporter.REPARTITION_PROPERTY, 8), 1)
    val shufflePartitions = math.max(
      SparkConnectorImportSupport.readInt(SparkNebulaConnectorImporter.SHUFFLE_PARTITIONS_PROPERTY, repartition),
      1
    )
    val parallelSchemas = math.max(
      SparkConnectorImportSupport.readInt(SparkNebulaConnectorImporter.PARALLEL_SCHEMA_PROPERTY, 4),
      1
    )
    val sparkMaster = SparkConnectorImportSupport.readString(SparkNebulaConnectorImporter.SPARK_MASTER_PROPERTY, "local[*]")
    val connectionRetry = math.max(
      SparkConnectorImportSupport.readInt(SparkNebulaConnectorImporter.CONNECTION_RETRY_PROPERTY, 3),
      0
    )
    val executionRetry = math.max(
      SparkConnectorImportSupport.readInt(SparkNebulaConnectorImporter.EXECUTION_RETRY_PROPERTY, 3),
      0
    )
    val disableWriteLog = SparkConnectorImportSupport.readBoolean(
      SparkNebulaConnectorImporter.DISABLE_WRITE_LOG_PROPERTY,
      defaultValue = true
    )
    val overwrite = SparkConnectorImportSupport.readBoolean(
      SparkNebulaConnectorImporter.OVERWRITE_PROPERTY,
      defaultValue = true
    )

    val spark = SparkConnectorImportSupport.createSparkSession(
      appName = "import-nebula-demo",
      sparkMaster = sparkMaster,
      shufflePartitions = shufflePartitions
    )

    try {
      SparkNebulaConnectorImporter.ensureSchemaReady(graphConfig, bundleDir)

      val connectionConfig = NebulaConnectionConfig
        .builder()
        .withMetaAddress(graphConfig.getMetaAddress)
        .withGraphAddress(graphConfig.getGraphAddress)
        .withTimeout(timeoutMs)
        .withConnectionRetry(connectionRetry)
        .withExecuteRetry(executionRetry)
        .build()

      val vertexCommonConfig = VertexCommonConfig(
        space = graphConfig.getSpace,
        batch = batchSize,
        user = graphConfig.getUsername,
        password = graphConfig.getPassword,
        disableWriteLog = disableWriteLog,
        overwrite = overwrite
      )

      val edgeCommonConfig = EdgeCommonConfig(
        space = graphConfig.getSpace,
        batch = batchSize,
        user = graphConfig.getUsername,
        password = graphConfig.getPassword,
        disableWriteLog = disableWriteLog,
        overwrite = overwrite
      )

      if (importVertices) {
        val vertexTasks = prepareTasks(
          bundleDir,
          NebulaImporterBundleWriter.VERTICES_DIR_NAME,
          NebulaImporterBundleWriter.vertexSchemas(),
          "vertex"
        )
        validateCompatibility(analyzeVertexTasks(vertexTasks, vertexCommonConfig))
        val importedVertexFiles = SparkConnectorImportSupport.runImportTasks(
          spark,
          vertexTasks,
          parallelSchemas,
          "vertex"
        ) { (taskSpark, task) =>
          val dataFrame = loadCsv(taskSpark, task.csvPath, vertexStruct(task.schema), repartition)
          val vertexConfig = WriteNebulaVertexConfig
            .builder()
            .withSpace(vertexCommonConfig.space)
            .withTag(task.name)
            .withVidField(vertexCommonConfig.vidField)
            .withVidAsProp(false)
            .withBatch(vertexCommonConfig.batch)
            .withUser(vertexCommonConfig.user)
            .withPasswd(vertexCommonConfig.password)
            .withDisableWriteLog(vertexCommonConfig.disableWriteLog)
            .withOverwrite(vertexCommonConfig.overwrite)
            .build()
          dataFrame.write.nebula(connectionConfig, vertexConfig).writeVertices()
        }
        println(s"import.vertices.finish files=$importedVertexFiles")
      }

      if (importEdges) {
        val edgeTasks = prepareTasks(
          bundleDir,
          NebulaImporterBundleWriter.EDGES_DIR_NAME,
          NebulaImporterBundleWriter.edgeSchemas(),
          "edge"
        )
        validateCompatibility(analyzeEdgeTasks(edgeTasks, edgeCommonConfig))
        val importedEdgeFiles = SparkConnectorImportSupport.runImportTasks(
          spark,
          edgeTasks,
          parallelSchemas,
          "edge"
        ) { (taskSpark, task) =>
          val dataFrame = loadCsv(taskSpark, task.csvPath, edgeStruct(task.schema), repartition)
          val edgeConfig = WriteNebulaEdgeConfig
            .builder()
            .withSpace(edgeCommonConfig.space)
            .withEdge(task.name)
            .withSrcIdField(edgeCommonConfig.srcField)
            .withDstIdField(edgeCommonConfig.dstField)
            .withRankField(edgeCommonConfig.rankField)
            .withSrcAsProperty(false)
            .withDstAsProperty(false)
            .withRankAsProperty(false)
            .withBatch(edgeCommonConfig.batch)
            .withUser(edgeCommonConfig.user)
            .withPasswd(edgeCommonConfig.password)
            .withDisableWriteLog(edgeCommonConfig.disableWriteLog)
            .withOverwrite(edgeCommonConfig.overwrite)
            .build()
          dataFrame.write.nebula(connectionConfig, edgeConfig).writeEdges()
        }
        println(s"import.edges.finish files=$importedEdgeFiles")
      }
    } finally {
      spark.stop()
    }
  }

  private def prepareTasks(basePath: Path,
                           subDirectory: String,
                           schemas: java.util.List[NebulaImporterBundleWriter.CsvSchema],
                           labelPrefix: String): Seq[SparkConnectorImportSupport.ImportTask] = {
    val dataDir = resolveDataDir(basePath, subDirectory)
    val tasks = schemas.asScala.toSeq.flatMap { schema =>
      val csvPath = dataDir.resolve(schema.getName + ".csv")
      if (!Files.isRegularFile(csvPath) || Files.size(csvPath) == 0L) {
        println(s"skip=$labelPrefix:${schema.getName} reason=missing_or_empty")
        None
      } else {
        Some(SparkConnectorImportSupport.ImportTask(schema.getName, csvPath, Files.size(csvPath), schema))
      }
    }.sortBy(task => -task.fileSize)

    val totalBytes = tasks.map(_.fileSize).sum
    println(s"import.dir label=$labelPrefix path=$dataDir")
    println(
      s"import.plan label=$labelPrefix files=${tasks.size} totalBytes=$totalBytes parallelism=" +
        math.min(math.max(SparkConnectorImportSupport.readInt(SparkNebulaConnectorImporter.PARALLEL_SCHEMA_PROPERTY, 4), 1), math.max(tasks.size, 1))
    )
    tasks
  }

  private def resolveDataDir(basePath: Path, subDirectory: String): Path = {
    val normalizedBasePath = basePath.toAbsolutePath.normalize
    val currentName = Option(normalizedBasePath.getFileName).map(_.toString).getOrElse("")
    if (currentName == subDirectory) {
      return normalizedBasePath
    }

    val nestedDir = normalizedBasePath.resolve(subDirectory)
    if (Files.isDirectory(nestedDir)) {
      return nestedDir
    }

    throw new IllegalArgumentException(
      s"Cannot resolve $subDirectory directory from path: $normalizedBasePath"
    )
  }

  private def loadCsv(spark: SparkSession, csvPath: Path, schema: StructType, repartition: Int): Dataset[Row] = {
    val dataFrame = spark.read
      .option("header", "false")
      .option("multiLine", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("nullValue", NebulaImporterBundleWriter.NULL_TOKEN)
      .schema(schema)
      .csv(csvPath.toString)
    dataFrame.repartition(repartition)
  }

  private def analyzeVertexTasks(tasks: Seq[SparkConnectorImportSupport.ImportTask],
                                 commonConfig: VertexCommonConfig): CompatibilitySummary = {
    val incompatible = tasks.collect {
      case task if vertexColumnNames(task.schema).headOption != Some(commonConfig.vidField) =>
        s"${task.name}: vidField expected=${commonConfig.vidField} actual=${vertexColumnNames(task.schema).headOption.getOrElse("<missing>")}"
    }
    CompatibilitySummary(
      kind = "vertex",
      sharedParameters =
        s"space=${commonConfig.space}, vidField=${commonConfig.vidField}, batch=${commonConfig.batch}, user=${commonConfig.user}, " +
          s"disableWriteLog=${commonConfig.disableWriteLog}, overwrite=${commonConfig.overwrite}",
      varyingParameters = "tag, schema(columns after vid)",
      compatible = incompatible.isEmpty,
      details = incompatible
    )
  }

  private def analyzeEdgeTasks(tasks: Seq[SparkConnectorImportSupport.ImportTask],
                               commonConfig: EdgeCommonConfig): CompatibilitySummary = {
    val incompatible = tasks.collect {
      case task if edgeColumnNames(task.schema).take(3) != Seq(commonConfig.srcField, commonConfig.dstField, commonConfig.rankField) =>
        s"${task.name}: edge fields expected=${Seq(commonConfig.srcField, commonConfig.dstField, commonConfig.rankField).mkString(",")} actual=${edgeColumnNames(task.schema).take(3).mkString(",")}"
    }
    CompatibilitySummary(
      kind = "edge",
      sharedParameters =
        s"space=${commonConfig.space}, srcField=${commonConfig.srcField}, dstField=${commonConfig.dstField}, rankField=${commonConfig.rankField}, " +
          s"batch=${commonConfig.batch}, user=${commonConfig.user}, disableWriteLog=${commonConfig.disableWriteLog}, overwrite=${commonConfig.overwrite}",
      varyingParameters = "edgeName, schema(columns after src/dst/rank)",
      compatible = incompatible.isEmpty,
      details = incompatible
    )
  }

  private def validateCompatibility(summary: CompatibilitySummary): Unit = {
    println(s"import.compatibility kind=${summary.kind} compatible=${summary.compatible}")
    println(s"import.compatibility.shared kind=${summary.kind} ${summary.sharedParameters}")
    println(s"import.compatibility.varying kind=${summary.kind} ${summary.varyingParameters}")
    if (!summary.compatible) {
      throw new IllegalStateException(
        s"Incompatible ${summary.kind} import parameters: ${summary.details.mkString("; ")}"
      )
    }
  }

  private def vertexStruct(schema: NebulaImporterBundleWriter.CsvSchema): StructType = {
    val fields = new java.util.ArrayList[StructField]()
    fields.add(DataTypes.createStructField("vid", DataTypes.StringType, false))
    for (property <- schema.getProperties.asScala) {
      fields.add(DataTypes.createStructField(property.getName, sparkType(property.getType), true))
    }
    DataTypes.createStructType(fields)
  }

  private def edgeStruct(schema: NebulaImporterBundleWriter.CsvSchema): StructType = {
    val fields = new java.util.ArrayList[StructField]()
    fields.add(DataTypes.createStructField("src", DataTypes.StringType, false))
    fields.add(DataTypes.createStructField("dst", DataTypes.StringType, false))
    fields.add(DataTypes.createStructField("rank", DataTypes.LongType, false))
    for (property <- schema.getProperties.asScala) {
      fields.add(DataTypes.createStructField(property.getName, sparkType(property.getType), true))
    }
    DataTypes.createStructType(fields)
  }

  private def vertexColumnNames(schema: NebulaImporterBundleWriter.CsvSchema): Seq[String] =
    Seq("vid") ++ schema.getProperties.asScala.map(_.getName)

  private def edgeColumnNames(schema: NebulaImporterBundleWriter.CsvSchema): Seq[String] =
    Seq("src", "dst", "rank") ++ schema.getProperties.asScala.map(_.getName)

  private def sparkType(nebulaType: String): DataType = {
    if (nebulaType == null) {
      return DataTypes.StringType
    }
    nebulaType.trim.toLowerCase match {
      case "int"                => DataTypes.IntegerType
      case "bool" | "boolean"   => DataTypes.BooleanType
      case "timestamp" | "long" => DataTypes.LongType
      case _                    => DataTypes.StringType
    }
  }

  private final case class VertexCommonConfig(space: String,
                                              vidField: String = "vid",
                                              batch: Int,
                                              user: String,
                                              password: String,
                                              disableWriteLog: Boolean,
                                              overwrite: Boolean)

  private final case class EdgeCommonConfig(space: String,
                                            srcField: String = "src",
                                            dstField: String = "dst",
                                            rankField: String = "rank",
                                            batch: Int,
                                            user: String,
                                            password: String,
                                            disableWriteLog: Boolean,
                                            overwrite: Boolean)

  private final case class CompatibilitySummary(kind: String,
                                                sharedParameters: String,
                                                varyingParameters: String,
                                                compatible: Boolean,
                                                details: Seq[String])
}
