package com.zhuanzhuan.lineage.app

import com.vesoft.nebula.connector.{NebulaConnectionConfig, WriteNebulaVertexConfig}
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.zhuanzhuan.lineage.storage.nebula.{NebulaGraphConfig, NebulaImporterBundleWriter, SparkNebulaConnectorImporter}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import java.nio.file.Path
import scala.collection.JavaConverters._

object SparkConnectorImportVerticesMain {
  def main(args: Array[String]): Unit = {
    SparkConnectorImportSupport.ensureHadoopHome()
    val bundleDir = SparkConnectorImportSupport.resolveBundleDir(args)
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

    println(s"bundleDir=$bundleDir")

    val spark = SparkConnectorImportSupport.createSparkSession(
      appName = "spark-connector-import-vertices-main",
      sparkMaster = sparkMaster,
      shufflePartitions = shufflePartitions
    )

    try {
      val connectionConfig = NebulaConnectionConfig
        .builder()
        .withMetaAddress(graphConfig.getMetaAddress)
        .withGraphAddress(graphConfig.getGraphAddress)
        .withTimeout(timeoutMs)
        .withConnectionRetry(connectionRetry)
        .withExecuteRetry(executionRetry)
        .build()

      val tasks = SparkConnectorImportSupport.prepareTasks(
        bundleDir,
        NebulaImporterBundleWriter.VERTICES_DIR_NAME,
        NebulaImporterBundleWriter.vertexSchemas(),
        "tag"
      )

      val importedFiles = SparkConnectorImportSupport.runImportTasks(
        spark,
        tasks,
        parallelSchemas,
        "tag"
      ) { (taskSpark, task) =>
        val df = loadCsv(taskSpark, task.csvPath, vertexStruct(task.schema), repartition)
        val vertexConfig = WriteNebulaVertexConfig
          .builder()
          .withSpace(graphConfig.getSpace)
          .withTag(task.name)
          .withVidField("vid")
          .withVidAsProp(false)
          .withBatch(batchSize)
          .withUser(graphConfig.getUsername)
          .withPasswd(graphConfig.getPassword)
          .withDisableWriteLog(disableWriteLog)
          .withOverwrite(overwrite)
          .build()
        df.write.nebula(connectionConfig, vertexConfig).writeVertices()
      }

      println(s"importedVertexFiles=$importedFiles")
    } finally {
      spark.stop()
    }
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

  private def vertexStruct(schema: NebulaImporterBundleWriter.CsvSchema): StructType = {
    val fields = new java.util.ArrayList[StructField]()
    fields.add(DataTypes.createStructField("vid", DataTypes.StringType, false))
    for (property <- schema.getProperties.asScala) {
      fields.add(DataTypes.createStructField(property.getName, sparkType(property.getType), true))
    }
    DataTypes.createStructType(fields)
  }

  private def sparkType(nebulaType: String): DataType = {
    if (nebulaType == null) {
      return DataTypes.StringType
    }
    nebulaType.trim.toLowerCase match {
      case "int"               => DataTypes.IntegerType
      case "timestamp" | "long" => DataTypes.LongType
      case _                   => DataTypes.StringType
    }
  }
}
