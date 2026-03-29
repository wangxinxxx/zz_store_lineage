package com.zhuanzhuan.lineage.app

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.{NebulaConnectionConfig, WriteNebulaVertexConfig}
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.nio.file.{Files, Path, Paths}
import java.util.Comparator

object SparkConnectorDebugMain {
  private val NullToken = "__ZZ_LINEAGE_NULL__"

  def main(args: Array[String]): Unit = {
    ensureHadoopHome()
    val bundleDir = resolveBundleDir(args)
    val vertexFileName = if (args.length >= 2) args(1) else "task_node.csv"
    val tagName = if (args.length >= 3) args(2) else "task_node"
    val csvPath = bundleDir.resolve("vertices").resolve(vertexFileName)
    println(s"bundleDir=$bundleDir")
    println(s"csvPath=$csvPath")

    val sparkConf = new SparkConf()
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))

    val spark = SparkSession.builder()
      .appName("spark-connector-debug-main")
      .master("local[*]")
      .config(sparkConf)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.catalogImplementation", "in-memory")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {
      val schema = new StructType()
        .add("vid", DataTypes.StringType, nullable = false)
        .add("task_name", DataTypes.StringType)
        .add("owner", DataTypes.StringType)
        .add("script_path", DataTypes.StringType)
        .add("biz_date", DataTypes.StringType)

      val df = spark.read
        .option("header", "false")
        .option("multiLine", "true")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("nullValue", NullToken)
        .schema(schema)
        .csv(csvPath.toString)

      val timeoutMs = math.max(sys.props.getOrElse("zz.lineage.nebula.timeoutMs", "3000").toInt, 1)

      val connectionConfig = NebulaConnectionConfig
        .builder()
        .withMetaAddress(sys.props.getOrElse("zz.lineage.nebula.metaAddress", "192.168.152.128:9559"))
        .withGraphAddress(sys.props.getOrElse("zz.lineage.nebula.graphAddress", "192.168.152.128:9669"))
        .withTimeout(timeoutMs)
        .withConnectionRetry(3)
        .withExecuteRetry(3)
        .build()

      val vertexBuilder = WriteNebulaVertexConfig
        .builder()
        .withSpace(sys.props.getOrElse("zz.lineage.nebula.space", "store_lineage"))
        .withTag(tagName)
        .withVidField("vid")
        .withVidAsProp(false)
        .withBatch(sys.props.getOrElse("zz.lineage.spark.connector.batch", "1024").toInt)
        .withUser(sys.props.getOrElse("zz.lineage.nebula.username", "root"))
        .withPasswd(sys.props.getOrElse("zz.lineage.nebula.password", "nebula"))
        .withDisableWriteLog(true)
      println(s"vertexBuilder.classpath=${vertexBuilder.getClass.getProtectionDomain.getCodeSource.getLocation}")
      println(s"vertexBuilder.vidPolicy=${Option(vertexBuilder.vidPolicy).getOrElse("<null>")}")
      val vertexConfig = vertexBuilder.build()

      df.write.nebula(connectionConfig, vertexConfig).writeVertices()
    } finally {
      spark.stop()
    }
  }

  private def resolveBundleDir(args: Array[String]): Path = {
    if (args.length >= 1) {
      return Paths.get(args(0)).toAbsolutePath.normalize
    }

    val bundlesRoot = Paths.get(".nebula-importer-bundles").toAbsolutePath.normalize
    if (!Files.isDirectory(bundlesRoot)) {
      System.err.println("Usage: SparkConnectorDebugMain <bundle-directory> [vertex-file-name] [tag-name]")
      System.err.println(s"Default bundle root does not exist: $bundlesRoot")
      System.exit(1)
    }

    val latestBundle = Files.list(bundlesRoot)
      .filter(Files.isDirectory(_))
      .max(new Comparator[Path] {
        override def compare(left: Path, right: Path): Int =
          Files.getLastModifiedTime(left).compareTo(Files.getLastModifiedTime(right))
      })

    if (!latestBundle.isPresent) {
      System.err.println("Usage: SparkConnectorDebugMain <bundle-directory> [vertex-file-name] [tag-name]")
      System.err.println(s"No bundle directory found under: $bundlesRoot")
      System.exit(1)
    }

    latestBundle.get().toAbsolutePath.normalize
  }

  private def ensureHadoopHome(): Unit = {
    val configuredProperty = sys.props.get("hadoop.home.dir").map(_.trim).filter(_.nonEmpty)
    val configuredEnv = sys.env.get("HADOOP_HOME").map(_.trim).filter(_.nonEmpty)
    if (configuredProperty.isDefined || configuredEnv.isDefined) {
      return
    }

    val fallback = Paths.get("tmp", "hadoop-home").toAbsolutePath.normalize
    if (!Files.isDirectory(fallback)) {
      System.err.println(s"HADOOP_HOME is unset and fallback directory does not exist: $fallback")
      System.exit(1)
    }

    System.setProperty("hadoop.home.dir", fallback.toString)
    println(s"hadoop.home.dir=$fallback")
  }
}
