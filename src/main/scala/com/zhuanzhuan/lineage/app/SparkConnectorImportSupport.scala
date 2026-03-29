package com.zhuanzhuan.lineage.app

import com.facebook.thrift.protocol.TCompactProtocol
import com.zhuanzhuan.lineage.storage.nebula.{NebulaImporterBundleWriter, SparkNebulaConnectorImporter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Path, Paths}
import java.util.Comparator
import java.util.concurrent.{Callable, ExecutorCompletionService, Executors}
import scala.collection.JavaConverters._

object SparkConnectorImportSupport {
  final case class ImportTask(name: String,
                              csvPath: Path,
                              fileSize: Long,
                              schema: NebulaImporterBundleWriter.CsvSchema)

  def ensureHadoopHome(): Unit = {
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

  def resolveBundleDir(args: Array[String]): Path = {
    if (args.length >= 1) {
      return Paths.get(args(0)).toAbsolutePath.normalize
    }

    val bundlesRoot = Paths.get(".nebula-importer-bundles").toAbsolutePath.normalize
    if (!Files.isDirectory(bundlesRoot)) {
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
      System.err.println(s"No bundle directory found under: $bundlesRoot")
      System.exit(1)
    }

    latestBundle.get().toAbsolutePath.normalize
  }

  def createSparkSession(appName: String, sparkMaster: String, shufflePartitions: Int): SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))

    val spark = SparkSession.builder()
      .appName(appName)
      .master(sparkMaster)
      .config(sparkConf)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.catalogImplementation", "in-memory")
      .config("spark.sql.shuffle.partitions", String.valueOf(shufflePartitions))
      .config("spark.scheduler.mode", "FAIR")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def prepareTasks(bundleDir: Path,
                   subDirectory: String,
                   schemas: java.util.List[NebulaImporterBundleWriter.CsvSchema],
                   labelPrefix: String): Seq[ImportTask] = {
    val tasks = schemas.asScala.toSeq.flatMap { schema =>
      val csvPath = bundleDir.resolve(subDirectory).resolve(schema.getName + ".csv")
      if (!Files.isRegularFile(csvPath) || Files.size(csvPath) == 0L) {
        println(s"skip=$labelPrefix:${schema.getName} reason=missing_or_empty")
        None
      } else {
        Some(ImportTask(schema.getName, csvPath, Files.size(csvPath), schema))
      }
    }.sortBy(task => -task.fileSize)

    val totalBytes = tasks.map(_.fileSize).sum
    println(
      s"import.plan label=$labelPrefix files=${tasks.size} totalBytes=$totalBytes parallelism=" +
        math.min(math.max(readInt(SparkNebulaConnectorImporter.PARALLEL_SCHEMA_PROPERTY, 4), 1), math.max(tasks.size, 1))
    )
    tasks
  }

  def runImportTasks(spark: SparkSession,
                     tasks: Seq[ImportTask],
                     requestedParallelism: Int,
                     labelPrefix: String)(runner: (SparkSession, ImportTask) => Unit): Int = {
    if (tasks.isEmpty) {
      return 0
    }

    val parallelism = math.min(math.max(requestedParallelism, 1), tasks.size)
    val executor = Executors.newFixedThreadPool(parallelism)
    val completion = new ExecutorCompletionService[ImportTask](executor)

    try {
      tasks.foreach { task =>
        completion.submit(new Callable[ImportTask] {
          override def call(): ImportTask = {
            val session = spark.newSession()
            val startedAt = System.currentTimeMillis()
            session.sparkContext.setLocalProperty("spark.scheduler.pool", s"$labelPrefix-${sanitize(task.name)}")
            try {
              println(s"import.start $labelPrefix=${task.name} path=${task.csvPath} sizeBytes=${task.fileSize}")
              runner(session, task)
              println(
                s"import.finish $labelPrefix=${task.name} durationMs=${System.currentTimeMillis() - startedAt}"
              )
              task
            } finally {
              session.sparkContext.setLocalProperty("spark.scheduler.pool", null)
            }
          }
        })
      }

      var completed = 0
      while (completed < tasks.size) {
        val finishedTask = completion.take().get()
        completed += 1
        println(s"import.progress label=$labelPrefix completed=$completed/${tasks.size} last=${finishedTask.name}")
      }
      tasks.size
    } catch {
      case error: Exception =>
        executor.shutdownNow()
        throw error
    } finally {
      executor.shutdown()
    }
  }

  def readString(key: String, defaultValue: String): String = {
    sys.props.get(key).map(_.trim).filter(_.nonEmpty).getOrElse(defaultValue)
  }

  def readInt(key: String, defaultValue: Int): Int = {
    sys.props.get(key).map(_.trim).filter(_.nonEmpty).flatMap { value =>
      try {
        Some(value.toInt)
      } catch {
        case _: NumberFormatException => None
      }
    }.getOrElse(defaultValue)
  }

  def readBoolean(key: String, defaultValue: Boolean): Boolean = {
    sys.props.get(key).map(_.trim).filter(_.nonEmpty).map(_.toBoolean).getOrElse(defaultValue)
  }

  private def sanitize(name: String): String =
    name.toLowerCase.replaceAll("[^a-z0-9]+", "_")
}
