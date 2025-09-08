package bike.rapido.dq

import java.time.LocalDate

final case class MetricPoint(
    table: String,
    partition: Option[String],
    metricDate: LocalDate,
    name: String,
    value: Double,
    details: Map[String, String] = Map.empty
)

final case class RuleResult(
    id: String,
    table: String,
    partition: Option[String],
    passed: Boolean,
    failRate: Double,
    sampledRows: Long,
    severity: String = "medium",
    examples: Seq[String] = Nil,
    observed: Option[String] = None,
    expected: Option[String] = None
)

final case class AppConfig(
    catalog: Option[String],
    schema: String,
    tables: List[String],
    partitionKey: Option[String],
    sampleFraction: Double = 0.01,
    slaHours: Int = 6,
    dimsForSegments: List[String] = List("geo_value", "time_value"),
    alertNullRateThreshold: Double = 0.2,
    alertDoDDeltaPct: Double = 0.10
)

object AppConfig {
  import com.typesafe.config.ConfigFactory

  def load(namespace: String = "dq"): AppConfig = {
    val cfg = ConfigFactory.load()
    val c   = if (cfg.hasPath(namespace)) cfg.getConfig(namespace) else cfg

    AppConfig(
      catalog = if (c.hasPath("catalog")) Option(c.getString("catalog")).filter(_.nonEmpty) else None,
      schema = c.getString("schema"),
      tables = if (c.hasPath("tables")) c.getStringList("tables").toArray(new Array[String](0)).toList else Nil,
      partitionKey = if (c.hasPath("partitionKey")) Option(c.getString("partitionKey")).filter(_.nonEmpty) else None,
      sampleFraction = if (c.hasPath("sampleFraction")) c.getDouble("sampleFraction") else 0.01,
      slaHours = if (c.hasPath("slaHours")) c.getInt("slaHours") else 6,
      dimsForSegments =
        if (c.hasPath("dimsForSegments")) c.getStringList("dimsForSegments").toArray(new Array[String](0)).toList
        else List("geo_value", "time_value"),
      alertNullRateThreshold = if (c.hasPath("alertNullRateThreshold")) c.getDouble("alertNullRateThreshold") else 0.2,
      alertDoDDeltaPct = if (c.hasPath("alertDoDDeltaPct")) c.getDouble("alertDoDDeltaPct") else 0.10
    )
  }
}

object SparkSessionFactory {
  import org.apache.spark.sql.SparkSession

  def local(appName: String = "spark-dq-profiler"): SparkSession =
    SparkSession.builder().appName(appName).master("local[*]").config("spark.ui.enabled", "false").getOrCreate()

  def cluster(appName: String = "spark-dq-profiler"): SparkSession =
    SparkSession.builder().appName(appName).getOrCreate()
}

