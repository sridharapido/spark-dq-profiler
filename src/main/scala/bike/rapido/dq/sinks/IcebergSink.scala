package bike.rapido.dq.sinks

import bike.rapido.dq.{MetricPoint, RuleResult}
import bike.rapido.dq.drift.DriftEvent
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.Source

class IcebergSink(spark: SparkSession) {
  private val metricsTable = "iceberg.obs.dq_metrics_daily"
  private val rulesTable   = "iceberg.obs.dq_rule_results"
  private val driftTable   = "iceberg.obs.dq_anomaly_events"

  // Best-effort ensure tables exist (DDL in resources); ignore if catalog not configured
  ensureTables()
  private def ensureTables(): Unit = {
    val files = Seq(
      "sql/dq_metrics_daily.sql",
      "sql/dq_rule_results.sql",
      "sql/dq_anomaly_events.sql"
    )
    files.foreach { path =>
      Option(getClass.getClassLoader.getResourceAsStream(path))
        .map(is => Source.fromInputStream(is, "UTF-8").mkString)
        .foreach { ddl => try spark.sql(ddl) catch { case _: Throwable => () } }
    }
  }

  def writeMetrics(ds: Dataset[MetricPoint]): Unit = {
    val df = ds.toDF().select(col("table").as("table_name"), col("partition").as("partition_value"), col("metricDate").as("metric_date"), col("name").as("metric_name"), col("value").as("metric_value"), col("details"))
    try df.write.mode("append").saveAsTable(metricsTable) catch { case _: Throwable => () }
  }
  def writeRules(ds: Dataset[RuleResult]): Unit = {
    val df = ds.toDF().withColumn("ts", current_timestamp()).select(col("table").as("table_name"), col("partition").as("partition_value"), col("id").as("rule_id"), col("passed"), col("failRate").as("fail_rate"), col("sampledRows").as("sampled_rows"), col("severity"), col("observed"), col("expected"), col("examples"), col("ts"))
    try df.write.mode("append").saveAsTable(rulesTable) catch { case _: Throwable => () }
  }
  def writeDrift(ds: Dataset[DriftEvent]): Unit = {
    val df = ds.toDF().withColumn("ts", current_timestamp()).withColumn("details", map()).select(col("table").as("table_name"), col("partition").as("partition_value"), col("name").as("metric_name"), col("score"), col("severity"), col("reference"), col("details"), col("ts"))
    try df.write.mode("append").saveAsTable(driftTable) catch { case _: Throwable => () }
  }
}
