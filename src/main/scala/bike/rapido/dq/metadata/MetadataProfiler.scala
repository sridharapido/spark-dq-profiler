package bike.rapido.dq.metadata

import bike.rapido.dq.{MetricPoint, RuleResult}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.time.{Duration, Instant, LocalDate}

object MetadataProfiler {
  def availability(spark: SparkSession, tableFqn: String, partitionKey: Option[String], expectedPartitions: Seq[String]): (Seq[MetricPoint], Seq[RuleResult]) = {
    val df = spark.table(s"$tableFqn$$partitions")
    partitionKey match {
      case Some(pk) if expectedPartitions.nonEmpty =>
        val rcByPart = df.groupBy(col(pk).as("partition")).agg(coalesce(sum(col("record_count").cast("long")), lit(0L)).as("record_count"))
        val rows = expectedPartitions.map { p =>
          val count = rcByPart.filter(col("partition") === lit(p)).select(coalesce(col("record_count"), lit(0L)).cast("long")).collect().headOption.map(_.getLong(0)).getOrElse(0L)
          val mpCount = MetricPoint(tableFqn, Some(p), LocalDate.now(), "partition_row_count", count.toDouble, Map("partitionKey" -> pk))
          val mpExist = MetricPoint(tableFqn, Some(p), LocalDate.now(), "partition_exists", if (count > 0) 1.0 else 0.0, Map("partitionKey" -> pk))
          val rr = if (count <= 0) Some(RuleResult("availability", tableFqn, Some(p), passed = false, 1.0, count, severity = "high", observed = Some("record_count<=0"), expected = Some("record_count>0"))) else None
          (Seq(mpCount, mpExist), rr.toSeq)
        }
        val (mps, rrs) = rows.unzip; (mps.flatten, rrs.flatten)
      case _ => (Seq.empty, Seq.empty)
    }
  }

  def freshness(spark: SparkSession, tableFqn: String, slaHours: Int): (Seq[MetricPoint], Seq[RuleResult]) = {
    val latest = spark.table(s"$tableFqn$$history").orderBy(desc("made_current_at")).limit(1)
    val latestRows = latest.limit(1).collect()
    if (latestRows.isEmpty) (Seq.empty, Seq(RuleResult("freshness", tableFqn, None, passed = false, 1.0, 0L, severity = "high", observed = Some("no_history"), expected = Some("recent_snapshot"))))
    else {
      val ts = latestRows.head.getAs[java.sql.Timestamp](0)
      val ageHours = Duration.between(ts.toInstant, Instant.now()).toHours
      val mp = MetricPoint(tableFqn, None, LocalDate.now(), "snapshot_age_hours", ageHours.toDouble)
      val rr = if (ageHours > slaHours) Seq(RuleResult("freshness", tableFqn, None, passed = false, 1.0, 1L, severity = "high", observed = Some(s"ageHours=$ageHours"), expected = Some(s"<= $slaHours"))) else Nil
      (Seq(mp), rr)
    }
  }

  def rowCountDoDWoW(spark: SparkSession, tableFqn: String, partitionKey: String): Seq[MetricPoint] = {
    val df = spark.table(s"$tableFqn$$partitions").select(col(partitionKey).cast("string").as("partition"), col("record_count").cast("double").as("record_count"))
    val win = Window.orderBy(col("partition").desc)
    val withLags = df.withColumn("rc_lag1", lag(col("record_count"), 1).over(win)).withColumn("rc_lag7", lag(col("record_count"), 7).over(win)).limit(8)
    withLags.collect().toSeq.flatMap { r =>
      val part = Option(r.getAs[String]("partition")).getOrElse("")
      val date = try LocalDate.parse(part) catch { case _: Throwable => LocalDate.now() }
      val rc = r.getAs[Double]("record_count")
      val d1 = Option(r.getAs[Double]("rc_lag1"))
      val d7 = Option(r.getAs[Double]("rc_lag7"))
      val dod = d1.map(prev => MetricPoint(tableFqn, Some(part), date, "row_count_dod_delta", if (prev == 0.0) 0.0 else (rc - prev) / prev, Map("partitionKey" -> partitionKey)))
      val wow = d7.map(prev => MetricPoint(tableFqn, Some(part), date, "row_count_wow_delta", if (prev == 0.0) 0.0 else (rc - prev) / prev, Map("partitionKey" -> partitionKey)))
      dod.toSeq ++ wow.toSeq
    }
  }
}

