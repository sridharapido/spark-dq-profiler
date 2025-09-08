package bike.rapido.dq.rules

import bike.rapido.dq.RuleResult
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SimpleRules {
  def sampleDF(spark: SparkSession, tableFqn: String, partitionKeyOpt: Option[String], partitions: Seq[String], frac: Double = 0.01): DataFrame = {
    val base = spark.table(tableFqn)
    val filtered = (partitionKeyOpt, partitions) match {
      case (Some(pk), ps) if ps.nonEmpty => base.filter(col(pk).isin(ps: _*))
      case _                             => base
    }
    filtered.sample(withReplacement = false, fraction = frac, seed = 42L).cache()
  }

  def pkDuplicates(sample: DataFrame, pkCols: Seq[String], tableFqn: String, partitionOpt: Option[String]): RuleResult = {
    if (pkCols.isEmpty) return RuleResult("pk_duplicates", tableFqn, partitionOpt, passed = true, 0.0, 0L, observed = Some("no_pk_cols"), expected = Some("unique"))
    val total = sample.count()
    if (total == 0) return RuleResult("pk_duplicates", tableFqn, partitionOpt, passed = true, 0.0, 0L)
    val keyCol = concat_ws("||", pkCols.map(col): _*)
    val acd = sample.agg(approx_count_distinct(keyCol).as("acd")).first().getLong(0)
    val dupRate = 1.0 - (acd.toDouble / total.toDouble)
    val pass = dupRate == 0.0
    RuleResult("pk_duplicates", tableFqn, partitionOpt, passed = pass, failRate = if (pass) 0.0 else dupRate, sampledRows = total, severity = if (pass) "medium" else "high", observed = Some(f"dupRate=$dupRate%.5f acd=$acd total=$total"), expected = Some("dupRate==0"))
  }

  def segmentNulls(sample: DataFrame, colName: String, dims: Seq[String], threshold: Double, tableFqn: String): Seq[RuleResult] = {
    val presentDims = dims.filter(sample.columns.contains)
    presentDims.flatMap { d =>
      val by = sample.groupBy(col(d).as("segment")).agg((sum(when(col(colName).isNull, lit(1)).otherwise(lit(0))).cast("double") / count(lit(1)).cast("double")).as("null_rate"), count(lit(1)).as("rows")).na.fill(Map("segment" -> "<null>"))
      by.collect().toSeq.map { r =>
        val seg = Option(r.getAs[Any]("segment")).map(_.toString).getOrElse("<null>")
        val rate = r.getAs[Double]("null_rate")
        val rows = r.getAs[Long]("rows")
        val pass = rate <= threshold
        RuleResult(s"null_rate_${colName}_by_$d", tableFqn, None, passed = pass, failRate = if (pass) 0.0 else rate, sampledRows = rows, severity = if (pass) "medium" else "high", examples = if (pass) Nil else Seq(seg), observed = Some(f"rate=$rate%.4f"), expected = Some(s"<= $threshold"))
      }
    }
  }
}

