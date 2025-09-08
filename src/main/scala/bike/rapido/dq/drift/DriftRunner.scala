package bike.rapido.dq.drift

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate

final case class DriftEvent(table: String, partition: Option[String], name: String, score: Double, severity: String, reference: String)

object DriftRunner {
  def run(spark: SparkSession, tableFqn: String, partitionKeyOpt: Option[String], partitions: Seq[String], cols: Seq[String], bins: Int = 10, psiThreshold: Double = 0.2): Seq[DriftEvent] = {
    val sample = sampleDF(spark, tableFqn, partitionKeyOpt, partitions)
    val todayPart: Option[String] = partitions.headOption
    cols.flatMap { colName => val dod = forRef(1, spark, tableFqn, partitionKeyOpt, todayPart, sample, colName, bins, psiThreshold); val wow = forRef(7, spark, tableFqn, partitionKeyOpt, todayPart, sample, colName, bins, psiThreshold); dod.toSeq ++ wow.toSeq }
  }
  private def forRef(minusDays: Int, spark: SparkSession, tableFqn: String, partitionKeyOpt: Option[String], todayPartOpt: Option[String], sample: DataFrame, colName: String, bins: Int, psiThreshold: Double): Option[DriftEvent] = {
    val refPartOpt = todayPartOpt.flatMap(p => parseDate(p).map(_.minusDays(minusDays)).map(_.toString))
    val cutsOpt = refPartOpt.flatMap { refPart => ReferenceBins.loadCuts(spark, tableFqn, colName, refPart).orElse { val refDF = refData(spark, tableFqn, partitionKeyOpt, refPart); if (refDF.isDefined && refDF.get.columns.contains(colName)) { val cuts = ReferenceBins.computeCuts(refDF.get, colName, bins); ReferenceBins.saveCuts(spark, tableFqn, colName, refPart, cuts); Some(cuts) } else None } }
    cutsOpt.map { cuts => val curHist = Histogram.histogram(sample.select(col(colName)), colName, cuts); val refHist = Histogram.histogram(refData(spark, tableFqn, partitionKeyOpt, refPartOpt.get).getOrElse(sample).select(col(colName)), colName, cuts); val score = PSI.psi(refHist.toSeq, curHist.toSeq); val severity = if (score >= psiThreshold) "high" else "medium"; DriftEvent(tableFqn, todayPartOpt, s"${colName}_psi_${if (minusDays == 1) "dod" else "wow"}", score, severity, reference = if (minusDays == 1) "yesterday" else "week") }
  }
  private def sampleDF(spark: SparkSession, tableFqn: String, partitionKeyOpt: Option[String], partitions: Seq[String]): DataFrame = { val base = spark.table(tableFqn); (partitionKeyOpt, partitions) match { case (Some(pk), ps) if ps.nonEmpty => base.filter(col(pk).isin(ps: _*)); case _ => base } }
  private def refData(spark: SparkSession, tableFqn: String, partitionKeyOpt: Option[String], partitionValue: String): Option[DataFrame] = { try { val base = spark.table(tableFqn); val df = partitionKeyOpt.map(pk => base.filter(col(pk) === lit(partitionValue))).getOrElse(base); Some(df) } catch { case _: Throwable => None } }
  private def parseDate(s: String): Option[LocalDate] = try Some(LocalDate.parse(s)) catch { case _: Throwable => None }
}

