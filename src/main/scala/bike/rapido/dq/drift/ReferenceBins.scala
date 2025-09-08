package bike.rapido.dq.drift

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import java.time.Instant

object ReferenceBins {
  val TableName = "iceberg.obs.dq_reference_bins"
  def ensureTable(spark: SparkSession): Unit = try spark.sql(
    s"""
       |CREATE TABLE IF NOT EXISTS iceberg.obs.dq_reference_bins (
       |  table_name STRING,
       |  column_name STRING,
       |  partition_value STRING,
       |  cuts ARRAY<DOUBLE>,
       |  created_at TIMESTAMP
       |) USING iceberg
       |PARTITIONED BY (created_at)
       |""".stripMargin) catch { case _: Throwable => () }
  def computeCuts(df: DataFrame, colName: String, bins: Int): Array[Double] = { val probs = (1 until bins).map(i => i.toDouble / bins.toDouble).toArray; df.stat.approxQuantile(colName, probs, 1e-6) }
  def saveCuts(spark: SparkSession, table: String, column: String, partitionValue: String, cuts: Array[Double]): Unit = { ensureTable(spark); try { import spark.implicits._; val now = Timestamp.from(Instant.now()); Seq((table, column, partitionValue, cuts.toSeq.map(_.toDouble), now)).toDF("table_name", "column_name", "partition_value", "cuts", "created_at").write.mode("append").saveAsTable(TableName) } catch { case _: Throwable => () } }
  def loadCuts(spark: SparkSession, table: String, column: String, partitionValue: String): Option[Array[Double]] = { ensureTable(spark); try { val rows = spark.table(TableName).filter(col("table_name") === lit(table) && col("column_name") === lit(column) && col("partition_value") === lit(partitionValue)).orderBy(col("created_at").desc).limit(1).select(col("cuts")).collect(); if (rows.nonEmpty) Some(rows.head.getAs[Seq[Double]](0).toArray) else None } catch { case _: Throwable => None } }
}

