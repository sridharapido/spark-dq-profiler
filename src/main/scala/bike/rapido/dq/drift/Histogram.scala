package bike.rapido.dq.drift

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Histogram {
  def histogram(sampleDf: DataFrame, colName: String, cuts: Array[Double]): Array[Double] = {
    val bins = cuts.sorted
    val bucketUdf = udf { v: java.lang.Double => if (v == null) bins.length else { val d = v.doubleValue(); var idx = 0; while (idx < bins.length && d > bins(idx)) idx += 1; idx } }
    val withBucket = sampleDf.select(bucketUdf(col(colName).cast("double")).as("bucket"))
    val counts = withBucket.groupBy(col("bucket")).agg(count(lit(1)).as("cnt")).collect()
    val size = bins.length + 2
    val raw = Array.fill[Long](size)(0L)
    counts.foreach { r => val b = r.getInt(0); val c = r.getLong(1); if (b >= 0 && b < size) raw(b) = c }
    val total = raw.map(_.toDouble).sum
    if (total == 0.0) raw.map(_ => 0.0) else raw.map(_.toDouble / total)
  }
}

