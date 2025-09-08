package bike.rapido.dq.iceberg

import bike.rapido.dq.Metadog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object IcebergWriter {
  case class Options(mode:String="append", overwritePartitions:Boolean=true)

  def alignToTable(spark: SparkSession, fqn:String, df:DataFrame): DataFrame = {
    val cols = spark.table(fqn).schema
    val dfCols = df.columns.toSet
    cols.fields.foldLeft(df){ (acc,f) => if (dfCols.contains(f.name)) acc else acc.withColumn(f.name, lit(null).cast(f.dataType)) }
  }

  def addMissingTableCols(spark: SparkSession, fqn:String, df:DataFrame): Unit = {
    val tCols = spark.table(fqn).schema.fieldNames.toSet
    val dfOnly = df.schema.fields.filter(f => !tCols.contains(f.name))
    dfOnly.foreach{ f => spark.sql(s"ALTER TABLE $fqn ADD COLUMN IF NOT EXISTS `${f.name}` ${f.dataType.sql}") }
  }

  def write(spark: SparkSession, fqn:String, df:DataFrame, opts:Options=Options()): Unit = {
    val t0 = System.nanoTime
    try {
      val exists = spark.catalog.tableExists(fqn)
      val outDf = if (exists) alignToTable(spark,fqn,df) else df
      if (exists) addMissingTableCols(spark, fqn, outDf)
      if (!exists) {
        outDf.write.mode("errorIfExists").saveAsTable(fqn)
      } else {
        val targetCols = spark.table(fqn).columns
        val ordered = outDf.select(targetCols.map(c => outDf.col(c)): _*)
        val tmp = s"__iceberg_io_tmp_${System.currentTimeMillis}__"
        ordered.createOrReplaceTempView(tmp)
        val colList = targetCols.map(c => s"`${c}`").mkString(", ")
        val sqlBody = s"SELECT ${colList} FROM ${tmp}"
        if (opts.overwritePartitions) spark.sql(s"INSERT OVERWRITE TABLE ${fqn} ${sqlBody}") else spark.sql(s"INSERT INTO ${fqn} ${sqlBody}")
        spark.catalog.dropTempView(tmp)
      }
      Metadog.counter("io.write.success", 1, Map("table"->fqn))
    } catch { case e: Throwable => Metadog.counter("io.write.failure", 1, Map("table"->fqn,"error"->e.getClass.getSimpleName)); throw e }
    finally { val ms = (System.nanoTime - t0) / 1e6d; Metadog.timer("io.write.ms", ms.toLong, Map("table"->fqn)) }
  }
}
