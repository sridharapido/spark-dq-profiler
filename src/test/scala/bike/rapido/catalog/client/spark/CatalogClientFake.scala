package bike.rapido.catalog.client.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

class CatalogClientFake(private val spark: SparkSession) {
  def loadDfFromPrestoQuery(sql: String): DataFrame = {
    import spark.implicits._
    Seq(1).toDF("ok")
  }
}

object CatalogClientFake {
  def using(spark: SparkSession, conf: java.util.Map[String,String]): CatalogClientFake = new CatalogClientFake(spark)
}
