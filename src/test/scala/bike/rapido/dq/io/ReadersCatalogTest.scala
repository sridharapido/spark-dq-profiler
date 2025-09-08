package bike.rapido.dq.io

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import bike.rapido.dq.io.readers._

class ReadersCatalogTest extends AnyFunSuite {
  test("catalog reader uses reflection client and returns a DataFrame") {
    implicit val spark: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    try {
      val cfg = Map(
        "reader" -> "catalog",
        "catalog.atlas.baseurls" -> "http://atlas:21000",
        "catalog.atlas.username" -> "user",
        "catalog.atlas.password" -> "secret",
        "catalog.tag" -> "default",
        "catalog.client.class" -> "bike.rapido.catalog.client.spark.CatalogClientFake"
      )
      val df = spark.read.source(cfg, "select 1 as ok")
      assert(df.count() == 1)
    } finally {
      spark.stop()
    }
  }
}
