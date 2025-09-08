package bike.rapido.dq

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class MetadogShimTest extends AnyFunSuite {
  test("Metadog NOOP when client not on classpath") {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    try {
      Metadog.init(spark, appName = "test-app", env = "test")
      assert(Metadog.isNoop)
      Metadog.counter("x", 1)
      Metadog.gauge("y", 1.0)
      Metadog.timer("z", 10)
    } finally {
      spark.stop()
    }
  }
}
