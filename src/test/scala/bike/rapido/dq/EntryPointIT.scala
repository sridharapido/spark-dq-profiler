package bike.rapido.dq

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.SparkConf

class EntryPointIT extends AnyFunSuite with DataFrameSuiteBase {

  override def conf: SparkConf = {
    val c = super.conf
    c.set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    c.set("spark.sql.catalog.local.type", "hadoop")
    c.set("spark.sql.catalog.local.warehouse", "target/warehouse")
    c
  }

  test("EntryPoint runs availability mode with local catalog and demo") {
    val args = Array(
      "--tables", "local.obs_demo.dq_src",
      "--mode", "availability",
      "--demo"
    )
    EntryPoint.main(args)
    succeed
  }
}

