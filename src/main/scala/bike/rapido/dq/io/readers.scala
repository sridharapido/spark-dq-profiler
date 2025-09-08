package bike.rapido.dq.io

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import scala.collection.JavaConverters._

object readers {
  implicit class CustomReader(val dfr: DataFrameReader) extends AnyVal {
    def source(config: Map[String, String], query: String)(implicit spark: SparkSession): DataFrame = {
      val safe = config - "catalog.atlas.password"
      println(s"CustomReader.config = ${safe.toList}")
      println(s"CustomReader.query = $query")
      config.getOrElse("reader", "presto").toLowerCase match {
        case "presto"  => presto(
          jdbcUrl     = config("presto.jdbc.url"),
          jdbcUser    = config("presto.jdbc.username"),
          query       = s"( $query ) q",
          jdbcDriver  = config.getOrElse("presto.jdbc.driver", "io.prestosql.jdbc.PrestoDriver")
        )
        case "catalog" => catalog(
          atlasBaseUrl = config("catalog.atlas.baseurls"),
          atlasUser    = config("catalog.atlas.username"),
          atlasPass    = config("catalog.atlas.password"),
          catalogTag   = config.getOrElse("catalog.tag", "default"),
          query        = query,
          clientClass  = config.getOrElse("catalog.client.class", "bike.rapido.catalog.client.spark.CatalogClient")
        )
        case other     => throw new IllegalArgumentException(s"Unsupported reader: $other")
      }
    }

    def presto(jdbcUrl: String, jdbcUser: String, query: String, jdbcDriver: String = "io.prestosql.jdbc.PrestoDriver"): DataFrame = {
      import java.util.Properties
      val props = new Properties(); props.setProperty("user", jdbcUser)
      dfr.option("driver", jdbcDriver).jdbc(url = jdbcUrl, table = query, properties = props)
    }

    def catalog(atlasBaseUrl: String,
                atlasUser: String,
                atlasPass: String,
                catalogTag: String,
                query: String,
                clientClass: String)
               (implicit spark: SparkSession): DataFrame = {
      val conn = Map(
        "catalog.atlas.baseurls" -> atlasBaseUrl,
        "catalog.atlas.username" -> atlasUser,
        "catalog.atlas.password" -> atlasPass,
        "catalog.tag"            -> catalogTag,
        "catalog.spark.dfLoadStrategy" -> "TEMP_VIEW",
        "catalog.spark.globalSqlStatementCachingEnabled" -> "true"
      ).asJava

      val cls = Class.forName(clientClass)
      val using = cls.getMethod("using", classOf[SparkSession], classOf[java.util.Map[String, String]])
      val client = using.invoke(null, spark, conn)
      val load = client.getClass.getMethod("loadDfFromPrestoQuery", classOf[String])
      load.invoke(client, query).asInstanceOf[org.apache.spark.sql.DataFrame]
    }
  }
}

