package bike.rapido.dq

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.util.Try
import scala.collection.JavaConverters._

trait MetricSink {
  def counter(name: String, delta: Long, tags: Map[String, String] = Map.empty): Unit
  def gauge(name: String, value: Double, tags: Map[String, String] = Map.empty): Unit
  def timer(name: String, millis: Long, tags: Map[String, String] = Map.empty): Unit
}

object NoopMetricSink extends MetricSink {
  def counter(name: String, delta: Long, tags: Map[String, String]): Unit = ()
  def gauge(name: String, value: Double, tags: Map[String, String]): Unit = ()
  def timer(name: String, millis: Long, tags: Map[String, String]): Unit = ()
}

object Metadog {
  private val log = LoggerFactory.getLogger(getClass)
  @volatile private var sink: MetricSink = NoopMetricSink
  @volatile private var defaultTags: Map[String, String] = Map.empty

  def init(spark: SparkSession, appName: String, env: String, defaults: Map[String, String] = Map.empty): Unit = {
    defaultTags = Map("app" -> appName, "env" -> env) ++ defaults
    val loaded = Try {
      val cls = Class.forName("com.yourorg.metadog.spark.MetadogClient")
      val builder = cls.getMethod("builder").invoke(null)
      val withSpark = builder.getClass.getMethod("withSpark", classOf[SparkSession]).invoke(builder, spark)
      val withTags = withSpark.getClass
        .getMethod("withDefaultTags", classOf[java.util.Map[String, String]])
        .invoke(withSpark, defaultTags.asJava)
      val client = withTags.getClass.getMethod("build").invoke(withTags)

      sink = new MetricSink {
        def counter(name: String, delta: Long, tags: Map[String, String]): Unit =
          client.getClass
            .getMethod(
              "counter",
              classOf[String],
              classOf[java.lang.Long],
              classOf[java.util.Map[String, String]]
            )
            .invoke(client, name, Long.box(delta), (defaultTags ++ tags).asJava)

        def gauge(name: String, value: Double, tags: Map[String, String]): Unit =
          client.getClass
            .getMethod(
              "gauge",
              classOf[String],
              classOf[java.lang.Double],
              classOf[java.util.Map[String, String]]
            )
            .invoke(client, name, Double.box(value), (defaultTags ++ tags).asJava)

        def timer(name: String, millis: Long, tags: Map[String, String]): Unit =
          client.getClass
            .getMethod(
              "timer",
              classOf[String],
              classOf[java.lang.Long],
              classOf[java.util.Map[String, String]]
            )
            .invoke(client, name, Long.box(millis), (defaultTags ++ tags).asJava)
      }
      true
    }.getOrElse(false)

    if (!loaded) log.info("Metadog not found on classpath; metrics sink is NOOP.")
  }

  def counter(name: String, delta: Long, tags: Map[String, String] = Map.empty): Unit = sink.counter(name, delta, tags)
  def gauge(name: String, value: Double, tags: Map[String, String] = Map.empty): Unit = sink.gauge(name, value, tags)
  def timer(name: String, millis: Long, tags: Map[String, String] = Map.empty): Unit = sink.timer(name, millis, tags)
  def isNoop: Boolean = sink eq NoopMetricSink
}

