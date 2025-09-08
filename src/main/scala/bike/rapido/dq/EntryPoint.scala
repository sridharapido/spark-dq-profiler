package bike.rapido.dq

import bike.rapido.dq.notify._
import bike.rapido.dq.notify.slack.{SlackClient, WebApiSlackClient, WebhookSlackClient}
import scopt.OParser
import java.time.Instant
import java.io.File

final case class CliArgs(
    mode: String = "all",
    tables: Seq[String] = Seq.empty,
    partitionKey: Option[String] = None,
    partitions: Seq[String] = Seq.empty,
    write: Boolean = false,
    demo: Boolean = false,
    // Reader flags
    reader: Option[String] = None,
    readerConf: Map[String, String] = Map.empty,
    // Notification flags
    doNotify: Boolean = false,
    notifyLevel: Option[String] = None,
    notifyConfig: Option[String] = None,
    notifyDryRun: Boolean = false
)

object EntryPoint {
  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[CliArgs]
    val parser = {
      import builder._
      OParser.sequence(
        programName("spark-dq-profiler"),
        head("spark-dq-profiler", "0.1"),
        opt[String]("mode").action((x, c) => c.copy(mode = x)).text("availability|rules|drift|all"),
        opt[Seq[String]]("tables").valueName("t1,t2").action((x, c) => c.copy(tables = x)),
        opt[String]("partition-key").action((x, c) => c.copy(partitionKey = Some(x))),
        opt[Seq[String]]("partitions").valueName("p1,p2").action((x, c) => c.copy(partitions = x)),
        opt[Unit]("write").action((_, c) => c.copy(write = true)),
        opt[Unit]("demo").action((_, c) => c.copy(demo = true)),
        // Reader flags
        opt[String]("reader").action((x, c) => c.copy(reader = Some(x))).text("presto|catalog"),
        opt[Map[String, String]]("reader-conf").valueName("k1=v1,k2=v2").action((x, c) => c.copy(readerConf = x)).text("Reader config key/values"),
        // Notify flags
        opt[Unit]("notify").action((_, c) => c.copy(doNotify = true)).text("Enable notifications"),
        opt[String]("notify-level").action((x, c) => c.copy(notifyLevel = Some(x))).text("basic|advanced|detailed"),
        opt[String]("notify-config").action((x, c) => c.copy(notifyConfig = Some(x))).text("/path/to/config.yml"),
        opt[Unit]("notify-dry-run").action((_, c) => c.copy(notifyDryRun = true)).text("Print messages without sending")
      )
    }

    OParser.parse(parser, args, CliArgs()) match {
      case Some(conf) => run(conf)
      case _          => System.exit(1)
    }
  }

  def run(c: CliArgs): Unit = {
    // In this scaffold, we focus on the notification subsystem integration.
    val notifyEnabled = c.doNotify
    val cfgOpt = c.notifyConfig.map(ConfigLoader.load)
    val cfg = cfgOpt.getOrElse(NotifyConfig())
    val finalCfg = cfg.copy(slack = cfg.slack.copy(
      level = c.notifyLevel.map(NotificationLevel.from).getOrElse(cfg.slack.level),
      dryRun = if (c.notifyDryRun) true else cfg.slack.dryRun
    ))

    // Optional: demo read using unified reader when configured
    if (c.reader.nonEmpty || c.readerConf.nonEmpty) {
      import bike.rapido.dq.io.readers._
      implicit val spark: org.apache.spark.sql.SparkSession = if (c.demo) SparkSessionFactory.local() else SparkSessionFactory.cluster()
      val cfgMap = c.readerConf ++ c.reader.map(r => "reader" -> r).toMap
      try {
        val df = spark.read.source(cfgMap, "select 1 as ok")
        val n = df.count()
        println(s"Reader connectivity check rows=$n")
      } catch { case t: Throwable => System.err.println(s"Reader connectivity failed: ${t.getMessage}") }
    }

    if (notifyEnabled) {
      val policy = new PolicyEngine(finalCfg)
      val client: Option[SlackClient] = if (!finalCfg.slack.enabled || finalCfg.slack.dryRun) None
      else {
        (finalCfg.slack.webhookUrl, finalCfg.slack.token) match {
          case (Some(url), _) => Some(new WebhookSlackClient(url))
          case (None, Some(tok)) => Some(new WebApiSlackClient(tok))
          case _ => None
        }
      }
      val audit = new JsonFileAuditSink(new File("."))
      val dispatcher = new NotificationDispatcher(finalCfg, client, audit)

      // Gather DQ events from the run; for this scaffold, we simulate when demo is on
      val events: Seq[DqEvent] = if (c.demo) demoEvents(c.tables) else Seq.empty
      val decisions = events.map(policy.evaluate)
      dispatcher.process(decisions)
    }
  }

  private def now: Instant = Instant.now()

  private def demoEvents(tables: Seq[String]): Seq[DqEvent] = {
    val tbl = if (tables.nonEmpty) tables.head else "db.sales.orders"
    val part = Some("ds=2025-09-08")
    val ts = now
    val ev1 = new DqEvent { val behavior = Behavior.Availability; val table = tbl; val partition = part; val timestamp = ts; val metrics = Map("available" -> 0.0) }
    val ev2 = new DqEvent { val behavior = Behavior.Drift; val table = tbl; val partition = part; val timestamp = ts; val metrics = Map("drift_score" -> 0.22) }
    val ev3 = new DqEvent { val behavior = Behavior.NullRatio; val table = tbl; val partition = part; val timestamp = ts; val metrics = Map("null_ratio" -> 0.12) }
    Seq(ev1, ev2, ev3)
  }
}
