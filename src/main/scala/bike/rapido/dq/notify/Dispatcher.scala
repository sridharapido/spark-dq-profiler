package bike.rapido.dq.notify

import bike.rapido.dq.notify.slack.{SlackClient, SlackMessage, SlackRenderer}
import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.time.{Instant, ZoneId}

trait Sleeper { def sleep(ms: Long): Unit }
object RealSleeper extends Sleeper { override def sleep(ms: Long): Unit = Thread.sleep(ms) }

final case class AuditRecord(
    key: String,
    level: String,
    severity: String,
    table: String,
    behavior: String,
    status: String,
    reason: String,
    timestamp: String
)

trait AuditSink { def write(rec: AuditRecord): Unit }

class JsonFileAuditSink(baseDir: File) extends AuditSink {
  private val day = java.time.LocalDate.now(ZoneId.of("UTC")).toString.replace("-", "")
  private val file = new File(baseDir, s"out/${day}/alerts_audit.ndjson")
  file.getParentFile.mkdirs()
  override def write(rec: AuditRecord): Unit = synchronized {
    val json = JsonUtil.obj(
      "key" -> rec.key,
      "level" -> rec.level,
      "severity" -> rec.severity,
      "table" -> rec.table,
      "behavior" -> rec.behavior,
      "status" -> rec.status,
      "reason" -> rec.reason,
      "timestamp" -> rec.timestamp
    ) + "\n"
    val fos = new FileOutputStream(file, true)
    fos.write(json.getBytes(StandardCharsets.UTF_8))
    fos.close()
  }
}

class NotificationDispatcher(
    cfg: NotifyConfig,
    clientOpt: Option[SlackClient],
    audit: AuditSink,
    sleeper: Sleeper = RealSleeper
) {
  private val sentKeys = scala.collection.mutable.Set[String]()
  private var posts = 0

  def process(decisions: Seq[Decision]): Unit = {
    val filtered = filterByLevel(decisions)
    val sendable = filtered.filter(d => d.status == DecisionStatus.Send || (cfg.slack.level == NotificationLevel.Detailed && d.alert.severity == Severity.INFO))
    // Always audit
    decisions.foreach(d => audit.write(toAudit(d)))

    if (!cfg.slack.enabled) return
    if (cfg.slack.dryRun) {
      // Print intended messages
      val msgs = buildMessages(sendable.map(_.alert))
      msgs.foreach { m => println(s"[DRY-RUN] Slack -> channel=${cfg.slack.channel.getOrElse("-")}: ${m.text}") }
      return
    }
    clientOpt.foreach { client =>
      val msgs = buildMessages(sendable.map(_.alert))
      msgs.foreach { m =>
        if (posts >= cfg.slack.maxMessagesPerRun) return
        // Dedup based on key
        val toInclude = !sentKeys.contains(m.text) // text includes key implicitly via renderer content
        if (toInclude) {
          val resp = withRetry(3) { client.post(m) }
          posts += 1
          sentKeys.add(m.text)
          if (!resp.ok) {
            System.err.println(s"Slack post failed status=${resp.status}")
          }
        }
      }
    }
  }
  private def filterByLevel(d: Seq[Decision]): Seq[Decision] = {
    cfg.slack.level match {
      case NotificationLevel.Basic =>
        d.filter { dec =>
          (dec.alert.behavior == Behavior.Availability && dec.status == DecisionStatus.Send) ||
          (dec.alert.behavior == Behavior.Freshness && dec.status == DecisionStatus.Send) ||
          (dec.status == DecisionStatus.Send && dec.alert.severity == Severity.CRITICAL)
        }
      case NotificationLevel.Advanced =>
        d.filter(_.status == DecisionStatus.Send) // include WARN + CRITICAL sends
      case NotificationLevel.Detailed =>
        d // include all decisions; INFO may be sent as summaries above
    }
  }

  private def buildMessages(alerts: Seq[Alert]): Seq[SlackMessage] = {
    val channel = cfg.slack.channel.getOrElse("")
    val level = cfg.slack.level
    // Batching: coalesce in groups of batchSize
    alerts.grouped(cfg.slack.batchSize).toSeq.map { group =>
      if (group.size == 1) {
        val a = group.head
        val text = SlackRenderer.renderText(a, cfg)
        val blocks = Some(SlackRenderer.renderBlocks(a, cfg, level))
        SlackMessage(channel, text, blocks)
      } else {
        val header = s"[${cfg.product}|${cfg.environment}] ${group.size} alerts (${group.map(_.severity.name).groupBy(identity).map{case (k,v) => s"$k=${v.size}"}.mkString(", ")})"
        val items = group.take(10).map(a => s"• ${a.table}${a.partition.map("/"+_).getOrElse("")}:${a.behavior.name} ${a.severity.name} ${a.message}").mkString("\n")
        val text = header
        val blocksJson = JsonUtil.arr(Seq(
          JsonUtil.obj("type" -> "section", "text" -> Map("type" -> "mrkdwn", "text" -> header)),
          JsonUtil.obj("type" -> "section", "text" -> Map("type" -> "mrkdwn", "text" -> items))
        ))
        SlackMessage(channel, text, Some(blocksJson))
      }
    }
  }

  private def withRetry[A](max: Int)(f: => A): A = {
    var attempts = 0
    var lastErr: Option[Throwable] = None
    while (attempts < max) {
      try { return f }
      catch {
        case t: Throwable =>
          lastErr = Some(t)
          val backoff = math.pow(2, attempts).toLong * 200L
          sleeper.sleep(backoff)
      }
      attempts += 1
    }
    // last attempt
    f
  }

  private def toAudit(d: Decision): AuditRecord = {
    AuditRecord(
      key = d.alert.key.toString,
      level = cfg.slack.level.name,
      severity = d.alert.severity.name,
      table = d.alert.table,
      behavior = d.alert.behavior.name,
      status = d.status.name,
      reason = d.reason,
      timestamp = d.alert.timestamp.toString
    )
  }

  
}
