package bike.rapido.dq.notify.slack

import bike.rapido.dq.notify._
import bike.rapido.dq.notify.JsonUtil
import java.net.{HttpURLConnection, URL}
import java.nio.charset.StandardCharsets

final case class SlackMessage(channel: String, text: String, blocks: Option[String] = None)

trait SlackClient { def post(message: SlackMessage): SlackResponse }
final case class SlackResponse(ok: Boolean, status: Int, body: String)

class WebhookSlackClient(webhookUrl: String) extends SlackClient {
  override def post(message: SlackMessage): SlackResponse = {
    val url = new URL(webhookUrl)
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST"); conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/json; charset=utf-8")
    val payload = JsonUtil.obj(
      (if (message.channel != null && message.channel.nonEmpty) Some("channel" -> message.channel) else None).toSeq ++
        Seq("text" -> message.text) ++
        message.blocks.map(b => "blocks" -> b).toSeq: _*
    )
    val os = conn.getOutputStream
    os.write(payload.getBytes(StandardCharsets.UTF_8)); os.flush(); os.close()
    val status = conn.getResponseCode
    val body = try {
      val is = if (status >= 200 && status < 300) conn.getInputStream else conn.getErrorStream
      if (is == null) "" else streamToString(is)
    } catch { case _: Throwable => "" }
    SlackResponse(status >= 200 && status < 300, status, body)
  }
  private def streamToString(is: java.io.InputStream): String = {
    val baos = new java.io.ByteArrayOutputStream()
    val buf = new Array[Byte](4096)
    var n = is.read(buf)
    while (n != -1) { baos.write(buf, 0, n); n = is.read(buf) }
    new String(baos.toByteArray, StandardCharsets.UTF_8)
  }
}

class WebApiSlackClient(token: String) extends SlackClient {
  private val endpoint = new URL("https://slack.com/api/chat.postMessage")
  override def post(message: SlackMessage): SlackResponse = {
    val conn = endpoint.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST"); conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/json; charset=utf-8")
    conn.setRequestProperty("Authorization", s"Bearer ${token}")
    val payload = JsonUtil.obj(
      (if (message.channel != null && message.channel.nonEmpty) Some("channel" -> message.channel) else None).toSeq ++
        Seq("text" -> message.text) ++
        message.blocks.map(b => "blocks" -> b).toSeq: _*
    )
    val os = conn.getOutputStream
    os.write(payload.getBytes(StandardCharsets.UTF_8)); os.flush(); os.close()
    val status = conn.getResponseCode
    val body = try {
      val is = if (status >= 200 && status < 300) conn.getInputStream else conn.getErrorStream
      if (is == null) "" else streamToString(is)
    } catch { case _: Throwable => "" }
    SlackResponse(status == 200, status, body)
  }
  private def streamToString(is: java.io.InputStream): String = {
    val baos = new java.io.ByteArrayOutputStream()
    val buf = new Array[Byte](4096)
    var n = is.read(buf)
    while (n != -1) { baos.write(buf, 0, n); n = is.read(buf) }
    new String(baos.toByteArray, StandardCharsets.UTF_8)
  }
}

object SlackRenderer {
  def renderText(alert: Alert, cfg: NotifyConfig): String = {
    val th = alert.threshold.map(t => f" (thr=$t%.3f)").getOrElse("")
    val sev = alert.severity.name
    val mention = alert.severity match {
      case Severity.CRITICAL => cfg.slack.mention.critical.getOrElse("")
      case Severity.WARN     => cfg.slack.mention.warn.getOrElse("")
      case Severity.INFO     => cfg.slack.mention.info.getOrElse("")
    }
    val m = if (mention.nonEmpty) s" $mention" else ""
    s"[${cfg.product}|${cfg.environment}] ${alert.table}${alert.partition.map(p => s"/$p").getOrElse("")} ${alert.behavior.name} ${sev}: ${alert.message} actual=${f"${alert.actual}%.4f"}${th}.${m}"
  }

  def renderBlocks(alert: Alert, cfg: NotifyConfig, level: NotificationLevel): String = level match {
    case NotificationLevel.Basic    => basicBlocks(alert, cfg)
    case NotificationLevel.Advanced => advancedBlocks(alert, cfg)
    case NotificationLevel.Detailed => detailedBlocks(alert, cfg)
  }

  private def basicBlocks(a: Alert, cfg: NotifyConfig): String = {
    val block = JsonUtil.obj(
      "type" -> "section",
      "text" -> Map("type" -> "mrkdwn", "text" -> renderText(a, cfg))
    )
    JsonUtil.arr(Seq(block))
  }

  private def field(k: String, v: String): String = JsonUtil.obj("type" -> "mrkdwn", "text" -> s"*${k}*: ${v}")

  private def advancedBlocks(a: Alert, cfg: NotifyConfig): String = {
    val fields = Seq(
      field("Table", s"${a.table}${a.partition.map(p => s"/$p").getOrElse("")}"),
      field("Behavior", a.behavior.name),
      field("Severity", a.severity.name),
      field("Actual", f"${a.actual}%.4f"),
      field("Threshold", a.threshold.map(t => f"$t%.3f").getOrElse("-")),
      field("Run At", a.timestamp.toString)
    )
    val blocks = Seq(
      JsonUtil.obj("type" -> "section", "text" -> Map("type" -> "mrkdwn", "text" -> renderText(a, cfg))),
      JsonUtil.obj("type" -> "section", "fields" -> fields)
    )
    JsonUtil.arr(blocks)
  }

  private def detailedBlocks(a: Alert, cfg: NotifyConfig): String = {
    val base = scala.util.parsing.json.JSON.parseFull(advancedBlocks(a, cfg)).get.asInstanceOf[List[Any]]
    val extra = if (a.extra.nonEmpty) {
      val fields = a.extra.toList.sortBy(_._1).map { case (k, v) => field(k, v) }
      Seq(JsonUtil.obj("type" -> "section", "fields" -> fields))
    } else Seq.empty
    JsonUtil.arr(base ++ extra)
  }
}
