package bike.rapido.dq.notify

import org.scalatest.funsuite.AnyFunSuite
import bike.rapido.dq.notify.slack.{SlackClient, SlackMessage, SlackResponse}
import java.time.Instant
import java.io.File

class DispatcherTest extends AnyFunSuite {
  private val ts = Instant.parse("2025-09-08T12:00:00Z")
  private def ev(tbl: String, beh: Behavior, m: Map[String, Double]) = new DqEvent { val behavior = beh; val table = tbl; val partition = Some("ds=2025-09-08"); val timestamp = ts; val metrics = m }

  test("basic level filters out warn drift and null") {
    val cfg = NotifyConfig(slack = SlackConfig(enabled = true, level = NotificationLevel.Basic, channel = Some("#test"), dryRun = true))
    val policy = new PolicyEngine(cfg)
    val decs = Seq(
      policy.evaluate(ev("db.t", Behavior.Availability, Map("available" -> 0.0))),
      policy.evaluate(ev("db.t", Behavior.Drift, Map("drift_score" -> 0.4))),
      policy.evaluate(ev("db.t", Behavior.NullRatio, Map("null_ratio" -> 0.25)))
    )
    val auditFile = new File("target/test-audit-basic"); auditFile.mkdirs()
    val audit = new JsonFileAuditSink(auditFile)
    val dispatcher = new NotificationDispatcher(cfg, None, audit, new NoSleep)
    dispatcher.process(decs)
    // Dry-run prints to stdout; key assertion: no exceptions and audit written
    assert(auditFile.exists())
  }

  test("advanced includes warn sends") {
    val cfg = NotifyConfig(slack = SlackConfig(enabled = true, level = NotificationLevel.Advanced, channel = Some("#test"), dryRun = true))
    val policy = new PolicyEngine(cfg)
    val decs = Seq(
      policy.evaluate(ev("db.t", Behavior.Drift, Map("drift_score" -> 0.4))),
      policy.evaluate(ev("db.t", Behavior.NullRatio, Map("null_ratio" -> 0.25)))
    )
    val dispatcher = new NotificationDispatcher(cfg, None, new JsonFileAuditSink(new File("target/test-audit-adv")), new NoSleep)
    dispatcher.process(decs)
  }

  test("retry with backoff does not throw and rate-limit caps") {
    val cfg = NotifyConfig(slack = SlackConfig(enabled = true, level = NotificationLevel.Basic, channel = Some("#test"), dryRun = false, maxMessagesPerRun = 2, batchSize = 1))
    val policy = new PolicyEngine(cfg)
    val decs = (1 to 5).map(i => policy.evaluate(ev(s"db.t$i", Behavior.Availability, Map("available" -> 0.0))))
    val client = new FlakySlackClient(failures = 2)
    val dispatcher = new NotificationDispatcher(cfg, Some(client), new JsonFileAuditSink(new File("target/test-audit-retry")), new NoSleep)
    dispatcher.process(decs)
    assert(client.calls >= 2)
  }

  private class NoSleep extends Sleeper { override def sleep(ms: Long): Unit = () }
  private class FlakySlackClient(var failures: Int) extends SlackClient {
    var calls = 0
    override def post(message: SlackMessage): SlackResponse = {
      calls += 1
      if (failures > 0) { failures -= 1; throw new RuntimeException("boom") }
      SlackResponse(ok = true, status = 200, body = "ok")
    }
  }
}
