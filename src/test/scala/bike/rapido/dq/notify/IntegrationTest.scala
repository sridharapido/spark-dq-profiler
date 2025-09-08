package bike.rapido.dq.notify

import org.scalatest.funsuite.AnyFunSuite
import java.io.File

class IntegrationTest extends AnyFunSuite {
  test("integration basic/advanced/detailed behavior") {
    val base = NotifyConfig(slack = SlackConfig(enabled = true, channel = Some("#test")))
    val policy = new PolicyEngine(base)
    val ts = java.time.Instant.parse("2025-09-08T12:00:00Z")
    def ev(b: Behavior, m: Map[String, Double]) = new DqEvent { val behavior = b; val table = "db.t"; val partition = Some("p"); val timestamp = ts; val metrics = m }
    val events = Seq(
      ev(Behavior.Availability, Map("available" -> 0.0)),
      ev(Behavior.Drift, Map("drift_score" -> 0.25)),
      ev(Behavior.NullRatio, Map("null_ratio" -> 0.05))
    )
    val decs = events.map(policy.evaluate)
    val audit = new JsonFileAuditSink(new File("target/test-integ"))
    new NotificationDispatcher(base.copy(slack = base.slack.copy(level = NotificationLevel.Basic, dryRun = true)), None, audit, new Sleeper{def sleep(ms:Long)=()} ).process(decs)
    new NotificationDispatcher(base.copy(slack = base.slack.copy(level = NotificationLevel.Advanced, dryRun = true)), None, audit, new Sleeper{def sleep(ms:Long)=()} ).process(decs)
    new NotificationDispatcher(base.copy(slack = base.slack.copy(level = NotificationLevel.Detailed, dryRun = true)), None, audit, new Sleeper{def sleep(ms:Long)=()} ).process(decs)
    assert(new File("target/test-integ").exists())
  }
}

