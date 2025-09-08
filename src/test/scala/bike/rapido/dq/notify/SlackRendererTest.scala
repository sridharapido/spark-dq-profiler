package bike.rapido.dq.notify

import org.scalatest.funsuite.AnyFunSuite
import bike.rapido.dq.notify.slack.SlackRenderer
import java.time.Instant

class SlackRendererTest extends AnyFunSuite {
  private val cfg = NotifyConfig(product = "spark-dq-profiler", environment = "test", slack = SlackConfig())
  private val alrt = Alert(Behavior.Drift, "db.t", Some("p"), actual = 0.42, threshold = Some(0.3), severity = Severity.WARN, message = "Drift above threshold", timestamp = Instant.parse("2025-09-08T12:00:00Z"))

  test("render basic text") {
    val text = SlackRenderer.renderText(alrt, cfg)
    assert(text.contains("drift"))
    assert(text.contains("0.4200"))
  }

  test("render advanced blocks contains fields") {
    val blocks = SlackRenderer.renderBlocks(alrt, cfg, NotificationLevel.Advanced)
    assert(blocks.contains("\"fields\""))
    assert(blocks.contains("\"Behavior\""))
  }
}
