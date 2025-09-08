package bike.rapido.dq.notify

import org.scalatest.funsuite.AnyFunSuite
import java.time.Instant

class PolicyEngineTest extends AnyFunSuite {
  private val baseCfg = NotifyConfig()
  private val policy = new PolicyEngine(baseCfg)
  private val ts = Instant.parse("2025-09-08T12:00:00Z")
  private def ev(beh: Behavior, m: Map[String, Double]) = new DqEvent { val behavior = beh; val table = "db.sales.orders"; val partition = Some("ds=2025-09-08"); val timestamp = ts; val metrics = m }

  test("availability failure sends critical") {
    val d = policy.evaluate(ev(Behavior.Availability, Map("available" -> 0.0)))
    assert(d.status == DecisionStatus.Send)
    assert(d.alert.severity == Severity.CRITICAL)
  }

  test("freshness stale beyond SLA sends") {
    val d = policy.evaluate(ev(Behavior.Freshness, Map("age_hours" -> 48.0)))
    assert(d.status == DecisionStatus.Send)
  }

  test("drift below threshold skipped") {
    val d = policy.evaluate(ev(Behavior.Drift, Map("drift_score" -> 0.1)))
    assert(d.status == DecisionStatus.Skip)
  }

  test("override drift threshold to 0.1 triggers send") {
    val cfg = baseCfg.copy(rules = RulesConfig(baseCfg.rules.default, Map("db.sales.orders" -> RuleOverrides(drift = Some(ThresholdRule(enabled = true, threshold = 0.1, severity = Severity.CRITICAL))))))
    val p = new PolicyEngine(cfg)
    val d = p.evaluate(ev(Behavior.Drift, Map("drift_score" -> 0.12)))
    assert(d.status == DecisionStatus.Send)
    assert(d.alert.severity == Severity.CRITICAL)
  }

  test("table override can mute null_ratio") {
    val cfg = baseCfg.copy(rules = RulesConfig(baseCfg.rules.default, Map("db.sales.orders" -> RuleOverrides(null_ratio = Some(ThresholdRule(enabled = false, threshold = 0.2, severity = Severity.WARN))))))
    val p = new PolicyEngine(cfg)
    val d = p.evaluate(ev(Behavior.NullRatio, Map("null_ratio" -> 0.9)))
    assert(d.status == DecisionStatus.Muted)
  }
}

