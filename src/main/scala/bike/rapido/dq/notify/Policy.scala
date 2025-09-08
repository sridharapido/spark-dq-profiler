package bike.rapido.dq.notify

import java.time.Instant

sealed trait DecisionStatus { def name: String }
object DecisionStatus {
  case object Send extends DecisionStatus { val name = "sent" }
  case object Skip extends DecisionStatus { val name = "skipped" }
  case object Muted extends DecisionStatus { val name = "muted" }
}

final case class Decision(alert: Alert, status: DecisionStatus, reason: String)

class PolicyEngine(cfg: NotifyConfig) {

  private def tableOverride(table: String): RuleOverrides = cfg.rules.overrides.getOrElse(table, RuleOverrides())

  private def ruleFor(table: String, behavior: Behavior): (Boolean, Severity, Option[Double], Option[Int]) = {
    val ov = tableOverride(table)
    behavior match {
      case Behavior.Availability =>
        val r = ov.availability.getOrElse(cfg.rules.default.availability)
        (r.enabled, r.severity, None, None)
      case Behavior.Freshness =>
        val r = ov.freshness.getOrElse(cfg.rules.default.freshness)
        (r.enabled, r.severity, None, Some(r.staleHours))
      case Behavior.Drift =>
        val r = ov.drift.getOrElse(cfg.rules.default.drift)
        (r.enabled, r.severity, Some(r.threshold), None)
      case Behavior.NullRatio =>
        val r = ov.null_ratio.getOrElse(cfg.rules.default.null_ratio)
        (r.enabled, r.severity, Some(r.threshold), None)
      case Behavior.RowCountDoD =>
        val r = ov.row_count_dod.getOrElse(cfg.rules.default.row_count_dod)
        (r.enabled, r.severity, Some(r.threshold), None)
      case Behavior.AverageChange =>
        val r = ov.average_change.getOrElse(cfg.rules.default.average_change)
        (r.enabled, r.severity, Some(r.threshold), None)
    }
  }

  def evaluate(e: DqEvent): Decision = {
    val (enabled, severity, thresholdOpt, staleHoursOpt) = ruleFor(e.table, e.behavior)
    if (!enabled) {
      return Decision(
        Alert(e.behavior, e.table, e.partition, actual = metricValue(e), threshold = thresholdOpt.map(_.toDouble), severity = severity, message = s"Muted by rule", timestamp = e.timestamp),
        DecisionStatus.Muted,
        reason = "rule_disabled"
      )
    }

    e.behavior match {
      case Behavior.Availability =>
        val ok = e.metrics.getOrElse("available", 1.0) >= 1.0
        val actual = if (ok) 1.0 else 0.0
        if (!ok)
          Decision(Alert(e.behavior, e.table, e.partition, actual, None, Severity.CRITICAL, "Availability failure", e.timestamp), DecisionStatus.Send, "availability_failed")
        else
          Decision(Alert(e.behavior, e.table, e.partition, actual, None, Severity.INFO, "Available", e.timestamp), DecisionStatus.Skip, "availability_ok")

      case Behavior.Freshness =>
        val ageH = e.metrics.getOrElse("age_hours", Double.NaN)
        val staleH = staleHoursOpt.getOrElse(24)
        if (!ageH.isNaN && ageH > staleH)
          Decision(Alert(e.behavior, e.table, e.partition, ageH, Some(staleH.toDouble), severity, s"Stale by ${ageH - staleH}h", e.timestamp), DecisionStatus.Send, "freshness_stale")
        else
          Decision(Alert(e.behavior, e.table, e.partition, ageH, Some(staleH.toDouble), Severity.INFO, "Fresh", e.timestamp), DecisionStatus.Skip, "freshness_within_sla")

      case Behavior.Drift =>
        val score = e.metrics.getOrElse("drift_score", Double.NaN)
        val thr = thresholdOpt.getOrElse(0.3)
        if (!score.isNaN && score > thr)
          Decision(Alert(e.behavior, e.table, e.partition, score, Some(thr), severity, "Drift above threshold", e.timestamp), DecisionStatus.Send, "drift_high")
        else
          Decision(Alert(e.behavior, e.table, e.partition, score, Some(thr), Severity.INFO, "Drift normal", e.timestamp), DecisionStatus.Skip, "drift_low")

      case Behavior.NullRatio =>
        val r = e.metrics.getOrElse("null_ratio", Double.NaN)
        val thr = thresholdOpt.getOrElse(0.2)
        if (!r.isNaN && r > thr)
          Decision(Alert(e.behavior, e.table, e.partition, r, Some(thr), severity, "Null ratio high", e.timestamp), DecisionStatus.Send, "null_high")
        else
          Decision(Alert(e.behavior, e.table, e.partition, r, Some(thr), Severity.INFO, "Null ratio ok", e.timestamp), DecisionStatus.Skip, "null_low")

      case Behavior.RowCountDoD =>
        val d = math.abs(e.metrics.getOrElse("delta_pct", Double.NaN))
        val thr = thresholdOpt.getOrElse(0.2)
        if (!d.isNaN && d > thr)
          Decision(Alert(e.behavior, e.table, e.partition, d, Some(thr), severity, "Row count DoD large change", e.timestamp), DecisionStatus.Send, "dod_large")
        else
          Decision(Alert(e.behavior, e.table, e.partition, d, Some(thr), Severity.INFO, "Row count DoD normal", e.timestamp), DecisionStatus.Skip, "dod_small")

      case Behavior.AverageChange =>
        val d = math.abs(e.metrics.getOrElse("change_ratio", Double.NaN))
        val thr = thresholdOpt.getOrElse(0.2)
        if (!d.isNaN && d > thr)
          Decision(Alert(e.behavior, e.table, e.partition, d, Some(thr), severity, "Average change high", e.timestamp), DecisionStatus.Send, "avg_high")
        else
          Decision(Alert(e.behavior, e.table, e.partition, d, Some(thr), Severity.INFO, "Average change normal", e.timestamp), DecisionStatus.Skip, "avg_low")
    }
  }

  private def metricValue(e: DqEvent): Double = e.metrics.headOption.map(_._2).getOrElse(Double.NaN)
}

