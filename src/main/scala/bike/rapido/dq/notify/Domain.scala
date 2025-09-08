package bike.rapido.dq.notify

import java.time.{Instant, ZoneOffset, ZonedDateTime}

sealed trait Behavior { def name: String }
object Behavior {
  case object Availability extends Behavior { val name = "availability" }
  case object Freshness extends Behavior { val name = "freshness" }
  case object Drift extends Behavior { val name = "drift" }
  case object NullRatio extends Behavior { val name = "null_ratio" }
  case object RowCountDoD extends Behavior { val name = "row_count_dod" }
  case object AverageChange extends Behavior { val name = "average_change" }

  def from(s: String): Behavior = s.toLowerCase match {
    case Availability.name  => Availability
    case Freshness.name     => Freshness
    case Drift.name         => Drift
    case NullRatio.name     => NullRatio
    case RowCountDoD.name   => RowCountDoD
    case AverageChange.name => AverageChange
    case other              => throw new IllegalArgumentException(s"Unknown behavior: $other")
  }
}

sealed trait Severity { def name: String }
object Severity {
  case object INFO extends Severity { val name = "INFO" }
  case object WARN extends Severity { val name = "WARN" }
  case object CRITICAL extends Severity { val name = "CRITICAL" }

  def from(s: String): Severity = s.toUpperCase match {
    case INFO.name     => INFO
    case WARN.name     => WARN
    case CRITICAL.name => CRITICAL
    case other         => throw new IllegalArgumentException(s"Unknown severity: $other")
  }
}

sealed trait NotificationLevel { def name: String }
object NotificationLevel {
  case object Basic extends NotificationLevel { val name = "basic" }
  case object Advanced extends NotificationLevel { val name = "advanced" }
  case object Detailed extends NotificationLevel { val name = "detailed" }

  def from(s: String): NotificationLevel = s.toLowerCase match {
    case Basic.name    => Basic
    case Advanced.name => Advanced
    case Detailed.name => Detailed
    case other         => throw new IllegalArgumentException(s"Unknown notify level: $other")
  }
}

final case class AlertKey(behavior: Behavior, table: String, partition: Option[String], runDate: String) {
  override def toString: String = s"${behavior.name}|$table|${partition.getOrElse("-")}|$runDate"
}

final case class Alert(
    behavior: Behavior,
    table: String,
    partition: Option[String],
    actual: Double,
    threshold: Option[Double],
    severity: Severity,
    message: String,
    timestamp: Instant,
    extra: Map[String, String] = Map.empty
) {
  lazy val key: AlertKey = {
    val day = ZonedDateTime.ofInstant(timestamp, ZoneOffset.UTC).toLocalDate.toString
    AlertKey(behavior, table, partition, day)
  }
}

// Minimal interface for DQ results to decouple from computation
trait DqEvent {
  def behavior: Behavior
  def table: String
  def partition: Option[String]
  def timestamp: Instant
  def metrics: Map[String, Double] // e.g., Map("null_ratio" -> 0.12)
}

