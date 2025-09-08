package bike.rapido.dq.sinks

import bike.rapido.dq.{MetricPoint, RuleResult}
import bike.rapido.dq.drift.DriftEvent

object JsonEncoding {
  private def esc(s: String): String = s.flatMap {
    case '"' => "\\\""; case '\\' => "\\\\"; case '\n' => "\\n"; case '\r' => "\\r"; case '\t' => "\\t"; case c if c.isControl => f"\\u${c.toInt}%04x"; case c => c.toString }
  def q(v: String): String = '"' + esc(v) + '"'
  def toJson(mp: MetricPoint): String = { val detailsJson = mp.details.map { case (k, v) => q(k) + ":" + q(v) }.mkString("{", ",", "}"); List("table" -> q(mp.table), "partition" -> mp.partition.map(q).getOrElse("null"), "metricDate" -> q(mp.metricDate.toString), "name" -> q(mp.name), "value" -> mp.value.toString, "details" -> detailsJson).map { case (k, v) => q(k) + ":" + v }.mkString("{", ",", "}") }
  def toJson(rr: RuleResult): String = { val examples = rr.examples.map(q).mkString("[", ",", "]"); val pairs = List("id" -> q(rr.id), "table" -> q(rr.table), "partition" -> rr.partition.map(q).getOrElse("null"), "passed" -> rr.passed.toString, "failRate" -> rr.failRate.toString, "sampledRows" -> rr.sampledRows.toString, "severity" -> q(rr.severity), "examples" -> examples, "observed" -> rr.observed.map(q).getOrElse("null"), "expected" -> rr.expected.map(q).getOrElse("null")); pairs.map { case (k, v) => q(k) + ":" + v }.mkString("{", ",", "}") }
  def toJson(de: DriftEvent): String = { val pairs = List("table" -> q(de.table), "partition" -> de.partition.map(q).getOrElse("null"), "name" -> q(de.name), "score" -> de.score.toString, "severity" -> q(de.severity), "reference" -> q(de.reference)); pairs.map { case (k, v) => q(k) + ":" + v }.mkString("{", ",", "}") }
}

object StdoutSink {
  def print(mp: MetricPoint): Unit = println(JsonEncoding.toJson(mp))
  def print(rr: RuleResult): Unit = println(JsonEncoding.toJson(rr))
  def print(de: DriftEvent): Unit = println(JsonEncoding.toJson(de))
}

