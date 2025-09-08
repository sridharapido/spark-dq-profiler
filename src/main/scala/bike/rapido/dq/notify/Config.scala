package bike.rapido.dq.notify

import org.yaml.snakeyaml.Yaml
import java.io.{File, FileInputStream}
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._

final case class SlackMention(critical: Option[String] = None, warn: Option[String] = None, info: Option[String] = None)

final case class SlackConfig(
    enabled: Boolean = false,
    level: NotificationLevel = NotificationLevel.Basic,
    token: Option[String] = None,
    webhookUrl: Option[String] = None,
    channel: Option[String] = None,
    mention: SlackMention = SlackMention(),
    dryRun: Boolean = false,
    maxMessagesPerRun: Int = 20,
    batchSize: Int = 10
)

final case class RuleToggle(enabled: Boolean = true, severity: Severity = Severity.WARN)
final case class ThresholdRule(enabled: Boolean = true, threshold: Double, severity: Severity = Severity.WARN)
final case class FreshnessRule(enabled: Boolean = true, staleHours: Int = 24, severity: Severity = Severity.WARN)

final case class DefaultRules(
    availability: RuleToggle = RuleToggle(enabled = true, severity = Severity.CRITICAL),
    freshness: FreshnessRule = FreshnessRule(),
    drift: ThresholdRule = ThresholdRule(enabled = true, threshold = 0.30),
    null_ratio: ThresholdRule = ThresholdRule(enabled = true, threshold = 0.20),
    row_count_dod: ThresholdRule = ThresholdRule(enabled = true, threshold = 0.20),
    average_change: ThresholdRule = ThresholdRule(enabled = true, threshold = 0.20)
)

final case class RuleOverrides(
    availability: Option[RuleToggle] = None,
    freshness: Option[FreshnessRule] = None,
    drift: Option[ThresholdRule] = None,
    null_ratio: Option[ThresholdRule] = None,
    row_count_dod: Option[ThresholdRule] = None,
    average_change: Option[ThresholdRule] = None
)

final case class RulesConfig(default: DefaultRules = DefaultRules(), overrides: Map[String, RuleOverrides] = Map.empty)

final case class NotifyConfig(
    product: String = "spark-dq-profiler",
    environment: String = sys.env.getOrElse("DQ_ENV", "dev"),
    slack: SlackConfig = SlackConfig(),
    rules: RulesConfig = RulesConfig(),
    links: Map[String, String] = Map.empty
)

object ConfigLoader {
  private def envSubstitute(in: String): String = {
    // replace ${ENV} patterns with env value if present
    val pattern = "\\$\\{([A-Za-z0-9_]+)\\}".r
    pattern.replaceAllIn(in, m => sys.env.getOrElse(m.group(1), m.matched))
  }

  def load(path: String): NotifyConfig = {
    val file = new File(path)
    if (!file.exists()) throw new IllegalArgumentException(s"Config file not found: $path")
    val raw = new String(java.nio.file.Files.readAllBytes(file.toPath), java.nio.charset.StandardCharsets.UTF_8)
    val substituted = envSubstitute(raw)
    if (path.endsWith(".yaml") || path.endsWith(".yml")) fromYaml(substituted)
    else if (path.endsWith(".json")) fromJson(substituted)
    else fromYaml(substituted) // default to yaml
  }

  private def fromYaml(y: String): NotifyConfig = {
    val yaml = new Yaml()
    val data = yaml.load(y).asInstanceOf[JMap[String, Object]]
    fromMap(data.asScala.toMap)
  }

  // Very small JSON subset via SnakeYAML not available; use Typesafe Config if needed.
  private def fromJson(j: String): NotifyConfig = {
    // naive JSON -> YAML conversion via SnakeYAML is not available; instead use a minimal parser by delegating to SnakeYAML after conversion
    // For simplicity, we reuse YAML loader since YAML is a superset of JSON.
    fromYaml(j)
  }

  private def toSeverity(s: Any): Severity = Severity.from(s.toString)
  private def toLevel(s: Any): NotificationLevel = NotificationLevel.from(s.toString)

  private def getMap(m: Map[String, Any], key: String): Map[String, Any] = m.get(key).map(_.asInstanceOf[JMap[String, Any]].asScala.toMap).getOrElse(Map.empty)

  private def fromMap(m: Map[String, Any]): NotifyConfig = {
    val product = m.get("product").map(_.toString).getOrElse("spark-dq-profiler")
    val environment = m.get("environment").map(_.toString).getOrElse(sys.env.getOrElse("DQ_ENV", "dev"))
    val slackM = getMap(m, "slack")
    val slack = SlackConfig(
      enabled = slackM.get("enabled").exists(_.toString.toBoolean),
      level = slackM.get("level").map(toLevel).getOrElse(NotificationLevel.Basic),
      token = slackM.get("token").map(_.toString),
      webhookUrl = slackM.get("webhook_url").orElse(slackM.get("webhookUrl")).map(_.toString),
      channel = slackM.get("channel").map(_.toString),
      mention = {
        val mm = getMap(slackM, "mention")
        SlackMention(mm.get("critical").map(_.toString).filter(_.nonEmpty), mm.get("warn").map(_.toString).filter(_.nonEmpty), mm.get("info").map(_.toString).filter(_.nonEmpty))
      },
      dryRun = slackM.get("dry_run").orElse(slackM.get("dryRun")).exists(_.toString.toBoolean),
      maxMessagesPerRun = slackM.get("max_messages_per_run").orElse(slackM.get("maxMessagesPerRun")).map(_.toString.toInt).getOrElse(20),
      batchSize = slackM.get("batch_size").orElse(slackM.get("batchSize")).map(_.toString.toInt).getOrElse(10)
    )

    val rulesM = getMap(m, "rules")
    val defaultM = getMap(rulesM, "default")
    def mergeToggle(base: RuleToggle, mm: Map[String, Any]): RuleToggle =
      base.copy(
        enabled = mm.get("enabled").map(_.toString.toBoolean).getOrElse(base.enabled),
        severity = mm.get("severity").map(toSeverity).getOrElse(base.severity)
      )
    def mergeThresh(base: ThresholdRule, mm: Map[String, Any]): ThresholdRule =
      base.copy(
        enabled = mm.get("enabled").map(_.toString.toBoolean).getOrElse(base.enabled),
        threshold = mm.get("threshold").map(_.toString.toDouble).getOrElse(base.threshold),
        severity = mm.get("severity").map(toSeverity).getOrElse(base.severity)
      )
    def mergeFresh(base: FreshnessRule, mm: Map[String, Any]): FreshnessRule =
      base.copy(
        enabled = mm.get("enabled").map(_.toString.toBoolean).getOrElse(base.enabled),
        staleHours = mm.get("stale_hours").orElse(mm.get("staleHours")).map(_.toString.toInt).getOrElse(base.staleHours),
        severity = mm.get("severity").map(toSeverity).getOrElse(base.severity)
      )

    val defaults = DefaultRules(
      availability = mergeToggle(DefaultRules().availability, getMap(defaultM, "availability")),
      freshness = mergeFresh(DefaultRules().freshness, getMap(defaultM, "freshness")),
      drift = mergeThresh(DefaultRules().drift, getMap(defaultM, "drift")),
      null_ratio = mergeThresh(DefaultRules().null_ratio, getMap(defaultM, "null_ratio")),
      row_count_dod = mergeThresh(DefaultRules().row_count_dod, getMap(defaultM, "row_count_dod")),
      average_change = mergeThresh(DefaultRules().average_change, getMap(defaultM, "average_change"))
    )

    val overridesM = getMap(rulesM, "overrides").map { case (tbl, v) =>
      val mm = v.asInstanceOf[JMap[String, Any]].asScala.toMap
      val ro = RuleOverrides(
        availability = Some(mergeToggle(defaults.availability, getMap(mm, "availability"))).filter(_ => getMap(mm, "availability").nonEmpty),
        freshness = Some(mergeFresh(defaults.freshness, getMap(mm, "freshness"))).filter(_ => getMap(mm, "freshness").nonEmpty),
        drift = Some(mergeThresh(defaults.drift, getMap(mm, "drift"))).filter(_ => getMap(mm, "drift").nonEmpty),
        null_ratio = Some(mergeThresh(defaults.null_ratio, getMap(mm, "null_ratio"))).filter(_ => getMap(mm, "null_ratio").nonEmpty),
        row_count_dod = Some(mergeThresh(defaults.row_count_dod, getMap(mm, "row_count_dod"))).filter(_ => getMap(mm, "row_count_dod").nonEmpty),
        average_change = Some(mergeThresh(defaults.average_change, getMap(mm, "average_change"))).filter(_ => getMap(mm, "average_change").nonEmpty)
      )
      tbl -> ro
    }

    val links = getMap(m, "links").mapValues(_.toString)

    NotifyConfig(product = product, environment = environment, slack = slack, rules = RulesConfig(defaults, overridesM), links = links)
  }
}

