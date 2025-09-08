# Slack Notifications for spark-dq-profiler

Overview
- Transport-agnostic core with a Slack transport behind an interface.
- Policy layer applies global + per-table rules and thresholds.
- Levels are filters: basic, advanced, detailed.
- Reliability: batching, rate limit, retries, dedup per run, audit trail.

Enable
- Build: `mvn -q -DskipTests package`
- Run with flags:
  - `--notify` to enable
  - `--notify-level=basic|advanced|detailed`
  - `--notify-config=/path/to/config.yml`
  - `--notify-dry-run` to print messages without sending

Config (YAML/JSON)
- See `examples/notify.yml` for a full template.
- Env substitution: `${SLACK_TOKEN}` is read from the environment.

Rules
- Defaults:
  - Availability: always notify on failure (CRITICAL)
  - Freshness: stale beyond 24h (WARN)
  - Drift: score > 0.30 (WARN)
  - Null Ratio: > 0.20 (WARN)
  - Row Count DoD: |delta_pct| > 0.20 (WARN)
  - Average change: |change_ratio| > 0.20 (WARN)
- Overrides: `rules.overrides."<db>.<schema>.<table>"` may override `enabled`, `threshold`, `severity`.
- Mute: set `enabled: false` at default or override level.

Levels
- basic: Availability + Freshness + any CRITICAL
- advanced: basic + all WARN
- detailed: all alerts including INFO summaries

Message
- Includes: product, environment, table/partition, behavior, severity, actual vs threshold, timestamp, dedup key.
- `detailed` includes extra fields map when provided.

Reliability & Controls
- Batching: groups of `batchSize` (default 10) collapsed into summary posts.
- Rate limit: `maxMessagesPerRun` (default 20).
- Retries: up to 3 with exponential backoff (starting at 200ms).
- Dry-run: prints intended messages; never calls Slack.
- Idempotent within run: dedup by key.
- Audit: `./out/YYYYMMDD/alerts_audit.ndjson` with key, level, severity, status, reason.

SOPs
- Add table-specific rule:
  1. Edit `notify.yml` → `rules.overrides."db.sales.orders".drift.threshold: 0.10`.
  2. Commit and deploy; no code changes required.
- Mute a rule:
  - Set `enabled: false` under the rule scope (default or table override).
- Test locally:
  - `--notify --notify-dry-run --notify-config=examples/notify.yml --demo --tables db.sales.orders`
- Rotate Slack creds:
  - Update secret in secret manager or environment; config uses `${SLACK_TOKEN}` or `${SLACK_WEBHOOK}`. No code change needed.

QA Checklist (staging)
- With `--notify --notify-level=basic`: Availability failure posts CRITICAL; mild drift/null do not post.
- With `advanced`: drift/null WARN appear; sampling/early warnings included when present.
- With `detailed`: summary includes INFO-level and run metrics.
- Overrides: Set `drift.threshold=0.10` for `db.sales.orders`; verify alert where global 0.30 would not.
- Dry-run: no HTTP calls; console shows intended text/channel.
- Slack failures: transient failures retry; errors logged; DQ exit code unaffected.
- Dedup: repeated events for same key only post once per run.
- Audit: audit NDJSON has status sent/skipped/muted and reason.

