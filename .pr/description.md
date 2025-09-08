Title: Add Slack notification subsystem with levels, rules, and CLI wiring

Summary
- Introduces a transport-agnostic notification core and a Slack transport implementing basic/advanced/detailed levels.
- Adds YAML/JSON configuration with env var substitution, global defaults, and per-table overrides.
- Implements policy evaluation, level filtering, batching, rate-limit, retry/backoff, per-run dedup, and an audit trail.
- Wires CLI flags: `--notify`, `--notify-level`, `--notify-config`, `--notify-dry-run`.
- Includes unit tests, formatting snapshot tests for Slack blocks, and an integration test simulating a run.

Highlights
- Levels are filters, not separate code paths.
- Mute rules globally or per-table.
- Dry-run shows intended messages without network calls; secrets never logged.
- Audit written to `out/YYYYMMDD/alerts_audit.ndjson`.

Screenshots
- See dry-run console outputs in QA logs (attach from local run):
  - basic: critical availability only
  - advanced: adds warning drift/null
  - detailed: includes INFO and summary

Risk & Rollout
- Feature-flagged behind `--notify` and `slack.enabled`.
- Failure to notify does not impact DQ exit codes.

Testing
- `mvn test` — unit + integration tests green.
- Manual dry-run: see README quick demo.

