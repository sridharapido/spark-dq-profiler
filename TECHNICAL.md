# Technical Documentation

Overview
- This repo provides a Spark-based Data Quality (DQ) profiler with availability, rules, and drift checks. It now includes:
  - Notification subsystem (transport-agnostic core + Slack transport).
  - Unified reader helper for Presto JDBC or Catalog-based reads.
  - Iceberg writer (existing CTAS/INSERT) and an Iceberg schema reconciler utility.

Architecture
- Core DQ: `bike.rapido.dq.*`
  - `EntryPoint`: CLI orchestration, demo harness, notification wiring, optional IO reader smoke-check.
  - `model.scala`: core model and `SparkSessionFactory`.
- Notifications: `bike.rapido.dq.notify.*`
  - `Domain.scala`: Alert/Severity/Behavior/Levels + DqEvent minimal interface.
  - `Config.scala`: YAML/JSON config loader with env substitution.
  - `Policy.scala`: rule evaluation using defaults and per-table overrides.
  - `Dispatcher.scala`: level filter, dedup, batching, rate limit, retry, audit sink.
  - `slack/SlackTransport.scala`: Slack clients (webhook/web API) + renderer (basic/advanced/detailed).
- IO Readers: `bike.rapido.dq.io.readers`
  - `spark.read.source(cfg, sql)`: Presto JDBC or Catalog mode via reflection.
- Iceberg IO:
  - `bike.rapido.dq.iceberg.IcebergWriter`: Align DF to table columns and write (CTAS/INSERT path).
  - `bike.rapido.dq.iceberg.IcebergSchemaReconciler`: HiveCatalog utility for schema convergence.
- Telemetry: `bike.rapido.dq.Metadog`
  - Optional reflection-based shim; NOOP if client absent.

Notification Levels
- `basic`: Availability + Freshness + CRITICAL.
- `advanced`: `basic` plus all WARN.
- `detailed`: everything including INFO and richer summaries.
- Implemented as filters on `Decision`, not code-path forks.

Reliability
- Batching: group alerts (default 10) and coalesce into a summary block.
- Rate limit: cap messages per run (default 20).
- Retry: up to 3, exponential backoff (200/400/800ms).
- Dedup: stable key per (behavior, table, partition, runDate).
- Dry-run: prints messages, no network calls.
- Audit: NDJSON at `./out/YYYYMMDD/alerts_audit.ndjson`.

Configuration
- `NotifyConfig` (YAML/JSON): product, environment, slack (enabled, level, token/webhook, channel, mentions), rules (defaults, overrides), links.
- Env variables may inject secrets: `${SLACK_TOKEN}`.

Extensibility
- Add new transport: implement a client and renderer using `Alert` payloads, then inject via `NotificationDispatcher`.
- Add new behavior/rule: extend `Behavior` and augment `PolicyEngine.evaluate`; dispatcher remains unchanged.
- Replace IO reader client class for Catalog mode via `reader-conf.catalog.client.class`.

Testing Strategy
- Unit tests:
  - Policy evaluation and level filtering.
  - Dispatcher dedup/rate-limit/retry.
  - Slack renderer block formatting.
  - Metadog NOOP path.
  - Readers catalog path via a fake client (reflection).
- Integration tests:
  - End-to-end simulation with notifications across levels.

Build & Toolchain
- Java 8 target, Scala 2.12.x via scala-maven-plugin.
- Spark dependencies are `provided` to support cluster deployments.
- Iceberg Hive metastore dependency included for reconciler utility.

Security
- Secrets never logged; reader redacts Atlas password field.
- Slack tokens/webhooks are substituted via env and redacted in logs.

Limitations / Future Work
- Readers Presto path expects a driver and reachable JDBC endpoint for full integration tests.
- Iceberg reconciler utility not yet wired into the default writer path.
- Additional transports (email/PagerDuty) can be added using the existing abstractions.
