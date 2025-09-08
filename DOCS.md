# spark-dq-profiler — Detailed Documentation

This project provides a lightweight, single-module Spark application for profiling data quality and detecting drift. It targets Spark 3.3.x (Scala 2.12) running on Java 8, and ships a shaded CLI for local and cluster use.

Namespaces: `bike.rapido.dq.*`

Contents
- Features Overview
- Architecture & Packages
- CLI Usage
- Outputs & Sinks
- IO Readers
- Iceberg Tables
- Development & Testing
- Deployment Notes

## Features Overview

- Availability
  - Validates expected partitions via `$partitions` metadata; flags missing or zero-row partitions.
  - Tracks freshness via `$history` metadata; flags when snapshot age exceeds SLA hours.
- Rules
  - Deterministic sampling with optional partition filtering.
  - Composite PK duplicate rate checks via approx distinct count.
  - Segment-level null-rate checks for a configured measure across whitelisted dimensions.
- Drift (PSI)
  - Reference bins (quantile cut points) computed/persisted per (table, column, partition).
  - PSI vs yesterday and last week; thresholds → severity.
- Sinks
  - Stdout (one-line JSON), file (NDJSON), or Iceberg tables.

## Architecture & Packages

- `bike.rapido.dq`
  - `MetricPoint`, `RuleResult`: canonical outputs for metrics and validations.
  - `SparkSessionFactory`: `local()` for demos, `cluster()` for production.
  - `EntryPoint`: CLI; orchestrates availability, rules, and drift, and routes outputs to sinks.

- `bike.rapido.dq.metadata`
  - `MetadataProfiler`:
    - `availability(spark, table, partitionKeyOpt, expectedPartitions)`
    - `freshness(spark, table, slaHours)`
    - `rowCountDoDWoW(spark, table, partitionKey)`

- `bike.rapido.dq.rules`
  - `SimpleRules`:
    - `sampleDF(spark, table, partitionKeyOpt, partitions, frac)` (deterministic sample)
    - `pkDuplicates(sample, pkCols, table, partition)` (acd vs count)
    - `segmentNulls(sample, col, dims, threshold, table)`

- `bike.rapido.dq.drift`
  - `PSI`: Population Stability Index calculation with safe guards.
  - `Histogram`: Normalized histogram with null bucket using saved cuts.
  - `ReferenceBins`: compute, load, and save quantile cut points in Iceberg.
  - `DriftRunner`: orchestrates DoD/WoW comparisons and emits `DriftEvent`.

- `bike.rapido.dq.sinks`
  - `StdoutSink`: one-line JSON encoders for all outputs.
  - `JsonFileSink`: writes NDJSON files to `./out/YYYYMMDD/*.json`.
  - `IcebergSink`: best-effort DDL creation and append into three Iceberg tables.
  - `WebhookNotifier`: Slack Incoming Webhook via Apache HttpClient.

- `bike.rapido.dq.io`
  - `readers`:
    - `spark.read.source(config, sql)` — unified Presto JDBC or Catalog-mode read via reflection.

## CLI Usage

Main class: `bike.rapido.dq.EntryPoint`

Options:
- `--catalog`, `--schema`, `--tables`, `--partition-key`, `--partitions`
- `--mode` one of `availability|rules|drift|all`
- `--write` (persist outputs via Iceberg or JSON files)
- Notifications:
  - `--notify` (enable notification dispatch)
  - `--notify-level=basic|advanced|detailed`
  - `--notify-config=/path/to/config.yml` (YAML/JSON; env vars like ${SLACK_TOKEN} supported)
  - `--notify-dry-run` (print intended messages only)
- `--demo` (local Spark; creates a tiny in-memory demo table when missing)

Examples:

```
mvn -q -DskipTests package
spark-submit --class bike.rapido.dq.EntryPoint target/*-shaded.jar \
  --demo --tables dummy_table --mode all --write
```

## Outputs & Sinks

- Stdout: one-line JSON per event for easy log ingestion.
- JsonFileSink: NDJSON files under `./out/YYYYMMDD/metrics.json`, `rules.json`, `drift.json`.
- IcebergSink (if catalog is resolvable):
  - Creates DDLs if missing, then appends to the tables below.

## IO Readers

Unified helper is under `bike.rapido.dq.io.readers`.

Usage (Presto):

```
import bike.rapido.dq.io.readers._
implicit val spark: org.apache.spark.sql.SparkSession = ...

val cfg = Map(
  "reader" -> "presto",
  "presto.jdbc.url" -> "jdbc:presto://presto:8080/hive/default",
  "presto.jdbc.username" -> "etl_user"
)
val df = spark.read.source(cfg, "select * from hive.default.orders where ds='2025-09-08'")
```

Usage (Catalog):

```
import bike.rapido.dq.io.readers._
implicit val spark: org.apache.spark.sql.SparkSession = ...

val cfg = Map(
  "reader" -> "catalog",
  "catalog.atlas.baseurls" -> "http://atlas:21000",
  "catalog.atlas.username" -> "user",
  "catalog.atlas.password" -> "secret",
  "catalog.tag" -> "default",
  // Optional override if your client differs:
  // "catalog.client.class" -> "bike.rapido.catalog.client.spark.CatalogClient"
)
val df = spark.read.source(cfg, "select id, metric from some_dataset")
```

## Notification Config

Single file (YAML or JSON). Example:

```
slack:
  enabled: true
  level: basic
  token: ${SLACK_TOKEN}
  channel: "#dq-alerts"
  mention:
    critical: "@oncall-dp"
    warn: ""

rules:
  default:
    availability: { enabled: true, severity: critical }
    freshness: { enabled: true, stale_hours: 24, severity: warn }
    drift: { enabled: true, threshold: 0.30, severity: warn }
    null_ratio: { enabled: true, threshold: 0.20, severity: warn }
    row_count_dod: { enabled: true, threshold: 0.20, severity: warn }
    average_change: { enabled: true, threshold: 0.20, severity: warn }

  overrides:
    "db.sales.orders":
      drift: { threshold: 0.10, severity: critical }
      null_ratio: { enabled: false }
```

Levels are filters on emitted alerts:
- basic: availability + freshness + CRITICAL only
- advanced: basic + WARN
- detailed: all, including INFO summaries

Reliability & audit
- Retries with backoff on failure; notification never fails the job
- Per-run idempotency and rate-limit; batching supported
- Audit trail written to `./out/YYYYMMDD/alerts_audit.ndjson`

## Iceberg Tables

DDL files live under `src/main/resources/sql` and are executed best-effort:

- `iceberg.obs.dq_metrics_daily`
- `iceberg.obs.dq_rule_results`
- `iceberg.obs.dq_anomaly_events`

## Development & Testing

Requirements: Java 8+, Maven 3.8+.

- Tests: `mvn -q -DskipTests=false test`
- Package: `mvn -q -DskipTests package`

Spark and Iceberg dependencies are marked `provided` — use `spark-submit` for non-demo runs.

## Deployment Notes

- For clusters with Iceberg configured, `--write` will target Iceberg; otherwise JSON files are written locally.
- Slack webhook is optional, used for high-severity notifications when `--write` is enabled.
