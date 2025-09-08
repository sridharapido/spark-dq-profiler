# DQ Behavior Audit (Zero-Assumption)

## Overview
We audited the repository against the initial Data Quality behaviors (Availability, Freshness, Null Ratio, Row Count DoD, Average) and the DQ storage model. All findings are based on observable build/run logs and CLI/script outputs. We did not rely on code-level details.

Non-functional guardrails: Java 8 + Spark 3.3.x Maven build succeeded. Locally running the fat jar requires Spark on the classpath or spark-submit. A “cheap-first” approach appears intended (metadata-based checks), but we could not run some behaviors to confirm.

## Behaviors — Present vs Missing

- O1 Availability — Partial
  - Observed: Build succeeds. A demo script exists. Running locally attempts to start but needs Spark runtime.
  - Evidence: build.txt (success tail); demo-availability.txt (script attempts); demo-availability-jar.txt (java -jar attempt)
  - Missing: Confirmed writes to a partition status store and a critical alert row; human-readable log of the alert.

- O2 Freshness — Unknown
  - We did not find a runnable CLI path that prints latest partition and expected (yesterday) nor logs of that run.
  - Evidence: build.txt only.
  - Missing: Observable run, persisted stats row, and a critical alert/log when stale.

- O3 Null Ratio — No
  - We did not find a runnable entrypoint for a metadata-based null ratio. No run logs or persisted ratio evidence.
  - Evidence: build.txt only.
  - Missing: CLI to compute ratios (metadata), persisted results, alerts when threshold > 0.2.

- O4 Row Count DoD — Partial
  - Observed: Build succeeds; demo run attempts but blocked by lack of Spark runtime.
  - Evidence: build.txt; demo-availability.txt.
  - Missing: A demo run that persists OK/Alert rows and prints a clear “dropped from X to Y” message.

- O5 Average — No
  - No runnable path observed to compute per-column averages for today vs yesterday and alert on change_ratio > 0.2.
  - Evidence: build.txt only.
  - Missing: CLI entrypoint; note on performance caveats (full scan vs metadata/sampling); persisted info/alerts; logs.

- DQ Storage — Partial
  - Observed: Some DQ-related DDLs are present (metrics/rules/drift). The canonical set from the doc (partition_status, partition_stats, partition_info, alerts) is not fully observed as a single, consistent namespace/tables.
  - Evidence: build.txt; resource DDLs present (see repo resources listing).
  - Missing: One canonical set of DQ tables with a stable namespace and columns; idempotent create-if-not-exists flow; observable writes from demo runs.

## Non-Functional Guardrails (Observed)
- Java 8 toolchain: Yes
  - Evidence: java_version.txt (Java 1.8 output)
- Spark 3.3.x via Maven: Yes (build resolves Spark 3.3.x deps)
  - Evidence: build.txt
- Maven build: Yes (success)
  - Evidence: build.txt (success tail)
- Fat jar runnable with local Iceberg catalog: Partial
  - The shaded jar builds, but local run depends on Spark runtime. The demo script attempts a fallback classpath; if Spark isn’t installed, run fails. 
  - Evidence: demo-availability.txt (script fallback message), demo-availability-jar.txt (ClassNotFound symptoms)
- “Cheap first”: Partial
  - Intended to use metadata tables, but without runnable demos for some objectives we cannot confirm end-to-end.
  - Evidence: build.txt only.

## Risks / Caveats
- Local run requires spark-submit or a local Spark install; the fallback classpath is best-effort and may not find Spark jars on every machine.
- DQ storage model diverges from the initial schema (metrics/rules/drift vs partition_* + alerts). This complicates downstream consumers and validation.
- Null ratio and average behaviors are not verifiable via CLI/demo; performance caveats (for averages) remain unassessed.
- Lack of a reader-side instrumentation means we can’t measure read IO timing/success.

## Unknown / Needs Clarification
1) Do we require true “multi-module” packaging, or is a single project acceptable?
2) Should null ratio and average checks ship as runnable CLI modes now, or remain tracked as future work?
3) What is the canonical DQ namespace/tables we’ll standardize on (partition_status/partition_stats/partition_info/alerts vs current metrics/rules/drift)?
4) Is spark-submit acceptable as the default demo path, or should we add a dev profile bundling Spark for local runs?

## Next Steps
- Add runnable CLI modes for Freshness, Null Ratio, and Average with clear console logs.
- Standardize on DQ storage tables and include idempotent DDL creation in demo runs.
- Document spark-submit usage in README; consider a dev-fat profile for local demos.
- Add a reader IO wrapper (JDBC/catalog) to emit timing and success/failure metrics in line with “cheap-first”.
- Add a simple CI workflow (Java 8; Maven build/test/format) and expand unit tests.

## Evidence Logs
- build.txt — Maven build/test output
- java_version.txt — Java toolchain version
- maven_version.txt — Maven version
- demo-availability.txt — run-local.sh attempt (with fallback)
- demo-availability-jar.txt — direct java -jar attempt
- demo-drift-jar.txt — direct java -jar (drift) attempt

All logs are in qa/logs/.
