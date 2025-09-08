# spark-dq-profiler

Single-module Maven project (Scala 2.12, Java 8) for Spark 3.3.x that profiles table availability, runs simple data quality rules, and computes drift (PSI). Namespaces are under `bike.rapido.dq`.

Quick start
- Build: `mvn -q -DskipTests package`
- Run demo: `bash scripts/run-local.sh` (uses spark-submit if available)
- Full docs: see `DOCS.md` (notifications: `NOTIFICATIONS.md`)
 - Local run guide: `LOCAL_RUN.md`

Highlights
- Availability checks via `$partitions` and `$history`
- Rules: PK duplicates, segment null rates
- Drift: PSI DoD/WoW with reference bins saved to Iceberg (if available)
- Outputs: stdout JSON, NDJSON files, or Iceberg tables
 - IO: Unified reader helper for Presto or Catalog via `bike.rapido.dq.io.readers`
 - Telemetry: Optional Metadog shim (NOOP if library not present)

Notifications (new)
- Slack notifications with levels: `basic|advanced|detailed`
- Global defaults + per-table overrides via YAML/JSON
- CLI flags: `--notify`, `--notify-level`, `--notify-config`, `--notify-dry-run`
- Dry-run prints messages, never calls Slack

Quick demo
```
mvn -q -DskipTests package
java -jar target/*-shaded.jar \
  --demo --tables db.sales.orders \
  --notify --notify-level=basic \
  --notify-config=examples/notify.yml \
  --notify-dry-run
```

License: Apache-2.0
