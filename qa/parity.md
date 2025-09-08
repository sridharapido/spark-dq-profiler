# IO & Metadog Parity Review

No external reference project was provided. This review notes observed patterns in this repo:

- IO Write Parity
  - IcebergWriter uses Spark catalog CTAS/INSERT and aligns schemas.
    - Evidence: src/main/scala/com/yourorg/iceberg/IcebergWriter.scala L39-L62
  - Writes emit Metadog counters/timers for success/failure/duration.
    - Evidence: src/main/scala/com/yourorg/iceberg/IcebergWriter.scala L60-L62

- IO Read Parity
  - No explicit reader wrapper found in this repo (reads are via spark.table).
    - Evidence: src/main/scala/bike/rapido/dq/rules/SimpleRules.scala L5-L14

- Metadog Parity
  - Optional; reflection-based client load via `-Dmetadog.fqn`; NOOP fallback.
    - Evidence: src/main/scala/com/yourorg/telemetry/Metadog.scala L24-L57, L64-L67

Unknown / Needs clarification:
- Which IO patterns (beyond Iceberg writes) should be instrumented for parity?
- Should reader wrappers (JDBC/catalog) be included as part of parity scope?
