# Local Run Guide

Requirements
- Java 8 (1.8)
- Maven 3.8+
- Scala 2.12 toolchain (handled via Maven plugin)
- Optional for full runs: Apache Spark 3.3.x on your machine (spark-submit)

Build
- Compile + test: `mvn -q -DskipTests=false test`
- Package shaded JAR: `mvn -q -DskipTests package`

Run (demo + notifications dry-run)
- The app ships a CLI entry `bike.rapido.dq.EntryPoint`.
- Example (dry-run Slack):
```
java -jar target/*-shaded.jar \
  --demo --tables db.sales.orders \
  --notify --notify-level=basic \
  --notify-config=examples/notify.yml \
  --notify-dry-run
```

Run (unified reader “catalog” mode demo)
- Uses the reflection-based client. Provide a client class if you don’t have one.
```
java -jar target/*-shaded.jar \
  --demo \
  --reader=catalog \
  --reader-conf=catalog.atlas.baseurls=http://atlas:21000,catalog.atlas.username=user,catalog.atlas.password=secret,catalog.client.class=bike.rapido.catalog.client.spark.CatalogClient
```

Run (unified reader “presto” mode demo)
- Requires a reachable Presto JDBC endpoint and driver on classpath in cluster runs.
```
java -jar target/*-shaded.jar \
  --demo \
  --reader=presto \
  --reader-conf=presto.jdbc.url=jdbc:presto://presto:8080/hive/default,presto.jdbc.username=etl_user
```

Spark submit (optional)
- For non-demo cluster runs, use `spark-submit`.
```
spark-submit --class bike.rapido.dq.EntryPoint target/*-shaded.jar \
  --tables db.sales.orders --mode all --notify --notify-config=/path/to/notify.yml
```

Notes
- Notifications are feature-flagged; they do not impact exit codes.
- Dry-run mode prints intended Slack messages and never performs network calls.
- Audit trail is written to `./out/YYYYMMDD/alerts_audit.ndjson` when notifications are evaluated.
