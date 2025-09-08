CREATE TABLE IF NOT EXISTS iceberg.obs.dq_anomaly_events (
  table_name STRING,
  partition_value STRING,
  metric_name STRING,
  score DOUBLE,
  severity STRING,
  reference STRING,
  details MAP<STRING,STRING>,
  ts TIMESTAMP
) USING iceberg
PARTITIONED BY (ts);

