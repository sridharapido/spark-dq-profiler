CREATE TABLE IF NOT EXISTS iceberg.obs.dq_metrics_daily (
  table_name STRING,
  partition_value STRING,
  metric_date DATE,
  metric_name STRING,
  metric_value DOUBLE,
  details MAP<STRING,STRING>
) USING iceberg
PARTITIONED BY (metric_date);

