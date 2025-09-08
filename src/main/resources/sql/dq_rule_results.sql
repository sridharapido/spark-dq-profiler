CREATE TABLE IF NOT EXISTS iceberg.obs.dq_rule_results (
  table_name STRING,
  partition_value STRING,
  rule_id STRING,
  passed BOOLEAN,
  fail_rate DOUBLE,
  sampled_rows BIGINT,
  severity STRING,
  observed STRING,
  expected STRING,
  examples ARRAY<STRING>,
  ts TIMESTAMP
) USING iceberg
PARTITIONED BY (ts);

