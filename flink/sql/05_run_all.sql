-- ================================================================
-- 05_run_all.sql — Run All AML Detection Jobs at Once
-- Flink SQL Client (Standalone)
--
-- Usage:
--   /opt/flink/bin/sql-client.sh -f flink/sql/05_run_all.sql
--
-- Or paste each file manually in order in the SQL Client:
--   01_kafka_source.sql → 02_kudu_aml_alerts.sql → 03_large_cash_job.sql → 04_smurfing_job.sql
-- ================================================================

-- [Step 1] Kafka Source Table
CREATE TABLE kafka_aml_transactions (
  transaction_id  STRING,
  account_id      STRING,
  amount          DOUBLE,
  txn_time        BIGINT,
  channel         STRING,
  event_time AS TO_TIMESTAMP_LTZ(txn_time, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector'                              = 'kafka',
  'topic'                                  = 'sbi-aml-transactions',
  'properties.bootstrap.servers'           = 'ccycloud-1.jshin.root.comops.site:9093,ccycloud-2.jshin.root.comops.site:9093,ccycloud-3.jshin.root.comops.site:9093',
  'properties.security.protocol'           = 'SASL_SSL',
  'properties.sasl.mechanism'              = 'GSSAPI',
  'properties.sasl.kerberos.service.name'  = 'kafka',
  'properties.ssl.truststore.location'     = '/var/lib/cloudera-scm-agent/agent-cert/cm-auto-in_cluster_truststore.jks',
  'properties.ssl.truststore.password'     = 'zpXWTjeWPjvNDU4mQnDQPQKn50xfVI9HYX12DSc05x3',
  'scan.startup.mode'                      = 'earliest-offset',
  'format'                                 = 'json',
  'json.ignore-parse-errors'               = 'true'
);

-- [Step 2] Kudu Sink Table
CREATE TABLE aml_alerts (
  alert_id     STRING,
  account_id   STRING,
  alert_type   STRING,
  amount       DOUBLE,
  txn_count    INT,
  window_start BIGINT,
  window_end   BIGINT,
  created_at   BIGINT,
  PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
  'connector'  = 'kudu',
  'masters'    = 'ccycloud-1.jshin.root.comops.site:7051',
  'table-name' = 'default.aml_alerts'
);

-- [Step 3] Large Cash Detection Job
INSERT INTO aml_alerts
SELECT
  CONCAT('LC-', transaction_id) AS alert_id,
  account_id,
  'LARGE_CASH'                  AS alert_type,
  amount,
  1                             AS txn_count,
  txn_time                      AS window_start,
  txn_time                      AS window_end,
  UNIX_TIMESTAMP() * 1000       AS created_at
FROM kafka_aml_transactions
WHERE amount >= 1000000;

-- [Step 4] Smurfing Detection Job
INSERT INTO aml_alerts
SELECT
  CONCAT('SM-', account_id, '-',
    CAST(TUMBLE_START(event_time, INTERVAL '30' MINUTE) AS STRING)) AS alert_id,
  account_id,
  'SMURFING'                     AS alert_type,
  SUM(amount)                    AS amount,
  CAST(COUNT(*) AS INT)          AS txn_count,
  UNIX_TIMESTAMP() * 1000        AS window_start,
  UNIX_TIMESTAMP() * 1000        AS window_end,
  UNIX_TIMESTAMP() * 1000        AS created_at
FROM kafka_aml_transactions
GROUP BY
  account_id,
  TUMBLE(event_time, INTERVAL '30' MINUTE)
HAVING COUNT(*) >= 5;
