-- ================================================================
-- 01_kafka_source_table.sql — Create SSB Kafka Source Table
-- Flink 1.15.1 (CSA 1.9.0.1) syntax
--
-- How to run:
--   [Primary]   Run: bash ssb/render_sql.sh, then paste into SSB Web UI
--   [Secondary] python ssb/ssb_rest_client.py (auto-renders and submits)
-- ================================================================
-- Note: ${VAR} placeholders are automatically replaced with actual values
--       by envsubst (shell) or Python str.replace().
-- ================================================================

-- Drop and recreate if already exists
DROP TABLE IF EXISTS kafka_aml_transactions;

CREATE TABLE kafka_aml_transactions (
  -- Transaction data fields
  transaction_id  STRING     COMMENT 'Unique transaction ID',
  account_id      STRING     COMMENT 'Account ID',
  amount          DOUBLE     COMMENT 'Transaction amount (INR)',
  txn_time        BIGINT     COMMENT 'Transaction timestamp (epoch ms)',
  channel         STRING     COMMENT 'Transaction channel',
  merchant_city   STRING     COMMENT 'Transaction city',

  -- Event time for Flink window operations
  -- TO_TIMESTAMP_LTZ: converts epoch ms → TIMESTAMP (recommended in Flink 1.15)
  event_time AS TO_TIMESTAMP_LTZ(txn_time, 3),

  -- Watermark: allow up to 10 seconds of out-of-order events
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND

) WITH (
  -- Kafka connection
  'connector'                                  = 'kafka',
  'topic'                                      = '${KAFKA_TOPIC_TXN}',
  'properties.bootstrap.servers'               = '${KAFKA_BROKERS}',

  -- Security: SASL_SSL + Kerberos GSSAPI
  'properties.security.protocol'               = 'SASL_SSL',
  'properties.sasl.mechanism'                  = 'GSSAPI',
  'properties.sasl.kerberos.service.name'      = 'kafka',

  -- Auto-TLS: use in-cluster truststore (SSB server is on the same cluster)
  'properties.ssl.truststore.location'         = '${INCLUSTER_TRUSTSTORE_JKS}',
  'properties.ssl.truststore.password'         = '${TRUSTSTORE_PW}',

  -- Start reading from the latest offset
  'scan.startup.mode'                          = 'latest-offset',

  -- Data format
  'format'                                     = 'json',
  'json.ignore-parse-errors'                   = 'true'
);
