-- ================================================================
-- 01_kafka_source.sql — Create Kafka Source Table
-- Flink SQL Client (Standalone) — run at the start of every session
--
-- How to run: Connect with /opt/flink/bin/sql-client.sh, then paste
-- ================================================================
-- Environment variables are not available in Flink SQL Client.
-- All values are hardcoded directly. Update when switching environments.
-- ================================================================

CREATE TABLE kafka_aml_transactions (
  -- Transaction data fields
  transaction_id  STRING,
  account_id      STRING,
  amount          DOUBLE,
  txn_time        BIGINT,
  channel         STRING,

  -- Event time (used for Flink window operations)
  -- TO_TIMESTAMP_LTZ: converts epoch ms → TIMESTAMP
  event_time AS TO_TIMESTAMP_LTZ(txn_time, 3),

  -- Watermark: allow up to 10 seconds of late events
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND

) WITH (
  'connector'                              = 'kafka',
  'topic'                                  = 'sbi-aml-transactions',

  -- Kafka broker addresses (update for customer environment)
  'properties.bootstrap.servers'           = 'ccycloud-1.jshin.root.comops.site:9093,ccycloud-2.jshin.root.comops.site:9093,ccycloud-3.jshin.root.comops.site:9093',

  -- Security: Kerberos + Auto-TLS
  'properties.security.protocol'           = 'SASL_SSL',
  'properties.sasl.mechanism'              = 'GSSAPI',
  'properties.sasl.kerberos.service.name'  = 'kafka',
  'properties.ssl.truststore.location'     = '/var/lib/cloudera-scm-agent/agent-cert/cm-auto-in_cluster_truststore.jks',
  'properties.ssl.truststore.password'     = 'zpXWTjeWPjvNDU4mQnDQPQKn50xfVI9HYX12DSc05x3',

  -- earliest-offset: reads all existing Kafka data from the beginning
  -- latest-offset:   reads only new data arriving after job start
  'scan.startup.mode'                      = 'earliest-offset',

  'format'                                 = 'json',
  'json.ignore-parse-errors'               = 'true'
);
