-- ================================================================
-- 02_kudu_aml_alerts.sql — Create Kudu Sink Table (aml_alerts)
-- Flink SQL Client (Standalone) — run at the start of every session
--
-- Prerequisite: Kudu table must already exist (created via Impala DDL).
--   → Run infra/02_run_kudu_ddl.sh, or create manually in Hue Impala Editor
-- ================================================================
-- Kudu connector options:
--   'connector'  = 'kudu'
--   'masters'    = '<kudu-master:port>'   (NOT kudu.masters — important!)
--   'table-name' = '<kudu-table-name>'    (use 'default.*' format if created by Impala)
-- ================================================================

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
  -- Kudu Master address (update for customer environment)
  'masters'    = 'ccycloud-1.jshin.root.comops.site:7051',
  -- Kudu table name: use 'default.<table>' format when created by Impala
  'table-name' = 'default.aml_alerts'
);
