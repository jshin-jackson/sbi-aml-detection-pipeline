-- ================================================================
-- 03_large_cash_job.sql — Pattern 1: Large Cash Detection
-- Flink SQL Client (Standalone)
--
-- Prerequisite: Run 01_kafka_source.sql and 02_kudu_aml_alerts.sql first
--
-- Detection rule:
--   amount >= 1000000 INR (₹10 lakh, RBI CTR threshold)
--
-- Behavior:
--   Reads transactions from Kafka in real time and immediately
--   records any high-value transaction in the aml_alerts table.
-- ================================================================

INSERT INTO aml_alerts
SELECT
  -- alert_id: 'LC-' + transaction_id (ensures uniqueness)
  CONCAT('LC-', transaction_id)  AS alert_id,

  account_id,

  -- Alert type
  'LARGE_CASH'                   AS alert_type,

  -- Transaction amount
  amount,

  -- Transaction count (Large Cash is always a single transaction)
  1                              AS txn_count,

  -- Window time (single transaction — use txn_time directly)
  txn_time                       AS window_start,
  txn_time                       AS window_end,

  -- Alert creation time (epoch ms)
  UNIX_TIMESTAMP() * 1000        AS created_at

FROM kafka_aml_transactions

-- Core detection rule: ₹10 lakh or above
WHERE amount >= 1000000;
