-- ================================================================
-- 02_large_cash_job.sql — Pattern 1: Large Cash Detection
-- Flink 1.15.1 (CSA 1.9.0.1)
--
-- Detection rule:
--   amount >= ${LARGE_CASH_THRESHOLD} (default: ₹10,00,000 = RBI CTR threshold)
--
-- Behavior:
--   Reads transactions from Kafka in real time and immediately
--   records any high-value transaction in the aml_alerts table.
-- ================================================================

-- Run 01_kafka_source_table.sql before executing this in SSB Web UI.

INSERT INTO `kudu-aml`.`default`.`aml_alerts`
SELECT
  -- alert_id: 'LC-' + transaction_id (ensures uniqueness)
  CONCAT('LC-', transaction_id)         AS alert_id,

  account_id,

  -- Alert type
  'LARGE_CASH'                          AS alert_type,

  -- Transaction amount
  amount,

  -- Transaction count (Large Cash is always a single transaction)
  1                                     AS txn_count,

  -- Window time (single transaction — use txn_time directly)
  txn_time                              AS window_start,
  txn_time                              AS window_end,

  -- Alert creation time (epoch ms)
  UNIX_TIMESTAMP() * 1000               AS created_at

FROM kafka_aml_transactions

-- Core detection rule: amount at or above threshold
WHERE amount >= ${LARGE_CASH_THRESHOLD};

-- ================================================================
-- Demo talking point:
--   This WHERE clause IS the entire detection logic.
--   Highlight: "Real-time AML detection with a single line of SQL."
-- ================================================================
