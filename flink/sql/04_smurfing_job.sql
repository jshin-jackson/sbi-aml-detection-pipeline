-- ================================================================
-- 04_smurfing_job.sql — Pattern 2: Smurfing Detection
-- Flink SQL Client (Standalone)
--
-- Prerequisite: Run 01_kafka_source.sql and 02_kudu_aml_alerts.sql first
--
-- Detection rule:
--   Same account makes 5+ transactions within 30 minutes
--   (each below the threshold — intentional evasion behavior)
--
-- Method:
--   TUMBLE window: aggregate transactions per account in fixed 30-min windows.
--   Raise an alert when transaction count reaches 5 or more.
-- ================================================================

INSERT INTO aml_alerts
SELECT
  -- alert_id: 'SM-' + account_id + window start time (unique per 30-min window)
  CONCAT(
    'SM-',
    account_id,
    '-',
    CAST(TUMBLE_START(event_time, INTERVAL '30' MINUTE) AS STRING)
  )                              AS alert_id,

  account_id,

  -- Alert type
  'SMURFING'                     AS alert_type,

  -- Total amount in the 30-minute window
  SUM(amount)                    AS amount,

  -- Number of transactions in the window
  CAST(COUNT(*) AS INT)          AS txn_count,

  -- Alert creation time (epoch ms)
  UNIX_TIMESTAMP() * 1000        AS window_start,
  UNIX_TIMESTAMP() * 1000        AS window_end,
  UNIX_TIMESTAMP() * 1000        AS created_at

FROM kafka_aml_transactions

-- Group by account + fixed 30-minute window
GROUP BY
  account_id,
  TUMBLE(event_time, INTERVAL '30' MINUTE)

-- Core detection rule: 5+ transactions in 30 minutes
HAVING COUNT(*) >= 5;
