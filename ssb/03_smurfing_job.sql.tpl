-- ================================================================
-- 03_smurfing_job.sql — Pattern 2: Smurfing Detection
-- Flink 1.15.1 (CSA 1.9.0.1)
--
-- Detection rule:
--   Same account makes ${SMURFING_TXN_COUNT}+ transactions within ${SMURFING_WINDOW_MIN} minutes
--   (Each transaction is below the threshold — intentional evasion behavior)
--
-- Method:
--   TUMBLE window: aggregate transactions per account in fixed 30-minute windows.
--   → Raise an alert when transaction count exceeds threshold.
-- ================================================================

-- Run 01_kafka_source_table.sql before executing this in SSB Web UI.

INSERT INTO `kudu-aml`.`default`.`aml_alerts`
SELECT
  -- alert_id: 'SM-' + account_id + window start time (unique per 30-min window)
  CONCAT(
    'SM-',
    account_id,
    '-',
    CAST(TUMBLE_START(event_time, INTERVAL '${SMURFING_WINDOW_MIN}' MINUTE) AS STRING)
  )                                       AS alert_id,

  account_id,

  -- Alert type
  'SMURFING'                             AS alert_type,

  -- Total amount within the 30-minute window
  SUM(amount)                            AS amount,

  -- Number of transactions within the window
  CAST(COUNT(*) AS INT)                  AS txn_count,

  -- Window boundaries (epoch ms)
  UNIX_TIMESTAMP(
    CAST(TUMBLE_START(event_time, INTERVAL '${SMURFING_WINDOW_MIN}' MINUTE) AS STRING)
  ) * 1000                               AS window_start,

  UNIX_TIMESTAMP(
    CAST(TUMBLE_END(event_time, INTERVAL '${SMURFING_WINDOW_MIN}' MINUTE) AS STRING)
  ) * 1000                               AS window_end,

  -- Alert creation time (epoch ms)
  UNIX_TIMESTAMP() * 1000               AS created_at

FROM kafka_aml_transactions

-- Group by account + 30-minute fixed window
GROUP BY
  account_id,
  TUMBLE(event_time, INTERVAL '${SMURFING_WINDOW_MIN}' MINUTE)

-- Core detection rule: 5+ transactions within 30 minutes
HAVING COUNT(*) >= ${SMURFING_TXN_COUNT};

-- ================================================================
-- Demo talking point:
--   TUMBLE(event_time, INTERVAL '30' MINUTE) is Flink's time-window aggregation.
--   Explain: "Counts transactions per account every 30 minutes.
--             Alerts immediately when count reaches 5."
-- ================================================================
