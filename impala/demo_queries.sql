-- ================================================================
-- demo_queries.sql — Demo Verification and Real-time Result Queries
-- Run via: bash scripts/run_impala.sh
-- Or directly in Hue (https://<hue-host>:8889)
-- ================================================================

-- ----------------------------------------------------------------
-- [Query 1] Overall AML Alert Summary (main demo screen)
-- This is the first query to show customers.
-- ----------------------------------------------------------------
SELECT
  alert_type                                 AS "Alert Type",
  COUNT(*)                                   AS "Alert Count",
  ROUND(SUM(amount), 0)                      AS "Total Amount (INR)",
  ROUND(AVG(amount), 0)                      AS "Avg Amount (INR)",
  MAX(FROM_UNIXTIME(created_at / 1000))      AS "Latest Alert Time"
FROM default.aml_alerts
GROUP BY alert_type
ORDER BY COUNT(*) DESC;

-- ----------------------------------------------------------------
-- [Query 2] Large Cash — Immediate high-value transaction alerts
-- Talking point: "Detected in real time, the moment the transaction occurs."
-- ----------------------------------------------------------------
SELECT
  alert_id                                   AS "Alert ID",
  account_id                                 AS "Suspicious Account",
  CONCAT('₹', FORMAT(amount, 0))            AS "Transaction Amount (INR)",
  FROM_UNIXTIME(created_at / 1000)           AS "Detected At"
FROM default.aml_alerts
WHERE alert_type = 'LARGE_CASH'
ORDER BY created_at DESC
LIMIT 10;

-- ----------------------------------------------------------------
-- [Query 3] Smurfing — Distributed small-transaction pattern
-- Talking point: "Even small amounts are flagged if the pattern is suspicious."
-- ----------------------------------------------------------------
SELECT
  account_id                                 AS "Suspicious Account",
  txn_count                                  AS "Transactions in 30 min",
  CONCAT('₹', FORMAT(amount, 0))            AS "Total Amount (INR)",
  FROM_UNIXTIME(window_start / 1000)         AS "Window Start",
  FROM_UNIXTIME(window_end   / 1000)         AS "Window End",
  FROM_UNIXTIME(created_at   / 1000)         AS "Detected At"
FROM default.aml_alerts
WHERE alert_type = 'SMURFING'
ORDER BY created_at DESC
LIMIT 10;

-- ----------------------------------------------------------------
-- [Query 4] Top 10 High-Risk Accounts by Risk Score
-- Talking point: "Instantly identify which accounts are most at risk."
-- ----------------------------------------------------------------
SELECT
  account_id                                 AS "Account ID",
  ROUND(risk_score, 1)                       AS "Risk Score",
  last_alert_type                            AS "Last Alert Type",
  alert_count                                AS "Total Alerts",
  FROM_UNIXTIME(updated_at / 1000)           AS "Last Updated"
FROM default.aml_risk_score
ORDER BY risk_score DESC
LIMIT 10;

-- ----------------------------------------------------------------
-- [Query 5] Real-time Processing Statistics (show at the end of demo)
-- ----------------------------------------------------------------
SELECT
  COUNT(DISTINCT account_id)                 AS "Accounts Analyzed",
  COUNT(*)                                   AS "Total Alerts",
  COUNT(CASE WHEN alert_type = 'LARGE_CASH' THEN 1 END) AS "Large Cash Alerts",
  COUNT(CASE WHEN alert_type = 'SMURFING'   THEN 1 END) AS "Smurfing Alerts",
  MIN(FROM_UNIXTIME(created_at / 1000))      AS "First Alert At",
  MAX(FROM_UNIXTIME(created_at / 1000))      AS "Latest Alert At"
FROM default.aml_alerts;
