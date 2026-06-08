-- ================================================================
-- 02_kudu_ddl.sql — Create Kudu Tables
-- Run via: bash infra/02_run_kudu_ddl.sh
-- ================================================================
-- The ${KUDU_MASTERS} variable is automatically substituted
-- by the runner script using sed.
-- ================================================================

-- Drop existing tables (safe to re-run)
DROP TABLE IF EXISTS default.aml_transactions;
DROP TABLE IF EXISTS default.aml_alerts;
DROP TABLE IF EXISTS default.aml_risk_score;

-- ----------------------------------------------------------------
-- Table 1: Raw Transactions (stores all transactions)
-- ----------------------------------------------------------------
CREATE TABLE default.aml_transactions (
  transaction_id  STRING     COMMENT 'Unique transaction ID (UUID)',
  account_id      STRING NOT NULL COMMENT 'Account ID',
  amount          DOUBLE NOT NULL COMMENT 'Transaction amount (INR)',
  txn_time        BIGINT NOT NULL COMMENT 'Transaction timestamp (epoch ms)',
  channel         STRING     COMMENT 'Channel: ATM/NEFT/IMPS/UPI/RTGS/BRANCH',
  merchant_city   STRING     COMMENT 'Transaction city',
  location_lat    DOUBLE     COMMENT 'Latitude',
  location_lon    DOUBLE     COMMENT 'Longitude',
  PRIMARY KEY (transaction_id)
)
PARTITION BY HASH(transaction_id) PARTITIONS 4
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses'='${KUDU_MASTERS}');

-- ----------------------------------------------------------------
-- Table 2: AML Alerts
-- ----------------------------------------------------------------
CREATE TABLE default.aml_alerts (
  alert_id        STRING     COMMENT 'Unique alert ID',
  account_id      STRING NOT NULL COMMENT 'Suspicious account ID',
  alert_type      STRING NOT NULL COMMENT 'LARGE_CASH or SMURFING',
  amount          DOUBLE     COMMENT 'Transaction amount or total (INR)',
  txn_count       INT        COMMENT 'Transaction count (for Smurfing)',
  window_start    BIGINT     COMMENT 'Detection window start (epoch ms)',
  window_end      BIGINT     COMMENT 'Detection window end (epoch ms)',
  created_at      BIGINT NOT NULL COMMENT 'Alert creation time (epoch ms)',
  PRIMARY KEY (alert_id)
)
PARTITION BY HASH(alert_id) PARTITIONS 2
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses'='${KUDU_MASTERS}');

-- ----------------------------------------------------------------
-- Table 3: Per-account Risk Scores (upsert target)
-- ----------------------------------------------------------------
CREATE TABLE default.aml_risk_score (
  account_id      STRING     COMMENT 'Account ID (PK)',
  risk_score      DOUBLE NOT NULL COMMENT 'Risk score (0–100)',
  last_alert_type STRING     COMMENT 'Most recent alert type',
  alert_count     INT        COMMENT 'Total alert count',
  updated_at      BIGINT NOT NULL COMMENT 'Last update timestamp (epoch ms)',
  PRIMARY KEY (account_id)
)
PARTITION BY HASH(account_id) PARTITIONS 2
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses'='${KUDU_MASTERS}');

-- ----------------------------------------------------------------
-- Verify creation
-- ----------------------------------------------------------------
SHOW TABLES IN default LIKE 'aml_*';
