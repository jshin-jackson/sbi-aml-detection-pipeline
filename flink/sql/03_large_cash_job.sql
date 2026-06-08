-- ================================================================
-- 03_large_cash_job.sql — Pattern 1: Large Cash 탐지
-- Flink SQL Client (Standalone)
--
-- 전제: 01_kafka_source.sql, 02_kudu_aml_alerts.sql 먼저 실행
--
-- 탐지 기준:
--   amount >= 1000000 INR (₹10 lakh, RBI CTR 기준)
--
-- 동작:
--   Kafka에서 거래를 실시간으로 읽어
--   고액 거래 발생 즉시 aml_alerts 테이블에 기록
-- ================================================================

INSERT INTO aml_alerts
SELECT
  -- alert_id: 'LC-' + 거래ID (중복 방지)
  CONCAT('LC-', transaction_id)  AS alert_id,

  account_id,

  -- 알람 유형
  'LARGE_CASH'                   AS alert_type,

  -- 거래 금액
  amount,

  -- 거래 건수 (Large Cash는 단건)
  1                              AS txn_count,

  -- 시간창 (단건이므로 거래 시각 그대로)
  txn_time                       AS window_start,
  txn_time                       AS window_end,

  -- 알람 생성 시각 (epoch ms)
  UNIX_TIMESTAMP() * 1000        AS created_at

FROM kafka_aml_transactions

-- 핵심 탐지 조건: ₹10 lakh 이상
WHERE amount >= 1000000;
