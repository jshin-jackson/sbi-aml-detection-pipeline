-- ================================================================
-- 02_large_cash_job.sql — Pattern 1: Large Cash 탐지
-- Flink 1.15.1 (CSA 1.9.0.1)
--
-- 탐지 기준:
--   amount >= ${LARGE_CASH_THRESHOLD} (기본: ₹10,00,000 = RBI CTR 기준)
--
-- 동작:
--   Kafka에서 거래를 실시간으로 읽어
--   고액 거래 발생 즉시 aml_alerts 테이블에 기록
-- ================================================================

-- SSB Web UI에서 실행 전 01_kafka_source_table.sql을 먼저 실행하세요.

INSERT INTO `kudu-aml`.`default`.`aml_alerts`
SELECT
  -- alert_id: 'LC-' + 거래ID (중복 방지)
  CONCAT('LC-', transaction_id)         AS alert_id,

  account_id,

  -- 알람 유형
  'LARGE_CASH'                          AS alert_type,

  -- 거래 금액
  amount,

  -- 거래 건수 (Large Cash는 단건)
  1                                     AS txn_count,

  -- 시간창 (단건이므로 거래 시각 그대로 사용)
  txn_time                              AS window_start,
  txn_time                              AS window_end,

  -- 알람 생성 시각
  UNIX_TIMESTAMP() * 1000               AS created_at

FROM kafka_aml_transactions

-- 핵심 탐지 조건: 임계값 이상 거래
WHERE amount >= ${LARGE_CASH_THRESHOLD};

-- ================================================================
-- Demo 포인트:
--   이 SQL 한 줄(WHERE 절)이 핵심 탐지 로직입니다.
--   고객에게 "코딩 없이 SQL 한 줄로 실시간 AML 탐지"를 강조하세요.
-- ================================================================
