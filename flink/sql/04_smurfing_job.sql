-- ================================================================
-- 04_smurfing_job.sql — Pattern 2: Smurfing 탐지
-- Flink SQL Client (Standalone)
--
-- 전제: 01_kafka_source.sql, 02_kudu_aml_alerts.sql 먼저 실행
--
-- 탐지 기준:
--   동일 계좌에서 30분 내에 5회 이상 거래
--   (각 건은 임계값 미만이지만 합산하면 의심 패턴)
--
-- 방법:
--   TUMBLE 윈도우: 30분 단위 고정 시간창으로 계좌별 거래 집계
--   → 5회 이상이면 Alert 발생
-- ================================================================

INSERT INTO aml_alerts
SELECT
  -- alert_id: 'SM-' + 계좌ID + 윈도우 시작 시각 (30분 창마다 고유 ID)
  CONCAT(
    'SM-',
    account_id,
    '-',
    CAST(TUMBLE_START(event_time, INTERVAL '30' MINUTE) AS STRING)
  )                              AS alert_id,

  account_id,

  -- 알람 유형
  'SMURFING'                     AS alert_type,

  -- 30분 창 내 총 거래 금액
  SUM(amount)                    AS amount,

  -- 30분 창 내 거래 건수
  CAST(COUNT(*) AS INT)          AS txn_count,

  -- 알람 생성 시각 (epoch ms)
  UNIX_TIMESTAMP() * 1000        AS window_start,
  UNIX_TIMESTAMP() * 1000        AS window_end,
  UNIX_TIMESTAMP() * 1000        AS created_at

FROM kafka_aml_transactions

-- 계좌 + 30분 고정 시간창으로 그룹핑
GROUP BY
  account_id,
  TUMBLE(event_time, INTERVAL '30' MINUTE)

-- 핵심 탐지 조건: 30분 내 5회 이상
HAVING COUNT(*) >= 5;
