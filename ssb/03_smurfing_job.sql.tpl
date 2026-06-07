-- ================================================================
-- 03_smurfing_job.sql — Pattern 2: Smurfing 탐지
-- Flink 1.15.1 (CSA 1.9.0.1)
--
-- 탐지 기준:
--   동일 계좌에서 ${SMURFING_WINDOW_MIN}분 내에 ${SMURFING_TXN_COUNT}회 이상 거래
--   (각 건은 임계값 미만이지만 합산하면 의심 패턴)
--
-- 방법:
--   TUMBLE 윈도우: 30분 단위 고정 시간창으로 집계
--   → 같은 계좌의 거래를 그룹핑하여 횟수 기준 초과 시 Alert
-- ================================================================

-- SSB Web UI에서 실행 전 01_kafka_source_table.sql을 먼저 실행하세요.

INSERT INTO `kudu-aml`.`default`.`aml_alerts`
SELECT
  -- alert_id: 'SM-' + 계좌ID + 윈도우 시작 시각 (30분 창마다 고유 ID)
  CONCAT(
    'SM-',
    account_id,
    '-',
    CAST(TUMBLE_START(event_time, INTERVAL '${SMURFING_WINDOW_MIN}' MINUTE) AS STRING)
  )                                       AS alert_id,

  account_id,

  -- 알람 유형
  'SMURFING'                             AS alert_type,

  -- 30분 창 내 총 거래 금액
  SUM(amount)                            AS amount,

  -- 30분 창 내 거래 건수
  CAST(COUNT(*) AS INT)                  AS txn_count,

  -- 30분 시간창 시작/종료
  UNIX_TIMESTAMP(
    CAST(TUMBLE_START(event_time, INTERVAL '${SMURFING_WINDOW_MIN}' MINUTE) AS STRING)
  ) * 1000                               AS window_start,

  UNIX_TIMESTAMP(
    CAST(TUMBLE_END(event_time, INTERVAL '${SMURFING_WINDOW_MIN}' MINUTE) AS STRING)
  ) * 1000                               AS window_end,

  -- 알람 생성 시각
  UNIX_TIMESTAMP() * 1000               AS created_at

FROM kafka_aml_transactions

-- 계좌 + 30분 고정 시간창으로 그룹핑
GROUP BY
  account_id,
  TUMBLE(event_time, INTERVAL '${SMURFING_WINDOW_MIN}' MINUTE)

-- 핵심 탐지 조건: 30분 내 5회 이상
HAVING COUNT(*) >= ${SMURFING_TXN_COUNT};

-- ================================================================
-- Demo 포인트:
--   TUMBLE(event_time, INTERVAL '30' MINUTE) 이 Flink의 시간창 집계입니다.
--   "30분마다 계좌별 거래 건수를 세어 5회 이상이면 즉시 Alert"
--   고객에게 이 SQL이 Smurfing 방지 로직 전체임을 강조하세요.
-- ================================================================
