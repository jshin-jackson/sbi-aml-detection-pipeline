-- ================================================================
-- demo_queries.sql — Demo 검증 및 실시간 결과 확인 쿼리
-- 실행: bash scripts/run_impala.sh
-- 또는: Hue (https://<hue-host>:8889) 에서 직접 실행
-- ================================================================

-- ----------------------------------------------------------------
-- [Query 1] 전체 AML Alert 현황 (Demo 메인 화면)
-- 고객에게 가장 먼저 보여주는 쿼리입니다.
-- ----------------------------------------------------------------
SELECT
  alert_type                                 AS "탐지 유형",
  COUNT(*)                                   AS "알람 건수",
  ROUND(SUM(amount), 0)                      AS "총 금액 (INR)",
  ROUND(AVG(amount), 0)                      AS "평균 금액 (INR)",
  MAX(FROM_UNIXTIME(created_at / 1000))      AS "최근 알람 시각"
FROM default.aml_alerts
GROUP BY alert_type
ORDER BY COUNT(*) DESC;

-- ----------------------------------------------------------------
-- [Query 2] Large Cash — 고액 거래 즉시 탐지 결과
-- "거래 발생 즉시 실시간으로 탐지됩니다"를 강조하세요.
-- ----------------------------------------------------------------
SELECT
  alert_id                                   AS "알람 ID",
  account_id                                 AS "의심 계좌",
  CONCAT('₹', FORMAT(amount, 0))            AS "거래 금액 (INR)",
  FROM_UNIXTIME(created_at / 1000)           AS "탐지 시각"
FROM default.aml_alerts
WHERE alert_type = 'LARGE_CASH'
ORDER BY created_at DESC
LIMIT 10;

-- ----------------------------------------------------------------
-- [Query 3] Smurfing — 30분 내 분산 거래 탐지 결과
-- "겉으로는 소액이지만 패턴을 감지합니다"를 강조하세요.
-- ----------------------------------------------------------------
SELECT
  account_id                                 AS "의심 계좌",
  txn_count                                  AS "30분 내 거래 건수",
  CONCAT('₹', FORMAT(amount, 0))            AS "합계 금액 (INR)",
  FROM_UNIXTIME(window_start / 1000)         AS "창 시작",
  FROM_UNIXTIME(window_end   / 1000)         AS "창 종료",
  FROM_UNIXTIME(created_at   / 1000)         AS "탐지 시각"
FROM default.aml_alerts
WHERE alert_type = 'SMURFING'
ORDER BY created_at DESC
LIMIT 10;

-- ----------------------------------------------------------------
-- [Query 4] 계좌별 리스크 점수 Top 10
-- "어떤 계좌가 가장 위험한지 즉시 확인 가능합니다"
-- ----------------------------------------------------------------
SELECT
  account_id                                 AS "계좌 ID",
  ROUND(risk_score, 1)                       AS "리스크 점수",
  last_alert_type                            AS "최근 알람 유형",
  alert_count                                AS "총 알람 횟수",
  FROM_UNIXTIME(updated_at / 1000)           AS "최근 업데이트"
FROM default.aml_risk_score
ORDER BY risk_score DESC
LIMIT 10;

-- ----------------------------------------------------------------
-- [Query 5] 실시간 처리 통계 (Demo 마지막에 보여주세요)
-- ----------------------------------------------------------------
SELECT
  COUNT(DISTINCT account_id)                 AS "분석된 계좌 수",
  COUNT(*)                                   AS "총 알람 수",
  COUNT(CASE WHEN alert_type = 'LARGE_CASH' THEN 1 END) AS "고액 거래 알람",
  COUNT(CASE WHEN alert_type = 'SMURFING'   THEN 1 END) AS "Smurfing 알람",
  MIN(FROM_UNIXTIME(created_at / 1000))      AS "첫 알람 시각",
  MAX(FROM_UNIXTIME(created_at / 1000))      AS "최근 알람 시각"
FROM default.aml_alerts;
