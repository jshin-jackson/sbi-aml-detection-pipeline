-- ================================================================
-- 02_kudu_ddl.sql — Kudu 테이블 생성
-- 실행: bash infra/02_run_kudu_ddl.sh
-- ================================================================
-- ${KUDU_MASTERS} 변수는 실행 스크립트에서 envsubst로 자동 치환됩니다.
-- ================================================================

-- 기존 테이블 삭제 (재실행 시)
DROP TABLE IF EXISTS default.aml_transactions;
DROP TABLE IF EXISTS default.aml_alerts;
DROP TABLE IF EXISTS default.aml_risk_score;

-- ----------------------------------------------------------------
-- 테이블 1: 거래 원본 (모든 거래 저장)
-- ----------------------------------------------------------------
CREATE TABLE default.aml_transactions (
  transaction_id  STRING     COMMENT '거래 고유 ID (UUID)',
  account_id      STRING NOT NULL COMMENT '계좌 ID',
  amount          DOUBLE NOT NULL COMMENT '거래 금액 (INR)',
  txn_time        BIGINT NOT NULL COMMENT '거래 시각 (epoch ms)',
  channel         STRING     COMMENT '채널: ATM/NEFT/IMPS/UPI/RTGS/BRANCH',
  merchant_city   STRING     COMMENT '거래 도시',
  location_lat    DOUBLE     COMMENT '위도',
  location_lon    DOUBLE     COMMENT '경도',
  PRIMARY KEY (transaction_id)
)
PARTITION BY HASH(transaction_id) PARTITIONS 4
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses'='${KUDU_MASTERS}');

-- ----------------------------------------------------------------
-- 테이블 2: AML 탐지 알람
-- ----------------------------------------------------------------
CREATE TABLE default.aml_alerts (
  alert_id        STRING     COMMENT '알람 고유 ID',
  account_id      STRING NOT NULL COMMENT '의심 계좌 ID',
  alert_type      STRING NOT NULL COMMENT 'LARGE_CASH 또는 SMURFING',
  amount          DOUBLE     COMMENT '거래 금액 또는 합계 (INR)',
  txn_count       INT        COMMENT '거래 건수 (Smurfing 시)',
  window_start    BIGINT     COMMENT '탐지 시간창 시작 (epoch ms)',
  window_end      BIGINT     COMMENT '탐지 시간창 종료 (epoch ms)',
  created_at      BIGINT NOT NULL COMMENT '알람 생성 시각 (epoch ms)',
  PRIMARY KEY (alert_id)
)
PARTITION BY HASH(alert_id) PARTITIONS 2
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses'='${KUDU_MASTERS}');

-- ----------------------------------------------------------------
-- 테이블 3: 계좌별 리스크 점수 (upsert 대상)
-- ----------------------------------------------------------------
CREATE TABLE default.aml_risk_score (
  account_id      STRING     COMMENT '계좌 ID (PK)',
  risk_score      DOUBLE NOT NULL COMMENT '리스크 점수 (0~100)',
  last_alert_type STRING     COMMENT '최근 알람 유형',
  alert_count     INT        COMMENT '총 알람 횟수',
  updated_at      BIGINT NOT NULL COMMENT '마지막 업데이트 시각 (epoch ms)',
  PRIMARY KEY (account_id)
)
PARTITION BY HASH(account_id) PARTITIONS 2
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses'='${KUDU_MASTERS}');

-- ----------------------------------------------------------------
-- 생성 확인
-- ----------------------------------------------------------------
SHOW TABLES IN default LIKE 'aml_*';
