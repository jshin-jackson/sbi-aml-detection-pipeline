-- ================================================================
-- 02_kudu_aml_alerts.sql — Kudu Sink Table 생성 (aml_alerts)
-- Flink SQL Client (Standalone) — 매 세션 시작 시 실행
--
-- 전제: Kudu 테이블이 이미 Impala DDL로 생성되어 있어야 합니다.
--   → infra/02_run_kudu_ddl.sh 실행 또는 Hue Impala Editor에서 DDL 실행
-- ================================================================
-- Kudu 커넥터 옵션:
--   'connector'  = 'kudu'
--   'masters'    = '<kudu-master:port>'   (kudu.masters 아님 — 주의)
--   'table-name' = '<kudu-table-name>'    (Impala 생성 시 default.* 형식)
-- ================================================================

CREATE TABLE aml_alerts (
  alert_id     STRING,
  account_id   STRING,
  alert_type   STRING,
  amount       DOUBLE,
  txn_count    INT,
  window_start BIGINT,
  window_end   BIGINT,
  created_at   BIGINT,
  PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
  'connector'  = 'kudu',
  -- Kudu Master 주소 (고객 환경에서 변경)
  'masters'    = 'ccycloud-1.jshin.root.comops.site:7051',
  -- Kudu 테이블명: Impala로 생성 시 'default.<table>' 형식
  'table-name' = 'default.aml_alerts'
);
