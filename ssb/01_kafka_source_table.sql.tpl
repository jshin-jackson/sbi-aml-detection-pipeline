-- ================================================================
-- 01_kafka_source_table.sql — SSB Kafka Source Table 생성
-- Flink 1.15.1 (CSA 1.9.0.1) 문법
--
-- 실행 방법:
--   [Primary]   bash ssb/render_sql.sh 실행 후 SSB Web UI에 붙여넣기
--   [Secondary] python ssb/ssb_rest_client.py (자동 렌더링 + 제출)
-- ================================================================
-- 참고: ${변수} 는 envsubst 또는 Python에서 자동으로 실제 값으로 치환됩니다.
-- ================================================================

-- 기존 테이블이 있으면 삭제 후 재생성
DROP TABLE IF EXISTS kafka_aml_transactions;

CREATE TABLE kafka_aml_transactions (
  -- 거래 데이터 필드
  transaction_id  STRING     COMMENT '거래 고유 ID',
  account_id      STRING     COMMENT '계좌 ID',
  amount          DOUBLE     COMMENT '거래 금액 (INR)',
  txn_time        BIGINT     COMMENT '거래 시각 (epoch ms)',
  channel         STRING     COMMENT '거래 채널',
  merchant_city   STRING     COMMENT '거래 도시',

  -- 이벤트 타임 (Flink 윈도우 연산에 사용)
  -- TO_TIMESTAMP_LTZ: epoch ms → TIMESTAMP 변환 (Flink 1.15 권장 함수)
  event_time AS TO_TIMESTAMP_LTZ(txn_time, 3),

  -- Watermark: 10초 지연 허용 (네트워크 지연 대응)
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND

) WITH (
  -- Kafka 연결
  'connector'                                  = 'kafka',
  'topic'                                      = '${KAFKA_TOPIC_TXN}',
  'properties.bootstrap.servers'               = '${KAFKA_BROKERS}',

  -- 보안: SASL_SSL + Kerberos GSSAPI
  'properties.security.protocol'               = 'SASL_SSL',
  'properties.sasl.mechanism'                  = 'GSSAPI',
  'properties.sasl.kerberos.service.name'      = 'kafka',

  -- Auto-TLS: In-cluster truststore 사용 (SSB 서버와 동일 클러스터)
  'properties.ssl.truststore.location'         = '${INCLUSTER_TRUSTSTORE_JKS}',
  'properties.ssl.truststore.password'         = '${TRUSTSTORE_PW}',

  -- 읽기 시작 위치: 최신 메시지부터
  'scan.startup.mode'                          = 'latest-offset',

  -- 데이터 형식
  'format'                                     = 'json',
  'json.ignore-parse-errors'                   = 'true'
);
