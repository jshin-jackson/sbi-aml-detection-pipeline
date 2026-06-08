-- ================================================================
-- 01_kafka_source.sql — Kafka Source Table 생성
-- Flink SQL Client (Standalone) — 매 세션 시작 시 실행
--
-- 실행: /opt/flink/bin/sql-client.sh 접속 후 붙여넣기
-- ================================================================
-- 환경 변수가 없으므로 실제 값을 직접 입력합니다.
-- 고객 환경 전환 시 아래 값들을 변경하세요.
-- ================================================================

CREATE TABLE kafka_aml_transactions (
  -- 거래 데이터 필드
  transaction_id  STRING,
  account_id      STRING,
  amount          DOUBLE,
  txn_time        BIGINT,
  channel         STRING,

  -- 이벤트 타임 (Flink 윈도우 연산용)
  -- TO_TIMESTAMP_LTZ: epoch ms → TIMESTAMP 변환
  event_time AS TO_TIMESTAMP_LTZ(txn_time, 3),

  -- Watermark: 10초 지연 허용
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND

) WITH (
  'connector'                              = 'kafka',
  'topic'                                  = 'sbi-aml-transactions',

  -- Kafka 브로커 주소 (고객 환경에서 변경)
  'properties.bootstrap.servers'           = 'ccycloud-1.jshin.root.comops.site:9093,ccycloud-2.jshin.root.comops.site:9093,ccycloud-3.jshin.root.comops.site:9093',

  -- Kerberos + Auto-TLS 보안 설정
  'properties.security.protocol'           = 'SASL_SSL',
  'properties.sasl.mechanism'              = 'GSSAPI',
  'properties.sasl.kerberos.service.name'  = 'kafka',
  'properties.ssl.truststore.location'     = '/var/lib/cloudera-scm-agent/agent-cert/cm-auto-in_cluster_truststore.jks',
  'properties.ssl.truststore.password'     = 'zpXWTjeWPjvNDU4mQnDQPQKn50xfVI9HYX12DSc05x3',

  -- earliest-offset: 이미 Kafka에 저장된 데이터도 처음부터 읽음
  -- latest-offset: 이후 새로 들어오는 데이터만 읽음
  'scan.startup.mode'                      = 'earliest-offset',

  'format'                                 = 'json',
  'json.ignore-parse-errors'               = 'true'
);
