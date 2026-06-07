#!/usr/bin/env bash
# ================================================================
# 01_kafka_setup.sh — Kafka 토픽 생성
# Phase 2에서 한 번만 실행합니다.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/env.conf"

TRUSTSTORE_PW=$(cat "${TRUSTSTORE_PW_FILE}")

echo ""
echo "================================================================"
echo " Kafka 토픽 생성 (ENV: ${ENV_NAME})"
echo " Broker: ${KAFKA_BROKERS}"
echo "================================================================"

# Kerberos 인증
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
echo "[Kerberos] kinit 완료: ${PRINCIPAL}"

# 임시 Kafka client 설정 파일 생성
KAFKA_CLIENT_CONF=$(mktemp /tmp/kafka-client-XXXXXX.properties)
cat > "${KAFKA_CLIENT_CONF}" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
ssl.truststore.location=${TRUSTSTORE_JKS}
ssl.truststore.password=${TRUSTSTORE_PW}
EOF

cleanup() {
  rm -f "${KAFKA_CLIENT_CONF}"
}
trap cleanup EXIT

# 토픽 생성 함수
create_topic() {
  local topic="$1"
  local partitions="$2"
  local rf="$3"
  local retention_ms="${4:-86400000}"  # 기본 1일

  echo ""
  echo "Creating topic: ${topic}"
  kafka-topics \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${rf}" \
    --config retention.ms="${retention_ms}" \
    --config compression.type=snappy

  echo "  [OK] ${topic} (partitions=${partitions}, rf=${rf})"
}

# 거래 원본 토픽 (메인 스트림)
create_topic "${KAFKA_TOPIC_TXN}"    "${KAFKA_PARTITIONS}" "${KAFKA_RF}" "86400000"   # 보존: 1일

# Alert 토픽 (SSB 출력 — 선택적)
create_topic "${KAFKA_TOPIC_ALERTS}" "2"                   "${KAFKA_RF}" "604800000"  # 보존: 7일

# 생성된 토픽 목록 확인
echo ""
echo "=== 생성된 토픽 목록 ==="
kafka-topics \
  --bootstrap-server "${KAFKA_BROKERS}" \
  --command-config "${KAFKA_CLIENT_CONF}" \
  --list | grep "sbi.aml"

echo ""
echo "[완료] Kafka 토픽 생성 완료!"
echo "  다음 단계: bash infra/02_run_kudu_ddl.sh"
