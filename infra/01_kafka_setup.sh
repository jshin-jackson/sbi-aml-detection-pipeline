#!/usr/bin/env bash
# =============================================================================
# 01_kafka_setup.sh — Kafka 토픽 생성
# Phase 2에서 한 번만 실행합니다.
#
# 사용법:
#   source config/env.conf
#   bash infra/01_kafka_setup.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# ---------------------------------------------------------------------------
# config 로드
# ---------------------------------------------------------------------------
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf 파일이 없습니다."
  echo "        ln -sf config/env.internal.conf config/env.conf"
  exit 1
fi
source "${ROOT_DIR}/config/env.conf"

# ---------------------------------------------------------------------------
# 색상 출력 헬퍼 (sibling 프로젝트 패턴)
# ---------------------------------------------------------------------------
info()  { echo "[INFO]  $*"; }
ok()    { echo "[OK]    $*"; }
warn()  { echo "[WARN]  $*"; }
err()   { echo "[ERROR] $*" >&2; exit 1; }

KAFKA_TOPICS_CMD="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}/bin/kafka-topics.sh"

echo ""
echo "================================================================"
echo " SBI AML — Kafka 토픽 생성 (ENV: ${ENV_NAME})"
echo " Broker: ${KAFKA_BROKERS}"
echo "================================================================"

# ---------------------------------------------------------------------------
# Kerberos 티켓 확인
# ---------------------------------------------------------------------------
info "Kerberos 티켓 확인..."
if ! klist -s 2>/dev/null; then
  info "TGT 없음 — kinit 실행"
  kinit -kt "${KAFKA_KEYTAB}" "${KAFKA_PRINCIPAL}" \
    || err "kinit 실패 — keytab 확인 필요: ${KAFKA_KEYTAB}"
fi
ok "Kerberos 티켓 유효 (${KAFKA_PRINCIPAL})"

# ---------------------------------------------------------------------------
# 임시 JAAS + client.properties 생성 (conf/ 파일 기반)
# conf/kafka_jaas.conf 와 conf/kafka_kerberos.properties 를 렌더링
# ---------------------------------------------------------------------------
TMPDIR_LOCAL=$(mktemp -d)
trap 'rm -rf "${TMPDIR_LOCAL}"' EXIT

JAAS_CONF="${TMPDIR_LOCAL}/kafka-jaas.conf"
CLIENT_PROPS="${TMPDIR_LOCAL}/kafka-client.properties"

# conf/kafka_jaas.conf 의 변수를 실제 값으로 치환
sed \
  -e "s|\${KAFKA_KEYTAB}|${KAFKA_KEYTAB}|g" \
  -e "s|\${KAFKA_PRINCIPAL}|${KAFKA_PRINCIPAL}|g" \
  "${ROOT_DIR}/conf/kafka_jaas.conf" > "${JAAS_CONF}"

# client.properties 생성 (conf/kafka_kerberos.properties 기반)
sed \
  -e "s|\${KAFKA_KEYTAB}|${KAFKA_KEYTAB}|g" \
  -e "s|\${KAFKA_PRINCIPAL}|${KAFKA_PRINCIPAL}|g" \
  -e "s|\${TRUSTSTORE_JKS}|${TRUSTSTORE_JKS}|g" \
  -e "s|\${TRUSTSTORE_PW}|${TRUSTSTORE_PW}|g" \
  "${ROOT_DIR}/conf/kafka_kerberos.properties" \
  | grep -v "^#" | grep -v "^$" | grep -v "sasl.jaas.config" > "${CLIENT_PROPS}"

# JAAS 설정 추가 (CLI용)
cat >> "${CLIENT_PROPS}" <<EOF
ssl.truststore.type=JKS
EOF

export KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONF}"
ok "JAAS + client.properties 생성 완료 (conf/ 기반)"

# ---------------------------------------------------------------------------
# Truststore 비밀번호 확인
# ---------------------------------------------------------------------------
if [ -z "${TRUSTSTORE_PW}" ]; then
  err "TRUSTSTORE_PW 가 설정되지 않았습니다. source config/env.conf 를 먼저 실행하세요."
fi
ok "Truststore: ${TRUSTSTORE_JKS}"

# ---------------------------------------------------------------------------
# 토픽 생성 함수 (--if-not-exists: 이미 존재해도 오류 없이 통과)
# ---------------------------------------------------------------------------
create_topic() {
  local topic="$1"
  local partitions="${2:-${KAFKA_PARTITIONS}}"
  local rf="${3:-${KAFKA_RF}}"
  local retention_ms="${4:-86400000}"

  "${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${CLIENT_PROPS}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${rf}" \
    --config "retention.ms=${retention_ms}" \
    --config cleanup.policy=delete \
    --config compression.type=snappy \
  && ok "토픽 생성 완료 (또는 이미 존재): ${topic}" \
  || err "토픽 생성 실패: ${topic}"
}

# ---------------------------------------------------------------------------
# 토픽 생성
# ---------------------------------------------------------------------------
info "토픽 생성 시작..."
create_topic "${KAFKA_TOPIC_TXN}"    "${KAFKA_PARTITIONS}" "${KAFKA_RF}" "86400000"   # 보존: 1일
create_topic "${KAFKA_TOPIC_ALERTS}" "2"                   "${KAFKA_RF}" "604800000"  # 보존: 7일

# ---------------------------------------------------------------------------
# 토픽 최종 확인
# ---------------------------------------------------------------------------
info "전체 토픽 목록 (sbi-aml-*):"
"${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${CLIENT_PROPS}" \
    --list 2>/dev/null | grep "^sbi-aml-" || warn "sbi-aml- 접두사 토픽을 찾을 수 없습니다."

echo ""
ok "Kafka 토픽 설정 완료!"
echo "  다음 단계: bash infra/02_run_kudu_ddl.sh"
