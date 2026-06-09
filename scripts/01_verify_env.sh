#!/usr/bin/env bash
# ================================================================
# 01_verify_env.sh — 전체 환경 자동 검증 스크립트
# Phase 1에서 실행합니다. 모든 항목이 OK여야 다음 Phase로 진행합니다.
#
# 사용법:
#   source config/env.conf
#   bash scripts/01_verify_env.sh
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# config 로드
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf 파일이 없습니다."
  echo "        다음 명령으로 설정 파일을 연결하세요:"
  echo "        ln -sf config/env.internal.conf config/env.conf"
  exit 1
fi
source "${ROOT_DIR}/config/env.conf"

PASS=0
FAIL=0

ok()      { echo "  [OK]   $1"; ((PASS+=1)); }
fail()    { echo "  [FAIL] $1"; ((FAIL+=1)); }
section() { echo ""; echo "=== $1 ==="; }

echo ""
echo "================================================================"
echo " SBI AML Pipeline — 환경 검증 (ENV: ${ENV_NAME})"
echo "================================================================"

# ------------------------------------------------------------------
section "1. 설정 파일 확인"
# ------------------------------------------------------------------
[ -n "${KAFKA_BROKERS}" ]   && ok "KAFKA_BROKERS 설정됨"        || fail "KAFKA_BROKERS 미설정"
[ -n "${KUDU_MASTERS}" ]    && ok "KUDU_MASTERS: ${KUDU_MASTERS}" || fail "KUDU_MASTERS 미설정"
[ -n "${SSB_HOST}" ]        && ok "SSB_HOST 설정됨"              || fail "SSB_HOST 미설정"
[ -n "${IMPALA_HOST}" ]     && ok "IMPALA_HOST: ${IMPALA_HOST}"  || fail "IMPALA_HOST 미설정"
[ -n "${PRINCIPAL}" ]       && ok "PRINCIPAL: ${PRINCIPAL}"      || fail "PRINCIPAL 미설정"
[ -n "${KAFKA_KEYTAB}" ]    && ok "KAFKA_KEYTAB: ${KAFKA_KEYTAB}" || fail "KAFKA_KEYTAB 미설정"

# conf/ 파일 존재 여부
for conf_file in \
    "${ROOT_DIR}/conf/kafka_jaas.conf" \
    "${ROOT_DIR}/conf/kafka_kerberos.properties"; do
  [ -f "${conf_file}" ] \
    && ok "conf 파일 존재: $(basename "${conf_file}")" \
    || fail "conf 파일 없음: ${conf_file}"
done

# ------------------------------------------------------------------
section "2. Kerberos 인증"
# ------------------------------------------------------------------
if [ ! -f "${KEYTAB}" ]; then
  fail "Keytab 파일 없음: ${KEYTAB}"
else
  ok "Keytab 파일 존재: ${KEYTAB}"
  if kinit -kt "${KEYTAB}" "${PRINCIPAL}" 2>/dev/null; then
    ok "kinit 성공 (${PRINCIPAL})"
    klist 2>/dev/null | grep -q "Ticket cache" && ok "TGT 발급 확인" || fail "TGT 확인 실패"
  else
    fail "kinit 실패 — keytab 또는 principal 확인 필요"
  fi
fi

# ------------------------------------------------------------------
section "3. Auto-TLS 인증서 파일 확인"
# ------------------------------------------------------------------
for cert_var in TRUSTSTORE_JKS INCLUSTER_TRUSTSTORE_JKS CA_PEM; do
  cert_path="${!cert_var}"
  if [ -f "${cert_path}" ]; then
    ok "${cert_var}: ${cert_path}"
  else
    fail "${cert_var} 파일 없음: ${cert_path}"
  fi
done

if [ -z "${TRUSTSTORE_PW}" ]; then
  fail "TRUSTSTORE_PW 미설정 — config/env.conf에 TRUSTSTORE_PW 값을 입력하세요"
else
  ok "TRUSTSTORE_PW 설정 확인"
fi

# ------------------------------------------------------------------
section "4. Kafka 연결 테스트 (SASL_SSL + GSSAPI)"
# ------------------------------------------------------------------
KAFKA_TOPICS_CMD="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}/bin/kafka-topics.sh"

TMPDIR_VERIFY=$(mktemp -d)
trap 'rm -rf "${TMPDIR_VERIFY}"' EXIT

JAAS_CONF="${TMPDIR_VERIFY}/kafka-jaas.conf"
KAFKA_CLIENT_CONF="${TMPDIR_VERIFY}/kafka-client.properties"

# conf/kafka_jaas.conf 기반으로 렌더링
sed \
  -e "s|\${KAFKA_KEYTAB}|${KAFKA_KEYTAB}|g" \
  -e "s|\${KAFKA_PRINCIPAL}|${KAFKA_PRINCIPAL}|g" \
  "${ROOT_DIR}/conf/kafka_jaas.conf" > "${JAAS_CONF}"

# client.properties 생성
cat > "${KAFKA_CLIENT_CONF}" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
ssl.truststore.location=${TRUSTSTORE_JKS}
ssl.truststore.password=${TRUSTSTORE_PW}
ssl.truststore.type=JKS
request.timeout.ms=30000
EOF

export KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONF}"

if "${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --list &>/dev/null; then
  ok "Kafka 연결 성공 (${KAFKA_BROKERS})"

  # 토픽 존재 여부 확인
  "${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --list 2>/dev/null | grep -q "^${KAFKA_TOPIC_TXN}$" \
    && ok "토픽 존재: ${KAFKA_TOPIC_TXN}" \
    || fail "토픽 없음: ${KAFKA_TOPIC_TXN} (infra/01_kafka_setup.sh 실행 필요)"

  "${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --list 2>/dev/null | grep -q "^${KAFKA_TOPIC_ALERTS}$" \
    && ok "토픽 존재: ${KAFKA_TOPIC_ALERTS}" \
    || fail "토픽 없음: ${KAFKA_TOPIC_ALERTS} (infra/01_kafka_setup.sh 실행 필요)"
else
  fail "Kafka 연결 실패 — 브로커 주소 또는 TRUSTSTORE_PW 확인 필요"
fi

# ------------------------------------------------------------------
section "5. Impala 연결 테스트 (Kerberos + SSL)"
# ------------------------------------------------------------------
if impala-shell -k --ssl \
    --ca_cert="${CA_PEM}" \
    -i "${IMPALA_HOST}:${IMPALA_PORT}" \
    -q "SELECT 'Impala OK' AS status" \
    --quiet 2>/dev/null | grep -q "Impala OK"; then
  ok "Impala 연결 성공 (${IMPALA_HOST})"
else
  fail "Impala 연결 실패 — 호스트 또는 Kerberos 설정 확인 필요"
fi

# ------------------------------------------------------------------
section "6. Kudu 접근 테스트"
# ------------------------------------------------------------------
if kudu table list "${KUDU_MASTERS}" &>/dev/null; then
  ok "Kudu Masters 접근 성공 (${KUDU_MASTERS})"

  # AML 테이블 존재 여부 확인
  for table in "default.aml_transactions" "default.aml_alerts" "default.aml_risk_score"; do
    kudu table list "${KUDU_MASTERS}" 2>/dev/null | grep -q "^${table}$" \
      && ok "Kudu 테이블 존재: ${table}" \
      || fail "Kudu 테이블 없음: ${table} (infra/02_run_kudu_ddl.sh 실행 필요)"
  done
else
  fail "Kudu Masters 접근 실패 — 호스트 또는 Kerberos 설정 확인 필요"
fi

# ------------------------------------------------------------------
section "7. SSB REST API 테스트 (HTTPS)"
# ------------------------------------------------------------------
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  --cacert "${CA_PEM}" \
  -u "${SSB_USER}:${SSB_PASSWORD}" \
  "${SSB_HOST}/api/v1/sessions" 2>/dev/null || echo "000")

if [ "${HTTP_CODE}" = "200" ]; then
  ok "SSB REST API 연결 성공 (${SSB_HOST})"
elif [ "${HTTP_CODE}" = "401" ]; then
  fail "SSB 인증 실패 (HTTP 401) — SSB_USER/SSB_PASSWORD 확인 필요"
else
  fail "SSB 연결 실패 (HTTP ${HTTP_CODE}) — SSB_HOST 또는 CA_PEM 확인 필요"
fi

# ------------------------------------------------------------------
echo ""
echo "================================================================"
echo " 결과: ${PASS}개 성공 / ${FAIL}개 실패"
echo "================================================================"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "[주의] FAIL 항목을 먼저 해결한 후 다음 Phase를 진행하세요."
  echo "       문제 해결: README.md > 문제 해결 가이드 참고"
  exit 1
else
  echo ""
  echo "[완료] 모든 환경 검증 통과! Phase 2를 시작하세요."
  exit 0
fi
