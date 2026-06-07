#!/usr/bin/env bash
# ================================================================
# verify_env.sh — 전체 환경 자동 검증 스크립트
# Phase 1에서 실행합니다. 모든 항목이 OK여야 다음 Phase로 진행합니다.
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

ok()   { echo "  [OK]  $1"; ((PASS+=1)); }
fail() { echo "  [FAIL] $1"; ((FAIL+=1)); }
section() { echo ""; echo "=== $1 ==="; }

echo ""
echo "================================================================"
echo " SBI AML Pipeline — 환경 검증 (ENV: ${ENV_NAME})"
echo "================================================================"

# ------------------------------------------------------------------
section "1. 설정 파일 확인"
# ------------------------------------------------------------------
[ -n "${KAFKA_BROKERS}" ]   && ok "KAFKA_BROKERS 설정됨"   || fail "KAFKA_BROKERS 미설정"
[ -n "${KUDU_MASTERS}" ]    && ok "KUDU_MASTERS 설정됨"    || fail "KUDU_MASTERS 미설정"
[ -n "${SSB_HOST}" ]        && ok "SSB_HOST 설정됨"        || fail "SSB_HOST 미설정"
[ -n "${IMPALA_HOST}" ]     && ok "IMPALA_HOST 설정됨"     || fail "IMPALA_HOST 미설정"
[ -n "${PRINCIPAL}" ]       && ok "PRINCIPAL: ${PRINCIPAL}" || fail "PRINCIPAL 미설정"

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
# 임시 Kafka client 설정 파일 생성
KAFKA_CLIENT_CONF=$(mktemp)
cat > "${KAFKA_CLIENT_CONF}" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
ssl.truststore.location=${TRUSTSTORE_JKS}
ssl.truststore.password=${TRUSTSTORE_PW}
EOF

if kafka-topics --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --list &>/dev/null; then
  ok "Kafka 연결 성공 (${KAFKA_BROKERS})"
else
  fail "Kafka 연결 실패 — 브로커 주소 또는 Kerberos 설정 확인 필요"
fi
rm -f "${KAFKA_CLIENT_CONF}"

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
