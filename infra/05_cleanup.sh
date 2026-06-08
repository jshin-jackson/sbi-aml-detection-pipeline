#!/usr/bin/env bash
# ================================================================
# 05_cleanup.sh — 전체 인프라 및 데이터 완전 초기화
#
# ⚠️  주의: 이 스크립트는 아래 리소스를 모두 삭제합니다.
#           실행 전 반드시 내용을 확인하세요.
#
#   - Kafka 토픽   : sbi-aml-transactions, sbi-aml-alerts
#   - Kudu 테이블  : aml_transactions, aml_alerts, aml_risk_score
#   - Flink Job   : 실행 중인 AML Job 종료
#   - SSB Job     : 실행 중인 AML Job 종료 (SSB가 있는 경우)
#   - 로컬 데이터  : /tmp/aml-data/, /tmp/aml-ssb/
#
# 사용법:
#   source config/env.conf
#   bash infra/05_cleanup.sh
#
# 재시작 시:
#   bash infra/01_kafka_setup.sh
#   bash infra/02_run_kudu_ddl.sh
#   python data_gen/generate_aml_data.py
# ================================================================
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# config 로드
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf 파일이 없습니다."
  echo "        ln -sf config/env.internal.conf config/env.conf"
  exit 1
fi
source "${ROOT_DIR}/config/env.conf"

PASS=0
FAIL=0

ok()      { echo "  [OK]   $1"; ((PASS+=1)); }
fail()    { echo "  [FAIL] $1"; ((FAIL+=1)); }
section() { echo ""; echo "=== $1 ==="; }
skip()    { echo "  [SKIP] $1"; }

echo ""
echo "================================================================"
echo " SBI AML Detection — 완전 초기화 (ENV: ${ENV_NAME})"
echo "================================================================"
echo ""
echo "  삭제 대상:"
echo "    - Kafka 토픽   : ${KAFKA_TOPIC_TXN}, ${KAFKA_TOPIC_ALERTS}"
echo "    - Kudu 테이블  : default.aml_transactions"
echo "                     default.aml_alerts"
echo "                     default.aml_risk_score"
echo "    - Flink Job   : 실행 중인 AML Job 종료"
echo "    - SSB Job     : 실행 중인 AML Job 종료 (SSB_HOST가 설정된 경우)"
echo "    - 로컬 데이터  : ${DATA_OUTPUT_DIR}, /tmp/aml-ssb"
echo ""
read -r -p "계속하시겠습니까? (yes/no): " CONFIRM
if [ "${CONFIRM}" != "yes" ]; then
  echo "취소되었습니다."
  exit 0
fi

# ------------------------------------------------------------------
section "1. Kerberos 인증"
# ------------------------------------------------------------------
if kinit -kt "${KEYTAB}" "${PRINCIPAL}" 2>/dev/null; then
  ok "kinit 성공 (${PRINCIPAL})"
else
  fail "kinit 실패 — keytab 확인 필요: ${KEYTAB}"
  exit 1
fi

# ------------------------------------------------------------------
section "2. Kafka 토픽 삭제"
# ------------------------------------------------------------------
KAFKA_TOPICS_CMD="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}/bin/kafka-topics.sh"

TMPDIR_CLEAN=$(mktemp -d)
trap 'rm -rf "${TMPDIR_CLEAN}"' EXIT

cat > "${TMPDIR_CLEAN}/jaas.conf" <<EOF
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="${KEYTAB}"
    principal="${PRINCIPAL}";
};
EOF

cat > "${TMPDIR_CLEAN}/client.properties" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=GSSAPI
sasl.kerberos.service.name=kafka
ssl.truststore.location=${TRUSTSTORE_JKS}
ssl.truststore.password=${TRUSTSTORE_PW}
ssl.truststore.type=JKS
EOF

export KAFKA_OPTS="-Djava.security.auth.login.config=${TMPDIR_CLEAN}/jaas.conf"

for TOPIC in "${KAFKA_TOPIC_TXN}" "${KAFKA_TOPIC_ALERTS}"; do
  if "${KAFKA_TOPICS_CMD}" \
      --bootstrap-server "${KAFKA_BROKERS}" \
      --command-config "${TMPDIR_CLEAN}/client.properties" \
      --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    if "${KAFKA_TOPICS_CMD}" \
        --bootstrap-server "${KAFKA_BROKERS}" \
        --command-config "${TMPDIR_CLEAN}/client.properties" \
        --delete --topic "${TOPIC}" 2>/dev/null; then
      ok "토픽 삭제 완료: ${TOPIC}"
    else
      fail "토픽 삭제 실패: ${TOPIC}"
    fi
  else
    skip "토픽 없음 (이미 삭제됨): ${TOPIC}"
  fi
done

# ------------------------------------------------------------------
section "3. Kudu 테이블 삭제 (Impala Shell)"
# ------------------------------------------------------------------
IMPALA_SQL="
DROP TABLE IF EXISTS default.aml_alerts;
DROP TABLE IF EXISTS default.aml_risk_score;
DROP TABLE IF EXISTS default.aml_transactions;
"

if impala-shell -k --ssl \
    --ca_cert="${CA_PEM}" \
    -i "${IMPALA_HOST}:${IMPALA_PORT}" \
    -q "${IMPALA_SQL}" \
    --quiet 2>/dev/null; then
  ok "Kudu 테이블 삭제 완료 (aml_transactions, aml_alerts, aml_risk_score)"
else
  fail "Kudu 테이블 삭제 실패 — Impala 연결 확인 필요"
fi

# ------------------------------------------------------------------
section "4. Flink Job 종료 (Standalone Flink가 있는 경우)"
# ------------------------------------------------------------------
FLINK_REST="${FLINK_REST_URL:-http://localhost:8081}"

if curl -s "${FLINK_REST}/jobs" &>/dev/null; then
  # 실행 중인 Job 목록 조회
  RUNNING_JOBS=$(curl -s "${FLINK_REST}/jobs" 2>/dev/null \
    | grep -o '"id":"[^"]*"' | grep -o '[a-f0-9]*' || true)

  if [ -z "${RUNNING_JOBS}" ]; then
    skip "실행 중인 Flink Job 없음"
  else
    for JOB_ID in ${RUNNING_JOBS}; do
      if curl -s -X PATCH "${FLINK_REST}/jobs/${JOB_ID}?mode=cancel" &>/dev/null; then
        ok "Flink Job 종료 완료: ${JOB_ID}"
      else
        fail "Flink Job 종료 실패: ${JOB_ID}"
      fi
    done
  fi
else
  skip "Flink REST API 미응답 — Flink가 실행 중이 아니거나 설치되지 않음 (${FLINK_REST})"
fi

# ------------------------------------------------------------------
section "5. SSB Job 종료 (CSA/SSB가 있는 경우)"
# ------------------------------------------------------------------
if [ -n "${SSB_HOST:-}" ] && [ -n "${SSB_USER:-}" ] && [ -n "${SSB_PASSWORD:-}" ]; then
  SSB_JOBS=$(curl -s --cacert "${CA_PEM}" \
    -u "${SSB_USER}:${SSB_PASSWORD}" \
    "${SSB_HOST}/api/v1/jobs" 2>/dev/null || echo "")

  if echo "${SSB_JOBS}" | grep -q '"id"'; then
    # aml_ 접두어를 가진 Job만 종료
    AML_JOB_IDS=$(echo "${SSB_JOBS}" \
      | grep -o '"id":[0-9]*' | grep -o '[0-9]*' || true)

    if [ -z "${AML_JOB_IDS}" ]; then
      skip "실행 중인 SSB AML Job 없음"
    else
      for JOB_ID in ${AML_JOB_IDS}; do
        if curl -s -X DELETE --cacert "${CA_PEM}" \
            -u "${SSB_USER}:${SSB_PASSWORD}" \
            "${SSB_HOST}/api/v1/jobs/${JOB_ID}" &>/dev/null; then
          ok "SSB Job 종료 완료: ID=${JOB_ID}"
        else
          fail "SSB Job 종료 실패: ID=${JOB_ID}"
        fi
      done
    fi
  else
    skip "SSB 연결 실패 또는 실행 중인 Job 없음"
  fi
else
  skip "SSB_HOST 미설정 — SSB Job 종료 건너뜀"
fi

# ------------------------------------------------------------------
section "6. 로컬 임시 데이터 삭제"
# ------------------------------------------------------------------
if [ -d "${DATA_OUTPUT_DIR}" ]; then
  rm -rf "${DATA_OUTPUT_DIR}"
  ok "로컬 데이터 삭제: ${DATA_OUTPUT_DIR}"
else
  skip "로컬 데이터 없음: ${DATA_OUTPUT_DIR}"
fi

if [ -d "/tmp/aml-ssb" ]; then
  rm -rf "/tmp/aml-ssb"
  ok "SSB 렌더링 파일 삭제: /tmp/aml-ssb"
else
  skip "SSB 렌더링 파일 없음: /tmp/aml-ssb"
fi

# 임시 Kafka 설정 파일
rm -f /tmp/kafka-client.properties /tmp/kafka-jaas.conf 2>/dev/null && true

# ------------------------------------------------------------------
echo ""
echo "================================================================"
echo " 결과: ${PASS}개 성공 / ${FAIL}개 실패"
echo "================================================================"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "[주의] FAIL 항목을 확인하세요. 수동으로 삭제가 필요할 수 있습니다."
  exit 1
else
  echo ""
  echo "[완료] 초기화 완료! 아래 순서로 재시작하세요:"
  echo ""
  echo "  source config/env.conf"
  echo "  bash infra/01_kafka_setup.sh"
  echo "  bash infra/02_run_kudu_ddl.sh"
  echo "  python data_gen/generate_aml_data.py"
  echo "  python data_gen/kafka_producer.py"
  exit 0
fi
