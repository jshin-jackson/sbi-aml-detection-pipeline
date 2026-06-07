#!/usr/bin/env bash
# ================================================================
# 02_run_kudu_ddl.sh — Kudu 테이블 생성 실행 스크립트
# Phase 2에서 한 번만 실행합니다.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/env.conf"

echo ""
echo "================================================================"
echo " Kudu 테이블 생성 (ENV: ${ENV_NAME})"
echo " Kudu Masters: ${KUDU_MASTERS}"
echo " Impala      : ${IMPALA_HOST}:${IMPALA_PORT}"
echo "================================================================"

# Kerberos 인증
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
echo "[Kerberos] kinit 완료: ${PRINCIPAL}"

# SQL 파일의 ${KUDU_MASTERS} 변수를 실제 값으로 치환
RENDERED_SQL=$(mktemp /tmp/kudu-ddl-XXXXXX.sql)
envsubst '${KUDU_MASTERS}' < "${SCRIPT_DIR}/02_kudu_ddl.sql" > "${RENDERED_SQL}"

cleanup() {
  rm -f "${RENDERED_SQL}"
}
trap cleanup EXIT

echo "[SQL] 변수 치환 완료 → ${RENDERED_SQL}"
echo ""

# Impala shell로 DDL 실행
impala-shell \
  -k \
  --ssl \
  --ca_cert="${CA_PEM}" \
  -i "${IMPALA_HOST}:${IMPALA_PORT}" \
  -f "${RENDERED_SQL}"

echo ""
echo "[완료] Kudu 테이블 생성 완료!"
echo "  생성된 테이블: aml_transactions / aml_alerts / aml_risk_score"
echo "  다음 단계: Ranger 정책 적용 후 python data_gen/generate_aml_data.py"
