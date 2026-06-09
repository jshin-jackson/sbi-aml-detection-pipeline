#!/usr/bin/env bash
# ================================================================
# 02_run_impala.sh — Impala 쿼리 실행 래퍼
# Phase 5 Demo 검증 및 실시간 결과 확인에 사용합니다.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

source "${ROOT_DIR}/config/env.conf"
kinit -kt "${KEYTAB}" "${PRINCIPAL}"

SQL_FILE="${1:-${ROOT_DIR}/impala/demo_queries.sql}"

echo ""
echo "================================================================"
echo " Impala 쿼리 실행 (${ENV_NAME})"
echo " Host : ${IMPALA_HOST}:${IMPALA_PORT}"
echo " File : ${SQL_FILE}"
echo "================================================================"
echo ""

impala-shell \
  -k \
  --ssl \
  --ca_cert="${CA_PEM}" \
  -i "${IMPALA_HOST}:${IMPALA_PORT}" \
  -f "${SQL_FILE}" \
  --output_delimiter='|' \
  --print_header
