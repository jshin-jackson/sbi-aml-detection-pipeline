#!/usr/bin/env bash
# ================================================================
# run_impala.sh — Impala query runner wrapper
# Used for Phase 5 demo verification and real-time result checks.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

source "${ROOT_DIR}/config/env.conf"
kinit -kt "${KEYTAB}" "${PRINCIPAL}"

SQL_FILE="${1:-${ROOT_DIR}/impala/demo_queries.sql}"

echo ""
echo "================================================================"
echo " Impala Query Execution (${ENV_NAME})"
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
