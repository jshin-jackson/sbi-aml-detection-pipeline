#!/usr/bin/env bash
# ================================================================
# 02_run_kudu_ddl.sh — Run Kudu Table Creation
# Run once in Phase 2.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/env.conf"

echo ""
echo "================================================================"
echo " Kudu Table Creation (ENV: ${ENV_NAME})"
echo " Kudu Masters: ${KUDU_MASTERS}"
echo " Impala      : ${IMPALA_HOST}:${IMPALA_PORT}"
echo "================================================================"

# Kerberos authentication
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
echo "[Kerberos] kinit succeeded: ${PRINCIPAL}"

# Substitute ${KUDU_MASTERS} in SQL file with actual value (uses sed — no gettext required)
RENDERED_SQL=$(mktemp /tmp/kudu-ddl-XXXXXX.sql)
sed "s|\${KUDU_MASTERS}|${KUDU_MASTERS}|g" "${SCRIPT_DIR}/02_kudu_ddl.sql" > "${RENDERED_SQL}"

cleanup() {
  rm -f "${RENDERED_SQL}"
}
trap cleanup EXIT

echo "[SQL] Variable substitution complete → ${RENDERED_SQL}"
echo ""

# Execute DDL via Impala shell
impala-shell \
  -k \
  --ssl \
  --ca_cert="${CA_PEM}" \
  -i "${IMPALA_HOST}:${IMPALA_PORT}" \
  -f "${RENDERED_SQL}"

echo ""
echo "[DONE] Kudu tables created successfully!"
echo "  Tables: aml_transactions / aml_alerts / aml_risk_score"
echo "  Next step: Apply Ranger policies, then run: python data_gen/generate_aml_data.py"
