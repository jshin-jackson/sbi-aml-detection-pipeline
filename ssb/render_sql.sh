#!/usr/bin/env bash
# ================================================================
# render_sql.sh — Render SQL templates (SSB Web UI primary method)
#
# Usage:
#   bash ssb/render_sql.sh
#   → Rendered SQL files created in /tmp/aml-ssb/
#   → Copy contents and paste into SSB Web UI
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

source "${ROOT_DIR}/config/env.conf"

OUTPUT_DIR="/tmp/aml-ssb"
mkdir -p "${OUTPUT_DIR}"

echo ""
echo "================================================================"
echo " SSB SQL Rendering (ENV: ${ENV_NAME})"
echo " Output directory: ${OUTPUT_DIR}"
echo "================================================================"
echo ""

# Render each SQL template — substitute env vars using sed (no gettext/envsubst required)
for tpl in "${SCRIPT_DIR}"/*.sql.tpl; do
  filename=$(basename "${tpl}" .tpl)
  output="${OUTPUT_DIR}/${filename}"
  sed \
    -e "s|\${KAFKA_BROKERS}|${KAFKA_BROKERS}|g" \
    -e "s|\${KAFKA_TOPIC_TXN}|${KAFKA_TOPIC_TXN}|g" \
    -e "s|\${INCLUSTER_TRUSTSTORE_JKS}|${INCLUSTER_TRUSTSTORE_JKS}|g" \
    -e "s|\${TRUSTSTORE_PW}|${TRUSTSTORE_PW}|g" \
    -e "s|\${LARGE_CASH_THRESHOLD}|${LARGE_CASH_THRESHOLD}|g" \
    -e "s|\${SMURFING_WINDOW_MIN}|${SMURFING_WINDOW_MIN}|g" \
    -e "s|\${SMURFING_TXN_COUNT}|${SMURFING_TXN_COUNT}|g" \
    "${tpl}" > "${output}"
  echo "  [OK] ${filename}"
done

echo ""
echo "================================================================"
echo " SSB Web UI Execution Order:"
echo "================================================================"
echo ""
echo " Open ${SSB_HOST} in your browser:"
echo ""
echo " [Step 1] 01_kafka_source_table.sql — Create Kafka Source Table"
cat "${OUTPUT_DIR}/01_kafka_source_table.sql"
echo ""
echo "------------------------------------------------------------"
echo " [Step 2] 02_large_cash_job.sql — Large Cash Detection Job"
cat "${OUTPUT_DIR}/02_large_cash_job.sql"
echo ""
echo "------------------------------------------------------------"
echo " [Step 3] 03_smurfing_job.sql — Smurfing Detection Job"
cat "${OUTPUT_DIR}/03_smurfing_job.sql"
echo ""
echo "================================================================"
echo " File locations:"
ls -la "${OUTPUT_DIR}/"
echo ""
echo " Paste each file's content into SSB Web UI in order and click Execute."
echo "================================================================"
