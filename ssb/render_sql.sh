#!/usr/bin/env bash
# ================================================================
# render_sql.sh — SQL 템플릿 렌더링 (SSB Web UI Primary 방식)
#
# 사용법:
#   bash ssb/render_sql.sh
#   → /tmp/aml-ssb/ 폴더에 렌더링된 SQL 파일 생성
#   → 내용을 복사하여 SSB Web UI에 붙여넣기
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

source "${ROOT_DIR}/config/env.conf"
TRUSTSTORE_PW=$(cat "${TRUSTSTORE_PW_FILE}")
export TRUSTSTORE_PW  # envsubst에서 사용하기 위해 export

OUTPUT_DIR="/tmp/aml-ssb"
mkdir -p "${OUTPUT_DIR}"

echo ""
echo "================================================================"
echo " SSB SQL 렌더링 (ENV: ${ENV_NAME})"
echo " 출력 디렉토리: ${OUTPUT_DIR}"
echo "================================================================"
echo ""

# 각 SQL 템플릿 렌더링
for tpl in "${SCRIPT_DIR}"/*.sql.tpl; do
  filename=$(basename "${tpl}" .tpl)
  output="${OUTPUT_DIR}/${filename}"
  envsubst < "${tpl}" > "${output}"
  echo "  [OK] ${filename}"
done

echo ""
echo "================================================================"
echo " SSB Web UI 실행 순서:"
echo "================================================================"
echo ""
echo " 브라우저에서 ${SSB_HOST} 접속 후:"
echo ""
echo " [Step 1] 01_kafka_source_table.sql — Kafka Source Table 생성"
cat "${OUTPUT_DIR}/01_kafka_source_table.sql"
echo ""
echo "------------------------------------------------------------"
echo " [Step 2] 02_large_cash_job.sql — Large Cash 탐지 Job"
cat "${OUTPUT_DIR}/02_large_cash_job.sql"
echo ""
echo "------------------------------------------------------------"
echo " [Step 3] 03_smurfing_job.sql — Smurfing 탐지 Job"
cat "${OUTPUT_DIR}/03_smurfing_job.sql"
echo ""
echo "================================================================"
echo " 파일 위치:"
ls -la "${OUTPUT_DIR}/"
echo ""
echo " SSB Web UI에서 각 파일 내용을 순서대로 붙여넣고 Execute 버튼을 누르세요."
echo "================================================================"
