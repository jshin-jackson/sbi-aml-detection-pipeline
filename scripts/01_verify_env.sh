#!/usr/bin/env bash
# ================================================================
# verify_env.sh — Automated environment verification script
# Run this in Phase 1. All items must pass before proceeding.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/.."

# Load config
if [ ! -f "${ROOT_DIR}/config/env.conf" ]; then
  echo "[ERROR] config/env.conf not found."
  echo "        Create the symlink with:"
  echo "        ln -sf config/env.internal.conf config/env.conf"
  exit 1
fi
source "${ROOT_DIR}/config/env.conf"

PASS=0
FAIL=0

ok()      { echo "  [OK]  $1"; ((PASS+=1)); }
fail()    { echo "  [FAIL] $1"; ((FAIL+=1)); }
section() { echo ""; echo "=== $1 ==="; }

echo ""
echo "================================================================"
echo " SBI AML Pipeline — Environment Verification (ENV: ${ENV_NAME})"
echo "================================================================"

# ------------------------------------------------------------------
section "1. Configuration Check"
# ------------------------------------------------------------------
[ -n "${KAFKA_BROKERS}" ]   && ok "KAFKA_BROKERS is set"   || fail "KAFKA_BROKERS not set"
[ -n "${KUDU_MASTERS}" ]    && ok "KUDU_MASTERS is set"    || fail "KUDU_MASTERS not set"
[ -n "${SSB_HOST}" ]        && ok "SSB_HOST is set"        || fail "SSB_HOST not set"
[ -n "${IMPALA_HOST}" ]     && ok "IMPALA_HOST is set"     || fail "IMPALA_HOST not set"
[ -n "${PRINCIPAL}" ]       && ok "PRINCIPAL: ${PRINCIPAL}" || fail "PRINCIPAL not set"

# ------------------------------------------------------------------
section "2. Kerberos Authentication"
# ------------------------------------------------------------------
if [ ! -f "${KEYTAB}" ]; then
  fail "Keytab file not found: ${KEYTAB}"
else
  ok "Keytab file exists: ${KEYTAB}"
  if kinit -kt "${KEYTAB}" "${PRINCIPAL}" 2>/dev/null; then
    ok "kinit succeeded (${PRINCIPAL})"
    klist 2>/dev/null | grep -q "Ticket cache" && ok "TGT verified" || fail "TGT verification failed"
  else
    fail "kinit failed — check keytab or principal"
  fi
fi

# ------------------------------------------------------------------
section "3. Auto-TLS Certificate Files"
# ------------------------------------------------------------------
for cert_var in TRUSTSTORE_JKS INCLUSTER_TRUSTSTORE_JKS CA_PEM; do
  cert_path="${!cert_var}"
  if [ -f "${cert_path}" ]; then
    ok "${cert_var}: ${cert_path}"
  else
    fail "${cert_var} not found: ${cert_path}"
  fi
done

if [ -z "${TRUSTSTORE_PW}" ]; then
  fail "TRUSTSTORE_PW not set — enter value in config/env.conf"
else
  ok "TRUSTSTORE_PW is set"
fi

# ------------------------------------------------------------------
section "4. Kafka Connection Test (SASL_SSL + GSSAPI)"
# ------------------------------------------------------------------
KAFKA_TOPICS_CMD="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}/bin/kafka-topics.sh"

# Create temporary JAAS and client config files
TMPDIR_VERIFY=$(mktemp -d)
trap 'rm -rf "${TMPDIR_VERIFY}"' EXIT

JAAS_CONF="${TMPDIR_VERIFY}/kafka-jaas.conf"
KAFKA_CLIENT_CONF="${TMPDIR_VERIFY}/kafka-client.properties"

cat > "${JAAS_CONF}" <<EOF
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="${KEYTAB}"
    principal="${PRINCIPAL}";
};
EOF

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
  ok "Kafka connection succeeded (${KAFKA_BROKERS})"
else
  fail "Kafka connection failed — check broker address or TRUSTSTORE_PW"
fi

# ------------------------------------------------------------------
section "5. Impala Connection Test (Kerberos + SSL)"
# ------------------------------------------------------------------
if impala-shell -k --ssl \
    --ca_cert="${CA_PEM}" \
    -i "${IMPALA_HOST}:${IMPALA_PORT}" \
    -q "SELECT 'Impala OK' AS status" \
    --quiet 2>/dev/null | grep -q "Impala OK"; then
  ok "Impala connection succeeded (${IMPALA_HOST})"
else
  fail "Impala connection failed — check hostname or Kerberos settings"
fi

# ------------------------------------------------------------------
section "6. Kudu Access Test"
# ------------------------------------------------------------------
if kudu table list "${KUDU_MASTERS}" &>/dev/null; then
  ok "Kudu Masters accessible (${KUDU_MASTERS})"
else
  fail "Kudu Masters not accessible — check hostname or Kerberos settings"
fi

# ------------------------------------------------------------------
section "7. SSB REST API Test (HTTPS)"
# ------------------------------------------------------------------
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  --cacert "${CA_PEM}" \
  -u "${SSB_USER}:${SSB_PASSWORD}" \
  "${SSB_HOST}/api/v1/sessions" 2>/dev/null || echo "000")

if [ "${HTTP_CODE}" = "200" ]; then
  ok "SSB REST API connection succeeded (${SSB_HOST})"
elif [ "${HTTP_CODE}" = "401" ]; then
  fail "SSB authentication failed (HTTP 401) — check SSB_USER/SSB_PASSWORD"
else
  fail "SSB connection failed (HTTP ${HTTP_CODE}) — check SSB_HOST or CA_PEM"
fi

# ------------------------------------------------------------------
echo ""
echo "================================================================"
echo " Result: ${PASS} passed / ${FAIL} failed"
echo "================================================================"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "[WARNING] Resolve all FAIL items before proceeding to the next phase."
  echo "          Refer to README.md > Troubleshooting"
  exit 1
else
  echo ""
  echo "[DONE] All environment checks passed! Proceed to Phase 2."
  exit 0
fi
