#!/usr/bin/env bash
# ================================================================
# 01_kafka_setup.sh — Create Kafka Topics
# Run once in Phase 2.
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/env.conf"

KAFKA_TOPICS_CMD="${KAFKA_HOME:-/opt/cloudera/parcels/CDH/lib/kafka}/bin/kafka-topics.sh"

echo ""
echo "================================================================"
echo " Kafka Topic Creation (ENV: ${ENV_NAME})"
echo " Broker: ${KAFKA_BROKERS}"
echo "================================================================"

# Kerberos authentication
kinit -kt "${KEYTAB}" "${PRINCIPAL}"
echo "[Kerberos] kinit succeeded: ${PRINCIPAL}"

# Create temporary JAAS + client config files
TMPDIR_KAFKA=$(mktemp -d)
trap 'rm -rf "${TMPDIR_KAFKA}"' EXIT

JAAS_CONF="${TMPDIR_KAFKA}/kafka-jaas.conf"
KAFKA_CLIENT_CONF="${TMPDIR_KAFKA}/kafka-client.properties"

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
connections.max.idle.ms=540000
EOF

export KAFKA_OPTS="-Djava.security.auth.login.config=${JAAS_CONF}"
echo "[Kafka] JAAS configuration ready"

# Topic creation function
create_topic() {
  local topic="$1"
  local partitions="$2"
  local rf="$3"
  local retention_ms="${4:-86400000}"

  echo ""
  echo "Creating topic: ${topic}"
  "${KAFKA_TOPICS_CMD}" \
    --bootstrap-server "${KAFKA_BROKERS}" \
    --command-config "${KAFKA_CLIENT_CONF}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${rf}" \
    --config retention.ms="${retention_ms}" \
    --config cleanup.policy=delete \
    --config compression.type=snappy

  echo "  [OK] ${topic} (partitions=${partitions}, rf=${rf})"
}

# Transaction topic (main stream) — 1 day retention
create_topic "${KAFKA_TOPIC_TXN}"    "${KAFKA_PARTITIONS}" "${KAFKA_RF}" "86400000"

# Alert topic (SSB output — optional) — 7 day retention
create_topic "${KAFKA_TOPIC_ALERTS}" "2"                   "${KAFKA_RF}" "604800000"

# List created topics
echo ""
echo "=== Created Topics ==="
"${KAFKA_TOPICS_CMD}" \
  --bootstrap-server "${KAFKA_BROKERS}" \
  --command-config "${KAFKA_CLIENT_CONF}" \
  --list 2>/dev/null | grep "sbi-aml" || echo "  (none found)"

echo ""
echo "[DONE] Kafka topics created successfully!"
echo "  Next step: bash infra/02_run_kudu_ddl.sh"
