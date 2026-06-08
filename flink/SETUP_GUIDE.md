# Apache Flink 1.20.1 Standalone Setup Guide

> Use this guide to install Apache Flink directly when CFM/CSA is not available.  
> AML detection jobs run using the **Flink SQL Client** only, without SSB (SQL Stream Builder).

---

## Verified Environment

```
OS         : RHEL 9.6
Java       : OpenJDK 21
Flink      : 1.20.1 (Standalone mode)
Kudu conn. : flink-connector-kudu-2.0-csa1.17.1.0.jar (CSA 1.17.1)
Kafka conn.: flink-sql-connector-kafka-3.4.0-1.20.jar
```

---

## Step 1: Install Java 21

```bash
sudo dnf install -y java-21-openjdk

# Verify
java -version   # 21.x.x

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-21
```

---

## Step 2: Install Flink 1.20.1

```bash
cd /opt
wget https://archive.apache.org/dist/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz
tar xzf flink-1.20.1-bin-scala_2.12.tgz
ln -s /opt/flink-1.20.1 /opt/flink
```

---

## Step 3: Configure Required JAR Files

### 3-1. Download Kafka SQL Connector (needs internet)

```bash
wget -P /opt/flink/lib/ \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
```

### 3-2. Kudu Connector — Copy from CDH Parcel

```bash
# Find Kudu connector (from CSA parcel)
# Example: /opt/cloudera/parcels/FLINK/ or another parcel location
find /opt/cloudera/ -name "flink-connector-kudu*.jar" 2>/dev/null
cp <found-path>/flink-connector-kudu-2.0-csa1.17.1.0.jar /opt/flink/lib/

# Kudu client dependencies — copy from CDH parcel
cp /opt/cloudera/parcels/CDH/jars/kudu-client-1.17.0.7.3.1.600-325.jar /opt/flink/lib/
cp /opt/cloudera/parcels/CDH/jars/kudu-proto-1.17.0.7.3.1.600-325.jar  /opt/flink/lib/

# async library (kudu-client dependency)
wget -P /opt/flink/lib/ \
  https://repo1.maven.org/maven2/com/stumbleupon/async/1.4.1/async-1.4.1.jar
```

### 3-3. Final /opt/flink/lib Contents

```
async-1.4.1.jar
flink-cep-1.20.1.jar
flink-connector-files-1.20.1.jar
flink-connector-kudu-2.0-csa1.17.1.0.jar    ← Kudu connector
flink-csv-1.20.1.jar
flink-dist-1.20.1.jar
flink-json-1.20.1.jar
flink-scala_2.12-1.20.1.jar
flink-sql-connector-kafka-3.4.0-1.20.jar     ← Kafka connector
flink-table-api-java-uber-1.20.1.jar
flink-table-planner-loader-1.20.1.jar
flink-table-runtime-1.20.1.jar
kudu-client-1.17.0.7.3.1.600-325.jar         ← Kudu client
kudu-proto-1.17.0.7.3.1.600-325.jar          ← Kudu Protobuf
log4j-*.jar (included by default)
```

> **Important:** Do NOT add `kudu-subprocess`, `kudu-backup`, `kudu-spark3`, or `kudu-hive` JARs.  
> They cause SLF4J conflicts and DNS resolver errors.

---

## Step 4: Configure flink-conf.yaml

Edit `/opt/flink/conf/flink-conf.yaml`:

```yaml
# REST / JobManager
rest.address: localhost
rest.port: 8081
jobmanager.rpc.address: localhost
jobmanager.rpc.port: 6123

# Memory (minimum for demo)
jobmanager.memory.process.size: 1600m
taskmanager.memory.process.size: 1728m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 2

# Kerberos
security.kerberos.login.use-ticket-cache: false
security.kerberos.login.keytab: /opt/cloudera/systest.keytab
security.kerberos.login.principal: systest@ROOT.COMOPS.SITE
security.kerberos.login.contexts: Client,KafkaClient

# JAAS (no quotes — avoids shell eval errors)
env.java.opts: -Djava.security.auth.login.config=/opt/flink/conf/flink-jaas.conf
```

### Create flink-jaas.conf

`/opt/flink/conf/flink-jaas.conf`:

```
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/opt/cloudera/systest.keytab"
    principal="systest@ROOT.COMOPS.SITE";
};
```

### Pin Java 21 in flink-env.sh

```bash
echo 'JAVA_HOME=/usr/lib/jvm/java-21' >> /opt/flink/conf/flink-env.sh
```

---

## Step 5: Start Flink

```bash
export JAVA_HOME=/usr/lib/jvm/java-21
/opt/flink/bin/start-cluster.sh

# Verify
curl http://localhost:8081/overview
```

---

## Step 6: Run AML Detection via SQL Client

```bash
/opt/flink/bin/sql-client.sh
```

Run the SQL files in `flink/sql/` in order inside the SQL Client.

> **Important:** Flink SQL Client is session-based — table definitions are lost when the session ends.  
> Always run DDLs in order at the start of each new session.

```sql
-- [Step 1] Paste contents of flink/sql/01_kafka_source.sql
-- [Step 2] Paste contents of flink/sql/02_kudu_aml_alerts.sql
-- [Step 3] Paste contents of flink/sql/03_large_cash_job.sql
-- [Step 4] Paste contents of flink/sql/04_smurfing_job.sql
```

Or run all at once:

```bash
/opt/flink/bin/sql-client.sh -f flink/sql/05_run_all.sql
```

---

## Step 7: Verify Results in Impala (Hue)

```sql
-- Run in Hue Impala Editor
SELECT alert_type, COUNT(*) AS cnt, SUM(amount) AS total_inr
FROM default.aml_alerts
GROUP BY alert_type;
```

---

## Troubleshooting

| Error | Cause | Fix |
|------|------|------|
| `UnsupportedClassVersionError: class file version 61.0` | Running on Java 11 | Set `JAVA_HOME=/usr/lib/jvm/java-21` and restart |
| `rest.address must be set` | flink-conf.yaml not configured | Add `rest.address: localhost` |
| `ClassNotFoundException: com.stumbleupon.async.Callback` | async JAR missing | Add `async-1.4.1.jar` to lib/ |
| `ServiceConfigurationError: DnsjavaInetAddressResolverProvider` | kudu-subprocess JAR conflict | Remove `kudu-subprocess-*.jar` |
| `Missing required options: masters` | Wrong Kudu option name | Use `'masters'` (not `kudu.masters`) |
| `Table 'kafka_aml_transactions' not found` | DDL not run in this session | Run 01_kafka_source.sql first |
| `ENV=unknown` | source config/env.conf not run | Run `source config/env.conf` before starting |
