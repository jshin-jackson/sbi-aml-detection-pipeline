# Apache Flink 1.20.1 Standalone 설치 가이드

> CFM/CSA가 설치되지 않은 환경에서 Apache Flink를 직접 설치하여  
> AML 탐지 Job을 실행하는 방법입니다.  
> SSB(SQL Stream Builder) 없이 **Flink SQL Client**를 사용합니다.

---

## 검증 환경

```
OS        : RHEL 9.6
Java      : OpenJDK 21
Flink     : 1.20.1 (Standalone 모드)
Kudu 커넥터: flink-connector-kudu-2.0-csa1.17.1.0.jar (CSA 1.17.1)
Kafka 커넥터: flink-sql-connector-kafka-3.4.0-1.20.jar
```

---

## Step 1: Java 21 설치

```bash
sudo dnf install -y java-21-openjdk

# 설치 확인
java -version   # 21.x.x

# JAVA_HOME 설정
export JAVA_HOME=/usr/lib/jvm/java-21
```

---

## Step 2: Flink 1.20.1 설치

```bash
cd /opt
wget https://archive.apache.org/dist/flink/flink-1.20.1/flink-1.20.1-bin-scala_2.12.tgz
tar xzf flink-1.20.1-bin-scala_2.12.tgz
ln -s /opt/flink-1.20.1 /opt/flink
```

---

## Step 3: 필요한 JAR 파일 구성

### 3-1. Kafka SQL 커넥터 다운로드 (인터넷 필요)

```bash
wget -P /opt/flink/lib/ \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
```

### 3-2. Kudu 커넥터 — CDH 파슬에서 복사

```bash
# Kudu 커넥터 (CSA 파슬에서)
# CSA 파슬 경로 예: /opt/cloudera/parcels/FLINK/ 또는 다른 위치에서 찾기
find /opt/cloudera/ -name "flink-connector-kudu*.jar" 2>/dev/null
cp <찾은 경로>/flink-connector-kudu-2.0-csa1.17.1.0.jar /opt/flink/lib/

# Kudu 클라이언트 의존성 — CDH 파슬에서 복사
cp /opt/cloudera/parcels/CDH/jars/kudu-client-1.17.0.7.3.1.600-325.jar /opt/flink/lib/
cp /opt/cloudera/parcels/CDH/jars/kudu-proto-1.17.0.7.3.1.600-325.jar  /opt/flink/lib/

# async 라이브러리 (kudu-client 의존성)
wget -P /opt/flink/lib/ \
  https://repo1.maven.org/maven2/com/stumbleupon/async/1.4.1/async-1.4.1.jar
```

### 3-3. 최종 /opt/flink/lib 구성 확인

```
async-1.4.1.jar
flink-cep-1.20.1.jar
flink-connector-files-1.20.1.jar
flink-connector-kudu-2.0-csa1.17.1.0.jar    ← Kudu 커넥터
flink-csv-1.20.1.jar
flink-dist-1.20.1.jar
flink-json-1.20.1.jar
flink-scala_2.12-1.20.1.jar
flink-sql-connector-kafka-3.4.0-1.20.jar     ← Kafka 커넥터
flink-table-api-java-uber-1.20.1.jar
flink-table-planner-loader-1.20.1.jar
flink-table-runtime-1.20.1.jar
kudu-client-1.17.0.7.3.1.600-325.jar         ← Kudu 클라이언트
kudu-proto-1.17.0.7.3.1.600-325.jar          ← Kudu Protobuf
log4j-*.jar (기본 포함)
```

> **주의:** `kudu-subprocess`, `kudu-backup`, `kudu-spark3`, `kudu-hive` JAR은  
> SLF4J 충돌 등을 유발하므로 절대 추가하지 마세요.

---

## Step 4: flink-conf.yaml 설정

`/opt/flink/conf/flink-conf.yaml` 편집:

```yaml
# REST / JobManager
rest.address: localhost
rest.port: 8081
jobmanager.rpc.address: localhost
jobmanager.rpc.port: 6123

# 메모리 (Demo 최소 설정)
jobmanager.memory.process.size: 1600m
taskmanager.memory.process.size: 1728m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 2

# Kerberos
security.kerberos.login.use-ticket-cache: false
security.kerberos.login.keytab: /opt/cloudera/systest.keytab
security.kerberos.login.principal: systest@ROOT.COMOPS.SITE
security.kerberos.login.contexts: Client,KafkaClient

# JAAS (따옴표 없이 작성 — shell eval 오류 방지)
env.java.opts: -Djava.security.auth.login.config=/opt/flink/conf/flink-jaas.conf
```

### flink-jaas.conf 생성

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

### flink-env.sh에 Java 21 고정

```bash
echo 'JAVA_HOME=/usr/lib/jvm/java-21' >> /opt/flink/conf/flink-env.sh
```

---

## Step 5: Flink 시작

```bash
export JAVA_HOME=/usr/lib/jvm/java-21
/opt/flink/bin/start-cluster.sh

# 정상 확인
curl http://localhost:8081/overview
```

---

## Step 6: SQL Client로 AML 탐지 실행

```bash
/opt/flink/bin/sql-client.sh
```

SQL Client에서 아래 SQL 파일들을 순서대로 실행합니다.

> **주의:** Flink SQL Client는 세션이 종료되면 테이블 정의가 사라집니다.  
> 매번 새 세션 시작 시 Step 1 → Step 2 → Step 3 순으로 실행해야 합니다.

```sql
-- [Step 1] flink/sql/01_kafka_source.sql 내용 붙여넣기
-- [Step 2] flink/sql/02_kudu_aml_alerts.sql 내용 붙여넣기
-- [Step 3] flink/sql/03_large_cash_job.sql 내용 붙여넣기
-- [Step 4] flink/sql/04_smurfing_job.sql 내용 붙여넣기
```

---

## Step 7: Impala(Hue)에서 결과 확인

```sql
-- Hue Impala Editor에서 실행
SELECT alert_type, COUNT(*) AS cnt, SUM(amount) AS total_inr
FROM default.aml_alerts
GROUP BY alert_type;
```

---

## 문제 해결

| 오류 | 원인 | 해결 |
|------|------|------|
| `UnsupportedClassVersionError: class file version 61.0` | Java 11로 실행 중 | `JAVA_HOME=/usr/lib/jvm/java-21` 설정 후 재시작 |
| `rest.address must be set` | flink-conf.yaml 미설정 | `rest.address: localhost` 추가 |
| `ClassNotFoundException: com.stumbleupon.async.Callback` | async JAR 누락 | `async-1.4.1.jar` 추가 |
| `ServiceConfigurationError: DnsjavaInetAddressResolverProvider` | kudu-subprocess JAR 충돌 | `kudu-subprocess-*.jar` 제거 |
| `NoClassDefFoundError: Missing required options: masters` | Kudu 옵션명 오류 | `'masters'` (kudu. prefix 없이) 사용 |
| `Table 'kafka_aml_transactions' not found` | 새 세션에서 DDL 미실행 | 01_kafka_source.sql 먼저 실행 |
| `ENV=unknown` | source config/env.conf 미실행 | `source config/env.conf` 후 실행 |
