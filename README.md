# SBI AML Detection Pipeline — Demo 가이드

> **대상:** Cloudera를 처음 사용하는 SBI (State Bank of India) 고객  
> **목적:** Cloudera 플랫폼으로 실시간 자금세탁(AML) 탐지를 PoC로 시연

---

## 이 Demo는 무엇을 보여주나요?

인도 최대 은행인 SBI에서 매일 수백만 건의 거래가 발생합니다.  
이 중 자금세탁(Money Laundering) 의심 거래를 **실시간으로 자동 탐지**하는 것이 이 Demo의 목표입니다.

### 탐지하는 2가지 AML 패턴

| 패턴 | 설명 | 기준 (RBI 규정) |
|------|------|----------------|
| **Large Cash** | 단일 거래가 일정 금액 이상 | ₹10,00,000 (10 lakh) 이상 |
| **Smurfing** | 임계값을 피하기 위해 여러 건으로 나눠 거래 | 30분 내 5회 이상 거래 |

### 사용하는 Cloudera 제품

```
CFM (NiFi)  →  Kafka  →  CSA/SSB (Flink)  →  Kudu  →  Impala (Hue)
  데이터 수집    메시지큐    실시간 AML 탐지    저장소     결과 조회
```

| 제품 | 역할 | 버전 |
|------|------|------|
| **CFM 4.12** | 거래 데이터 수집 및 Kafka 전송 | NiFi 2.6.0 |
| **Kafka + SMM** | 실시간 메시지 스트림 | CDP 7.3.1 |
| **CSA 1.9 / SSB** | SQL로 AML 패턴 탐지 | Flink 1.15.1 |
| **Kudu** | 실시간 Alert 저장 (빠른 읽기/쓰기) | CDP 7.3.1 |
| **Impala + Hue** | 탐지 결과 SQL 조회 | CDP 7.3.1 |
| **Ranger** | 보안 정책 (누가 무엇을 볼 수 있는지) | CDP 7.3.1 |

---

## 환경 정보

```
OS       : RHEL 9.6
CM       : Cloudera Manager 7.13.1
CDP      : 7.3.1
CFM      : 4.12.0  (Apache NiFi 2.6.0 기반)
CSA      : 1.9.0.1 (Apache Flink 1.15.1 기반)
네트워크 : Air-gapped (인터넷 차단 환경)
보안     : Kerberos + Auto-TLS + Ranger (전체 활성화)
실행 계정: systest
Keytab   : /opt/cloudera/systest.keytab
Python   : 3.9.x  ← RHEL 9.6 기본 내장, 추가 설치 불필요
```

> **Python 버전 확인:**
> ```bash
> python3 --version   # Python 3.9.x 출력 확인
> python3 -c "import sys; assert sys.version_info >= (3,9), 'Python 3.9 이상 필요'"
> ```

---

## 빠른 시작 (전체 흐름 요약)

```
Step 0  Python 환경   venv 생성 + air-gapped 패키지 설치
Step 1  환경 설정     config/env.conf 편집 (호스트명 입력)
Step 2  환경 검증     bash scripts/01_verify_env.sh
Step 3  인프라 구성   bash infra/01_kafka_setup.sh
                      bash infra/02_run_kudu_ddl.sh
Step 4  Ranger 정책   Ranger UI에서 수동 추가
Step 5  데이터 생성   python data_gen/generate_aml_data.py
Step 6  NiFi 설정     브라우저 → nifi/SETUP_GUIDE.md 따라하기
Step 7  SSB 탐지      bash ssb/render_sql.sh → SSB UI에 붙여넣기
Step 8  결과 확인     bash scripts/02_run_impala.sh
```

---

## AML 탐지 실행 방식 (2가지)

| 방식 | 도구 | 대상 환경 | 가이드 |
|------|------|----------|--------|
| **Primary** | CSA / SSB Web UI + Python REST | CFM + CSA 설치된 환경 | `ssb/` 폴더 |
| **Standalone** | Apache Flink SQL Client (직접 설치) | CFM/CSA 미설치 환경 | `flink/` 폴더 |

> Standalone 방식은 Apache Flink 1.20.1을 직접 설치하여 SSB 없이  
> Flink SQL Client만으로 AML 탐지 Job을 실행합니다.  
> 설치 방법: `flink/SETUP_GUIDE.md`

---

## 프로젝트 구조

```
sbi-aml-detection-pipeline/
│
├── config/                         ← [가장 먼저 편집]
│   ├── env.internal.conf           내부 테스트 환경 설정
│   ├── env.customer.conf           SBI 고객 환경 설정 (호스트명만 변경)
│   └── env.conf → env.internal.conf  현재 활성 환경 (symlink)
│
├── conf/                           ← Kafka/Flink 설정 파일 (conf/ 참조)
│   ├── kafka_jaas.conf             Kafka Kerberos JAAS 템플릿
│   ├── kafka_kerberos.properties   Kafka SSL+Kerberos 속성 파일
│   ├── krb5.conf.example           Kerberos 설정 예제
│   └── flink-conf.yaml.example     Flink 설정 예제
│
├── scripts/
│   ├── 01_verify_env.sh            환경 자동 검증 (Phase 1)
│   └── 02_run_impala.sh            Impala 쿼리 실행 래퍼
│
├── data_gen/
│   ├── generate_aml_data.py        AML 패턴 포함 거래 데이터 생성 (SDV)
│   ├── kafka_producer.py           Kafka 직접 전송 (NiFi 없이)
│   └── requirements.txt            Python 패키지 목록 + air-gapped 설치 가이드
│
├── infra/
│   ├── 01_kafka_setup.sh           Kafka 토픽 생성 (conf/ 기반 JAAS 렌더링)
│   ├── 02_kudu_ddl.sql             Kudu 테이블 스키마
│   ├── 02_run_kudu_ddl.sh          Kudu 테이블 생성 실행
│   ├── 03_ranger_policies.json     Ranger 보안 정책 템플릿
│   └── 05_cleanup.sh               전체 인프라 초기화
│
├── nifi/
│   └── SETUP_GUIDE.md              NiFi Flow 설정 단계별 가이드
│
├── ssb/
│   ├── 01_kafka_source_table.sql.tpl  Kafka 소스 테이블 정의
│   ├── 02_large_cash_job.sql.tpl      Large Cash 탐지 SQL
│   ├── 03_smurfing_job.sql.tpl        Smurfing 탐지 SQL
│   ├── render_sql.sh               SQL 렌더링 (Web UI 방식)
│   └── ssb_rest_client.py          Python API 방식 (자동 제출)
│
├── flink/                          ← [Standalone Flink — CFM/CSA 미설치 환경용]
│   ├── SETUP_GUIDE.md              Flink 1.20.1 설치 가이드 (JAR 구성, flink-conf.yaml)
│   └── sql/
│       ├── 01_kafka_source.sql     Kafka Source Table DDL
│       ├── 02_kudu_aml_alerts.sql  Kudu Sink Table DDL
│       ├── 03_large_cash_job.sql   Large Cash 탐지 INSERT
│       ├── 04_smurfing_job.sql     Smurfing 탐지 INSERT
│       └── 05_run_all.sql          전체 Job 일괄 실행
└── impala/
    └── demo_queries.sql            Demo 검증 쿼리 5개
```

---

## Step 0 — Python 환경 구성 (Air-gapped)

> **핵심 원칙:** `pip download`는 반드시 클러스터와 **동일한 OS인 RHEL 9.6 Bastion 머신**에서 실행합니다.
> macOS 등 다른 OS에서 실행하면 `sdv` 의존성인 `torch` wheel의 플랫폼 태그가 달라 설치가 실패합니다.

### RHEL 9.6 Bastion 머신에서 (인터넷 연결, 1회만)

```bash
# Python 3.9 버전 확인
python3 --version   # Python 3.9.x

# 빌드 도구 + gssapi 시스템 패키지 설치
sudo dnf install -y python3-gssapi krb5-devel gcc python3-devel

# venv 생성 (시스템 gssapi 공유)
python3 -m venv --system-site-packages /tmp/aml-venv
source /tmp/aml-venv/bin/activate
pip install --upgrade pip

# 패키지 다운로드 (플랫폼 옵션 없이 — Bastion이 RHEL 9.6이므로 자동 일치)
pip download -r data_gen/requirements.txt -d ./wheels/

tar cf aml-wheels.tar wheels/
scp aml-wheels.tar systest@<클러스터-호스트>:/tmp/
```

### 클러스터 노드에서 (오프라인 설치)

```bash
# gssapi 시스템 패키지 설치
sudo dnf install -y python3-gssapi krb5-devel

# venv 생성 및 오프라인 설치
python3 -m venv --system-site-packages /tmp/aml-venv
source /tmp/aml-venv/bin/activate

cd /tmp && tar xf aml-wheels.tar
pip install --no-index --find-links=./wheels/ -r /path/to/data_gen/requirements.txt

# 최종 확인
python3 -c "import gssapi, kafka, sdv, pandas, numpy; print('All OK')"
```

> **이후 모든 python 명령은 venv 활성화 후 실행:**
> ```bash
> source /tmp/aml-venv/bin/activate
> ```

---

## Phase 1 — 환경 설정 및 검증

### 1-1. 설정 파일 편집

`config/env.internal.conf`를 열어서 실제 클러스터 호스트명을 입력합니다.

```bash
# 변경할 항목 (CHANGEME 없는지 확인)
KAFKA_BROKERS="실제-브로커1:9093,실제-브로커2:9093,실제-브로커3:9093"
KUDU_MASTERS="실제-kudu마스터1:7051,실제-kudu마스터2:7051"
SSB_HOST="https://실제-SSB호스트:18121"
SSB_PASSWORD="실제-패스워드"
IMPALA_HOST="실제-impala호스트"
NIFI_HOST="https://실제-nifi호스트:8443"
TRUSTSTORE_PW="실제-truststore-패스워드"    # ← 반드시 입력
```

> **TRUSTSTORE_PW 확인 방법:**
> ```bash
> # CM 관리 노드에서 (sudo 필요)
> sudo cat /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.pw
> # 또는 Cloudera Manager UI → Administration → Security → Certificates
> ```

> **팁:** 호스트명은 Cloudera Manager → 서비스 → 인스턴스 탭에서 확인합니다.

### 1-2. 환경 검증 실행

```bash
source config/env.conf       # 환경 변수 로드
bash scripts/01_verify_env.sh
```

모든 항목이 `[OK]`이면 다음 Phase로 진행합니다.

**예상 출력:**
```
=== 1. 설정 파일 확인 ===
  [OK]  KAFKA_BROKERS 설정됨
  [OK]  KUDU_MASTERS 설정됨
  ...
=== 2. Kerberos 인증 ===
  [OK]  kinit 성공 (systest@ROOT.COMOPS.SITE)
  [OK]  TGT 발급 확인
...
[완료] 모든 환경 검증 통과! Phase 2를 시작하세요.
```

### 1-3. Ranger 정책 적용

Ranger는 "누가 어떤 데이터에 접근할 수 있는지" 제어하는 보안 시스템입니다.

1. 브라우저에서 Ranger UI 접속: `https://<ranger-host>:6182`
2. `infra/03_ranger_policies.json`을 참고하여 아래 정책을 **수동으로 추가**:

| 서비스 | 정책 이름 | 대상 | 권한 |
|--------|----------|------|------|
| cm_kafka | aml-kafka-admin | `sbi-aml-transactions`, `sbi-aml-alerts` | create, delete, configure, describe |
| cm_kafka | aml-kafka-producer | `sbi-aml-transactions`, `sbi-aml-alerts` | publish |
| cm_kafka | aml-kafka-consumer | `sbi-aml-transactions`, `sbi-aml-alerts` | consume |
| cm_kudu | aml-kudu-readwrite | `default.aml_transactions`, `default.aml_alerts`, `default.aml_risk_score` | read, write |
| cm_hive | aml-hive-access | `default.aml_transactions`, `default.aml_alerts`, `default.aml_risk_score` | select, create |

> **주의:** 기존 정책은 수정/삭제하지 않습니다. 새 정책만 추가합니다.

---

## Phase 2 — 인프라 구성

### 2-1. Kafka 토픽 생성

```bash
source config/env.conf
bash infra/01_kafka_setup.sh
```

생성되는 토픽:
- `sbi-aml-transactions` — 거래 데이터 (파티션 4개)
- `sbi-aml-alerts` — 알람 데이터 (파티션 2개)

### 2-2. Kudu 테이블 생성

```bash
bash infra/02_run_kudu_ddl.sh
```

생성되는 테이블:
- `aml_transactions` — 거래 원본 데이터
- `aml_alerts` — AML 알람
- `aml_risk_score` — 계좌별 리스크 점수

### 2-3. 테스트 데이터 생성

```bash
# venv 활성화
source /tmp/aml-venv/bin/activate

# 환경 변수 로드
source config/env.conf

# 데이터 생성 (정상 2000건 + AML 패턴 주입)
python data_gen/generate_aml_data.py
```

**생성되는 데이터:**
- 정상 거래 2000건 (SDV GaussianCopulaSynthesizer로 생성)
- Large Cash 거래 4~6건 (₹10 lakh 이상, 특정 계좌)
- Smurfing 거래 15~24건 (3개 계좌 × 5~8회, 30분 내)

생성된 파일: `/tmp/aml-data/aml_transactions_[날짜시각].jsonl`

---

## Phase 3 — CFM (NiFi) 설정

CFM/NiFi는 데이터를 수집하여 Kafka로 전달하는 파이프라인입니다.  
브라우저에서 설정합니다.

```
브라우저 → https://<NIFI_HOST>:8443/nifi
```

**상세 설정 방법:** `nifi/SETUP_GUIDE.md`를 참고하세요.

**사용하는 프로세서 5개:**

```
GetFile → SplitText → UpdateAttribute → PublishKafka2CDP → LogMessage
  파일읽기   줄단위분리    속성추가          Kafka전송(TLS+Kerberos)  로깅
```

> **SplitText 필수:** JSONL 파일을 줄 단위(Line Split Count=1)로 분리해야  
> Kafka 메시지 1건 = 거래 1건이 됩니다. 없으면 파일 전체가 1건으로 전송되어 SSB 파싱 실패합니다.

**사용하는 Controller Services:**

| 서비스 | 역할 |
|--------|------|
| `StandardSSLContextService` | Auto-TLS (truststore.jks) |
| `KerberosUserService` | Kerberos 인증 (NiFi 2.x 방식) |
| `JsonRecordSetWriter` | JSON 출력 형식 |

> **CFM 4.12 주의:** NiFi 2.x에서는 `KerberosCredentialsService` 대신  
> `KerberosUserService`를 사용합니다. 혼동하지 마세요.

---

## Phase 4 — CSA/SSB AML 탐지 설정

SSB(SQL Stream Builder)는 SQL만으로 실시간 데이터 분석을 할 수 있는 도구입니다.

### 방법 A: SSB Web UI (권장 — Demo 시연용)

```bash
# SQL 파일 렌더링 (환경 변수를 실제 값으로 치환)
source config/env.conf
bash ssb/render_sql.sh
```

출력된 SQL 3개를 SSB UI(`https://<SSB_HOST>:18121`)에서 순서대로 실행합니다.

```
[1단계] 01_kafka_source_table.sql → Execute
[2단계] 02_large_cash_job.sql     → Execute  (Large Cash 탐지 Job 시작)
[3단계] 03_smurfing_job.sql       → Execute  (Smurfing 탐지 Job 시작)
```

### 방법 B: Python 스크립트 (고객 기술팀 인계용)

> CSA 1.9.0.1은 PyFlink를 지원하지 않습니다.  
> Python으로 Flink Job을 제어하려면 SSB REST API를 사용합니다.

```bash
source /tmp/aml-venv/bin/activate
source config/env.conf
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE

# 전체 Job 제출
python ssb/ssb_rest_client.py

# 특정 Job만
python ssb/ssb_rest_client.py --job large_cash
python ssb/ssb_rest_client.py --job smurfing

# 실행 중인 Job 확인
python ssb/ssb_rest_client.py --list
```

> **고객 설명 포인트:**  
> "웹 UI에서도, Python 스크립트에서도 동일하게 실행됩니다.  
> 귀사 Python 팀이 기존 자동화 파이프라인에 통합할 수 있습니다."

---

## Phase 5 — Demo 검증 및 실행

### 5-1. Impala 쿼리로 결과 확인

```bash
source config/env.conf
bash scripts/02_run_impala.sh
```

또는 Hue(`https://<hue-host>:8889`)에서 `impala/demo_queries.sql` 내용 실행.

**예상 결과:**
```
탐지 유형    | 알람 건수 | 총 금액 (INR)
LARGE_CASH  |    5     | 23,500,000
SMURFING    |    3     | 12,800,000
```

### 5-2. Demo 시연 순서 (고객 앞)

```
[1] Cloudera Manager  → 서비스 상태 Green 확인
[2] NiFi Canvas       → 데이터 흐름 실시간 시각화
[3] SMM               → sbi-aml-transactions 메시지 수신율 그래프
[4] SSB Web UI        → Large Cash / Smurfing Job 실행 중 확인
[5] Hue (Impala)      → 쿼리 실행 → Alert 결과 실시간 증가 확인
[6] (선택) Python 스크립트 시연 → "Python으로도 동일하게 제어 가능"
```

### 5-3. 라이브 데모 (실시간 탐지 시연)

```bash
# 터미널 1: NiFi가 읽을 데이터를 계속 생성
source /tmp/aml-venv/bin/activate && source config/env.conf
python data_gen/generate_aml_data.py --rows 500

# 터미널 2: NiFi 없이 직접 Kafka 전송 (선택)
python data_gen/kafka_producer.py --rows 500 --rate 3

# 브라우저: Hue에서 쿼리 실행 → Alert 수 증가 확인
bash scripts/02_run_impala.sh
```

---

## 환경 전환 (내부 테스트 → SBI 고객 환경)

```bash
# SBI 고객 환경으로 전환
ln -sf config/env.customer.conf config/env.conf

# 고객 클러스터 정보 입력 (CHANGE_ME 항목들)
vi config/env.customer.conf

# 환경 검증 후 동일하게 실행
source config/env.conf
bash scripts/verify_env.sh
bash infra/01_kafka_setup.sh
bash infra/02_run_kudu_ddl.sh
# NiFi: Parameter Context 값만 고객 환경으로 변경
# SSB: render_sql.sh 재실행 후 Web UI에 붙여넣기
```

### 내부 환경으로 복귀

```bash
ln -sf config/env.internal.conf config/env.conf
```

---

## Kerberos 인증 방식 안내

이 프로젝트의 모든 컴포넌트는 **kinit + OS TGT** 방식을 사용합니다.

```
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
  ↓
OS Kerberos 티켓 캐시(ccache)에 TGT 저장
  ↓
각 컴포넌트가 GSSAPI로 TGT 참조하여 자동 인증
```

| 컴포넌트 | 인증 방식 |
|---------|---------|
| kafka_producer.py | 스크립트 내 `kinit` 자동 호출 |
| Kafka CLI (infra/*.sh) | 스크립트 내 `kinit` 자동 호출 |
| NiFi (CFM) | `KerberosUserService`가 keytab으로 처리 |
| SSB / Flink | SSB 서버가 자동 처리 |
| Impala Shell | `-k` 옵션으로 TGT 사용 |
| ssb_rest_client.py | 실행 전 수동 `kinit` 필요 (README Step 참조) |

> **TGT 유효시간:** 기본 10시간. Demo가 10시간 이내라면 재인증 불필요.  
> 장시간 테스트 시: `kinit -kt ... -r 7d` (갱신 가능 기간 설정)

---

## 문제 해결 가이드

### Python 패키지 설치 실패 (Air-gapped)

```
증상: No matching distribution found for confluent-kafka
해결: requirements.txt에는 confluent-kafka가 없습니다.
     kafka-python을 사용합니다. wheels/ 폴더에 kafka_python-*.whl 확인
```

### Kerberos 인증 실패

```
증상: kinit: Password incorrect
원인: keytab 파일이 없거나 경로 오류
해결:
  ls -la /opt/cloudera/systest.keytab
  klist -kt /opt/cloudera/systest.keytab
```

### Auto-TLS 인증서 파일 없음

```
증상: [FAIL] TRUSTSTORE_JKS 파일 없음
원인: CDP 에이전트가 설치되지 않은 노드에서 실행
해결: ls /var/lib/cloudera-scm-agent/agent-cert/
     CDP 에이전트가 설치된 노드에서 실행
```

### Kafka 연결 실패

```
증상: SASL authentication failed
원인 1: Kerberos TGT 만료 → source config/env.conf && bash scripts/verify_env.sh
원인 2: Ranger Kafka 정책 미적용 → Ranger UI 확인
원인 3: KAFKA_BROKERS 호스트명 오류 → config/env.conf 확인
```

### kafka_producer.py 실행 오류

```
증상: ModuleNotFoundError: No module named 'kafka'
해결: venv가 활성화되지 않음
     source /tmp/aml-venv/bin/activate
```

### SSB에서 Kafka 데이터가 보이지 않음

```
원인 1: NiFi Flow 중지 상태
원인 2: /tmp/aml-data/ 에 .jsonl 파일 없음
확인:   SMM UI → sbi-aml-transactions 토픽 → 메시지 수 확인
해결:   python data_gen/generate_aml_data.py
        python data_gen/kafka_producer.py
```

### Kudu 테이블 생성 실패

```
증상: Table already exists
해결: 02_kudu_ddl.sql에 DROP TABLE IF EXISTS 포함
     bash infra/02_run_kudu_ddl.sh 재실행
```

---

## 자주 묻는 질문 (FAQ)

**Q: Cloudera를 처음 쓰는데 각 제품이 무엇인가요?**

| 제품 | 쉬운 설명 | 비유 |
|------|----------|------|
| CFM/NiFi | 데이터를 A에서 B로 옮기는 파이프 | 우체부 |
| Kafka | 데이터를 잠시 보관하는 대기열 | 우편함 |
| CSA/SSB/Flink | SQL로 데이터를 실시간 분석 | 분석가 |
| Kudu | 빠르게 읽고 쓸 수 있는 저장소 | 정리된 서랍 |
| Impala | SQL로 저장된 데이터를 조회 | 도서관 사서 |
| Ranger | 누가 무엇에 접근할 수 있는지 제어 | 경비원 |

**Q: Python으로 Flink를 직접 실행할 수 없나요?**

CSA 1.9.0.1은 PyFlink(Python Flink API)를 지원하지 않습니다.  
PyFlink는 CSA 1.10부터 지원됩니다.  
현재 버전에서 Python을 사용하려면 `ssb_rest_client.py`(SSB REST API)를 사용하세요.

**Q: confluent-kafka 대신 kafka-python을 사용하는 이유는?**

`confluent-kafka`는 내부적으로 C 라이브러리(`librdkafka`)를 필요로 합니다.  
Air-gapped RHEL 환경에서는 C 라이브러리 빌드가 어려워 설치가 실패할 수 있습니다.  
`kafka-python`은 순수 Python으로 `pip download` → `--no-index` 방식으로 확실하게 설치됩니다.

**Q: Demo 데이터는 실제 거래 데이터인가요?**

아니요, SDV(Synthetic Data Vault) 라이브러리로 생성한 가상 데이터입니다.  
통계적 분포는 실제와 유사하지만, 실제 고객 정보는 포함되지 않습니다.

**Q: Demo 후 데이터는 어떻게 정리하나요?**

```bash
source config/env.conf

# Kafka 토픽 메시지 삭제
kafka-topics --bootstrap-server ${KAFKA_BROKERS} \
  --command-config /tmp/kafka-client.properties \
  --delete --topic sbi-aml-transactions

# Kudu 테이블 데이터 삭제 (Hue에서 실행)
# TRUNCATE TABLE default.aml_alerts;
# TRUNCATE TABLE default.aml_risk_score;

# 로컬 파일 삭제
rm -rf /tmp/aml-data/
```

---

## 기술 스택 상세

| 항목 | 값 |
|------|-----|
| CFM 버전 | 4.12.0 (Apache NiFi 2.6.0) |
| CSA 버전 | 1.9.0.1 (Apache Flink 1.15.1) |
| Python | **3.9.x** (RHEL 9.6 기본 내장, `python3 --version`으로 확인) |
| Kafka 라이브러리 | kafka-python 2.0+ (순수 Python, air-gapped 호환) |
| SDV | 1.9.0+ (GaussianCopulaSynthesizer) |
| 보안 | Kerberos + Auto-TLS + Ranger (전체 활성화) |
| Kerberos 방식 | kinit + OS TGT (GSSAPI) — 전 컴포넌트 동일 |
| 실행 계정 | systest (단일 계정) |
| Keytab 경로 | /opt/cloudera/systest.keytab |
| Kafka 포트 | 9093 (SASL_SSL) |
| Impala 포트 | 21050 |
| SSB 포트 | 18121 |
| NiFi 포트 | 8443 |

---

*이 Demo는 Cloudera SBI AML Detection PoC 프로젝트입니다.*  
*문의: Cloudera Solutions Engineering Team*
