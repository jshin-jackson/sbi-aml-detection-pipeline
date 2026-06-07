# CFM 4.12 (NiFi 2.6) — Flow 설정 가이드

## 개요

이 가이드는 CFM(Cloudera Flow Management) 4.12의 NiFi 2.x UI에서  
AML 거래 데이터를 Kafka로 전송하는 Flow를 설정하는 방법을 설명합니다.

**Flow 구조 (5개 프로세서):**
```
GetFile → SplitText → UpdateAttribute → PublishKafka2CDP → LogMessage
```

> **SplitText가 필요한 이유:**  
> JSONL 파일은 한 줄에 거래 1건입니다. SplitText(Line Split Count=1)로  
> 줄 단위로 나눠야 Kafka 메시지 1건 = 거래 1건이 됩니다.  
> SplitText 없으면 파일 전체가 메시지 1개로 전송되어 SSB 파싱 실패합니다.

**URL:** `https://<NIFI_HOST>:8443/nifi`

---

## Step 1: Parameter Context 생성 (환경 설정 중앙화)

> Parameter Context는 NiFi 2.x의 환경 설정 관리 기능입니다.  
> 값을 한 곳에서 관리하면 환경 전환 시 이 화면만 수정하면 됩니다.

1. NiFi UI 우측 상단 **≡ 메뉴** → **Parameter Contexts** 클릭
2. **+** 버튼으로 새 Parameter Context 생성
3. **Name:** `AML-Environment`
4. **Parameters** 탭에서 아래 값들을 하나씩 추가:

| Parameter 이름 | 값 (내부 환경 예시) | 설명 |
|---|---|---|
| `kafka.brokers` | `ccycloud-1.jshin.root.comops.site:9093,...` | Kafka 브로커 주소 |
| `kafka.topic.txn` | `sbi-aml-transactions` | 거래 Kafka 토픽 |
| `kerberos.keytab` | `/opt/cloudera/systest.keytab` | Kerberos keytab 경로 |
| `kerberos.principal` | `systest@ROOT.COMOPS.SITE` | Kerberos principal |
| `ssl.truststore.path` | `/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks` | TLS truststore 경로 |
| `ssl.truststore.password` | *(truststore 패스워드)* | Sensitive 체크 필수 |
| `data.input.dir` | `/tmp/aml-data` | 데이터 파일 디렉토리 |

5. **Apply** 클릭

---

## Step 2: Controller Services 설정

> Controller Services는 여러 프로세서가 공유하는 연결 설정입니다.  
> SSL, Kerberos 설정을 한 번만 하면 모든 프로세서에서 재사용합니다.

### 2-1. Process Group 생성

1. NiFi Canvas (빈 화면)에 마우스 우클릭 → **Add Process Group**
2. Name: `AML-Ingest-Flow`
3. Parameter Context: `AML-Environment` 선택
4. **Add** 클릭

### 2-2. Process Group 진입 및 Controller Services 설정

1. `AML-Ingest-Flow` 더블클릭하여 진입
2. 상단 메뉴 **Configure** (설정 아이콘) 클릭
3. **Controller Services** 탭 선택

**Controller Service 1: StandardSSLContextService**

| 속성 | 값 |
|---|---|
| Truststore Filename | `#{ssl.truststore.path}` |
| Truststore Password | `#{ssl.truststore.password}` |
| Truststore Type | `JKS` |

→ **Enable** (번개 아이콘) 클릭하여 활성화

**Controller Service 2: KerberosUserService**

> NiFi 2.x에서는 `KerberosCredentialsService` 대신 `KerberosUserService`를 사용합니다.

| 속성 | 값 |
|---|---|
| Kerberos Keytab | `#{kerberos.keytab}` |
| Kerberos Principal | `#{kerberos.principal}` |

→ **Enable** 클릭하여 활성화

**Controller Service 3: JsonRecordSetWriter**

| 속성 | 값 |
|---|---|
| Schema Access Strategy | `Infer Schema` |

→ **Enable** 클릭하여 활성화

---

## Step 3: 프로세서 추가

Canvas 빈 공간에서 각 프로세서를 드래그하여 추가합니다.

### Processor 1: GetFile

**추가 방법:** Canvas 우클릭 → Add Processor → `GetFile` 검색 → Add

| 속성 | 값 | 설명 |
|---|---|---|
| Input Directory | `#{data.input.dir}` | 데이터 파일 디렉토리 |
| File Filter | `.*\.jsonl` | JSONL 파일만 읽기 |
| Polling Interval | `5 sec` | 5초마다 새 파일 확인 |
| Keep Source File | `false` | 처리 후 파일 삭제 |

### Processor 2: SplitText

> **핵심:** JSONL 파일을 줄 단위로 분리합니다.  
> 거래 1건(한 줄) = FlowFile 1개 = Kafka 메시지 1개가 되어야 SSB Flink가 올바르게 파싱합니다.

| 속성 | 값 | 설명 |
|---|---|---|
| Line Split Count | `1` | 한 줄 = FlowFile 1개 |
| Header Line Count | `0` | 헤더 없음 |
| Remove Trailing Newlines | `true` | 개행문자 제거 |

### Processor 3: UpdateAttribute

| 속성 | 값 | 설명 |
|---|---|---|
| `mime.type` | `application/json` | Content-Type 설정 |

### Processor 4: PublishKafka2CDP

> `PublishKafka2CDP`는 CFM 4.x의 Cloudera 전용 Kafka 프로세서입니다.  
> NiFi 2.x의 `KerberosUserService`와 함께 동작합니다.

| 속성 | 값 | 설명 |
|---|---|---|
| Kafka Brokers | `#{kafka.brokers}` | 브로커 주소 |
| Topic Name | `#{kafka.topic.txn}` | 토픽 이름 |
| SSL Context Service | `StandardSSLContextService` | TLS 설정 |
| Kerberos User Service | `KerberosUserService` | Kerberos 인증 |
| Record Writer | `JsonRecordSetWriter` | JSON 형식 출력 |
| Delivery Guarantee | `Best Effort` | Demo용 (성능 우선) |

### Processor 5: LogMessage

| 속성 | 값 |
|---|---|
| Log Level | `info` |
| Log Prefix | `[AML-SENT]` |
| Log payload | `true` |

---

## Step 4: 프로세서 연결 (Connection)

프로세서 사이를 드래그하여 연결합니다.

```
GetFile ──[success]──► SplitText ──[splits]──► UpdateAttribute ──[success]──► PublishKafka2CDP ──[success]──► LogMessage
                           │                                                           │
                       [original]                                                 [failure]
                           │                                                           │
                       (terminate)                                                (terminate)
```

**연결 방법:**
1. GetFile → SplitText: Relationship `success` 선택
2. SplitText → UpdateAttribute: Relationship `splits` 선택
3. SplitText의 `original`: **Terminate** 선택 (원본 파일 참조 종료)
4. UpdateAttribute → PublishKafka2CDP: Relationship `success` 선택
5. PublishKafka2CDP → LogMessage: Relationship `success` 선택
6. PublishKafka2CDP의 `failure`: **Terminate** 선택

---

## Step 5: Flow 시작

1. Canvas 빈 공간 클릭 → **Ctrl+A** (전체 선택)
2. 우클릭 → **Start**
3. 모든 프로세서가 초록색(실행 중) 상태 확인

**확인:**
- SplitText: In/Out 카운터가 파일 1개 → N건으로 분리됨을 확인
- PublishKafka2CDP: 전송 성공 카운터 증가 확인
- SMM UI에서 `sbi-aml-transactions` 토픽 메시지 수 증가 확인

---

## Flow Export (환경 전환 백업용)

현재 Flow를 JSON으로 저장해두면 고객 환경에서 바로 Import할 수 있습니다.

1. `AML-Ingest-Flow` Process Group 우클릭 → **Download Flow Definition**
2. `aml_ingest_flow.json` 파일로 저장
3. 이 파일을 `nifi/` 폴더에 보관

**고객 환경에서 Import:**
1. NiFi Canvas 우클릭 → **Upload Flow Definition**
2. 저장된 JSON 파일 업로드
3. Parameter Context의 값만 고객 환경 값으로 수정

---

## 문제 해결

| 증상 | 원인 | 해결 방법 |
|---|---|---|
| `GetFile` 프로세서가 파일을 읽지 않음 | 디렉토리 권한 문제 | `chmod 755 /tmp/aml-data` |
| SSB에서 Kafka 메시지 파싱 실패 | SplitText 미설정 → 파일 전체가 1건으로 전송됨 | SplitText 추가, Line Split Count=1 확인 |
| `PublishKafka2CDP` 빨간 경고 | Kerberos 인증 실패 | `KerberosUserService` 상태 확인 |
| SSL 연결 오류 | Truststore 경로/패스워드 오류 | `StandardSSLContextService` 재확인 |
| `KerberosUserService` 활성화 실패 | Keytab 파일 없음 | `ls -la /opt/cloudera/systest.keytab` |
