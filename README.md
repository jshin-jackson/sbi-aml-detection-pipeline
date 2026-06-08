# SBI AML Detection Pipeline — Demo Guide

> **Audience:** SBI (State Bank of India) customers who are new to Cloudera  
> **Purpose:** Demonstrate real-time Anti-Money Laundering (AML) detection using the Cloudera platform as a PoC

---

## What Does This Demo Show?

SBI processes millions of transactions every day.  
This demo automatically detects suspicious money laundering transactions **in real time**.

### Two AML Patterns Detected

| Pattern | Description | Threshold (RBI Regulation) |
|------|------|----------------|
| **Large Cash** | A single transaction exceeds a defined amount | ≥ ₹10,00,000 (10 lakh) |
| **Smurfing** | Multiple small transactions to avoid the reporting threshold | 5 or more transactions within 30 minutes |

### Cloudera Products Used

```
CFM (NiFi)  →  Kafka  →  CSA/SSB (Flink)  →  Kudu  →  Impala (Hue)
 Data Ingestion  Message Queue  Real-time AML Detection  Storage  Query
```

| Product | Role | Version |
|------|------|------|
| **CFM 4.12** | Collect transaction data and send to Kafka | NiFi 2.6.0 |
| **Kafka + SMM** | Real-time message stream | CDP 7.3.1 |
| **CSA 1.9 / SSB** | Detect AML patterns using SQL | Flink 1.15.1 |
| **Kudu** | Store real-time alerts (fast read/write) | CDP 7.3.1 |
| **Impala + Hue** | Query detection results using SQL | CDP 7.3.1 |
| **Ranger** | Security policies (access control) | CDP 7.3.1 |

---

## AML Detection Execution Methods (2 Options)

| Method | Tool | Target Environment | Guide |
|------|------|----------|--------|
| **Primary** | CSA / SSB Web UI + Python REST API | Environment with CFM + CSA installed | `ssb/` directory |
| **Standalone** | Apache Flink SQL Client (direct install) | Environment without CFM/CSA | `flink/` directory |

> The Standalone method installs Apache Flink 1.20.1 directly and runs AML detection jobs  
> using only the Flink SQL Client, without SSB.  
> Installation guide: `flink/SETUP_GUIDE.md`

---

## Environment Info

```
OS        : RHEL 9.6
CM        : Cloudera Manager 7.13.1
CDP       : 7.3.1
CFM       : 4.12.0  (Apache NiFi 2.6.0)
CSA       : 1.9.0.1 (Apache Flink 1.15.1)
Network   : Air-gapped (no internet access)
Security  : Kerberos + Auto-TLS + Ranger (all enabled)
Run-as    : systest
Keytab    : /opt/cloudera/systest.keytab
Python    : 3.9.x  ← Built into RHEL 9.6, no additional install needed
```

> **Verify Python version:**
> ```bash
> python3 --version   # Expect: Python 3.9.x
> python3 -c "import sys; assert sys.version_info >= (3,9), 'Python 3.9+ required'"
> ```

---

## Quick Start (Summary)

```
Step 0  Python Setup    Create venv + install packages (air-gapped)
Step 1  Configuration   Edit config/env.conf (enter hostnames)
Step 2  Verify Env      bash scripts/verify_env.sh
Step 3  Infrastructure  bash infra/01_kafka_setup.sh
                        bash infra/02_run_kudu_ddl.sh
Step 4  Ranger Policies Add manually in Ranger UI
Step 5  Generate Data   python data_gen/generate_aml_data.py
Step 6  NiFi Setup      Browser → follow nifi/SETUP_GUIDE.md
Step 7  SSB Detection   bash ssb/render_sql.sh → paste into SSB UI
Step 8  Verify Results  bash scripts/run_impala.sh
```

---

## Project Structure

```
sbi-aml-detection-pipeline/
│
├── config/                         ← [Edit first]
│   ├── env.internal.conf           Internal test environment settings
│   ├── env.customer.conf           SBI customer environment settings (change hostnames only)
│   └── env.conf → env.internal.conf  Active environment (symlink)
│
├── scripts/
│   ├── verify_env.sh               Automated environment verification (Phase 1)
│   └── run_impala.sh               Impala query runner wrapper
│
├── data_gen/
│   ├── generate_aml_data.py        Generate transaction data with AML patterns (SDV)
│   ├── kafka_producer.py           Send data directly to Kafka (without NiFi)
│   └── requirements.txt            Python package list + air-gapped install guide
│
├── infra/
│   ├── 01_kafka_setup.sh           Create Kafka topics
│   ├── 02_kudu_ddl.sql             Kudu table schema
│   ├── 02_run_kudu_ddl.sh          Run Kudu table creation
│   └── 03_ranger_policies.json     Ranger security policy template
│
├── nifi/
│   └── SETUP_GUIDE.md              Step-by-step NiFi Flow setup guide
│
├── ssb/                            ← Primary: CSA/SSB method
│   ├── 01_kafka_source_table.sql.tpl  Kafka source table definition
│   ├── 02_large_cash_job.sql.tpl      Large Cash detection SQL
│   ├── 03_smurfing_job.sql.tpl        Smurfing detection SQL
│   ├── render_sql.sh               Render SQL templates (Web UI method)
│   └── ssb_rest_client.py          Python API method (auto-submit)
│
├── flink/                          ← Standalone Flink (no CFM/CSA needed)
│   ├── SETUP_GUIDE.md              Flink 1.20.1 install guide (JAR list, flink-conf.yaml)
│   └── sql/
│       ├── 01_kafka_source.sql     Kafka Source Table DDL
│       ├── 02_kudu_aml_alerts.sql  Kudu Sink Table DDL
│       ├── 03_large_cash_job.sql   Large Cash detection INSERT
│       ├── 04_smurfing_job.sql     Smurfing detection INSERT
│       └── 05_run_all.sql          Run all jobs at once
└── impala/
    └── demo_queries.sql            5 demo verification queries
```

---

## Step 0 — Python Environment Setup (Air-gapped)

> **Key rule:** Run `pip download` on a **RHEL 9.6 Bastion machine with the same OS/arch as the cluster**.  
> Running on macOS or other OS will fail because `sdv` depends on `torch`,  
> whose wheel platform tags won't match.

### On the RHEL 9.6 Bastion Machine (needs internet, one-time)

```bash
# Verify Python 3.9 (built into RHEL 9.6)
python3 --version   # Python 3.9.x

# Install gssapi and Kerberos build tools
sudo dnf install -y python3-gssapi krb5-devel gcc python3-devel

# Create venv (share system gssapi)
python3 -m venv --system-site-packages /tmp/aml-venv
source /tmp/aml-venv/bin/activate
pip install --upgrade pip

# Download packages (no platform flags needed — Bastion is RHEL 9.6)
pip download -r data_gen/requirements.txt -d ./wheels/

tar cf aml-wheels.tar wheels/
scp aml-wheels.tar systest@<cluster-host>:/tmp/
```

### On the Cluster Node (offline install)

```bash
# Install gssapi system package
sudo dnf install -y python3-gssapi krb5-devel

# Create venv and install offline
python3 -m venv --system-site-packages /tmp/aml-venv
source /tmp/aml-venv/bin/activate

cd /tmp && tar xf aml-wheels.tar
pip install --no-index --find-links=./wheels/ -r /path/to/data_gen/requirements.txt

# Verify
python3 -c "import gssapi, kafka, sdv, pandas, numpy; print('All OK')"
```

> **Always activate venv before running python commands:**
> ```bash
> source /tmp/aml-venv/bin/activate
> ```

---

## Phase 1 — Configuration & Verification

### 1-1. Edit Configuration File

Open `config/env.internal.conf` and enter the actual cluster hostnames.

```bash
# Items to change (ensure no CHANGEME values remain)
KAFKA_BROKERS="actual-broker1:9093,actual-broker2:9093,actual-broker3:9093"
KUDU_MASTERS="actual-kudu-master1:7051"
SSB_HOST="https://actual-ssb-host:18121"
SSB_PASSWORD="actual-password"
IMPALA_HOST="actual-impala-host"
NIFI_HOST="https://actual-nifi-host:8443"
TRUSTSTORE_PW="actual-truststore-password"    # ← Required
```

> **Find TRUSTSTORE_PW:**
> ```bash
> # On CM admin node (requires sudo)
> sudo cat /var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.pw
> # Or: Cloudera Manager UI → Administration → Security → Certificates
> ```

### 1-2. Run Environment Verification

```bash
source config/env.conf
bash scripts/verify_env.sh
```

All items must show `[OK]` before proceeding.

**Expected output:**
```
=== 1. Configuration Check ===
  [OK]  KAFKA_BROKERS is set
  [OK]  KUDU_MASTERS is set
  ...
=== 2. Kerberos Authentication ===
  [OK]  kinit succeeded (systest@ROOT.COMOPS.SITE)
  [OK]  TGT verified
...
[DONE] All environment checks passed! Proceed to Phase 2.
```

### 1-3. Apply Ranger Policies

Ranger controls "who can access what data."

1. Open Ranger UI: `https://<ranger-host>:6182`
2. Refer to `infra/03_ranger_policies.json` and add policies **manually**:

| Service | Policy Name | Resource | Permission |
|--------|----------|------|------|
| cm_kafka | aml-kafka-admin | `sbi-aml-transactions`, `sbi-aml-alerts` | create, delete, configure, describe |
| cm_kafka | aml-kafka-producer | `sbi-aml-transactions`, `sbi-aml-alerts` | publish |
| cm_kafka | aml-kafka-consumer | `sbi-aml-transactions`, `sbi-aml-alerts` | consume |
| cm_kudu | aml-kudu-readwrite | `default.aml_transactions`, `default.aml_alerts`, `default.aml_risk_score` | read, write |
| cm_hive | aml-hive-access | `default.aml_transactions`, `default.aml_alerts`, `default.aml_risk_score` | select, create |

> **Note:** Add new policies only — do not modify existing policies.

---

## Phase 2 — Infrastructure Setup

### 2-1. Create Kafka Topics

```bash
source config/env.conf
bash infra/01_kafka_setup.sh
```

Topics created:
- `sbi-aml-transactions` — transaction data (4 partitions)
- `sbi-aml-alerts` — alert data (2 partitions)

### 2-2. Create Kudu Tables

```bash
bash infra/02_run_kudu_ddl.sh
```

Tables created:
- `aml_transactions` — raw transaction data
- `aml_alerts` — AML alerts
- `aml_risk_score` — per-account risk scores

### 2-3. Generate Test Data

```bash
# Activate venv
source /tmp/aml-venv/bin/activate

# Load env vars
source config/env.conf

# Generate data (2000 normal + injected AML patterns)
python data_gen/generate_aml_data.py
```

**Data generated:**
- 2000 normal transactions
- 4–6 Large Cash transactions (≥ ₹10 lakh)
- 15–24 Smurfing transactions (3 accounts × 5–8 txns, within 30 min)

Output file: `/tmp/aml-data/aml_transactions_[timestamp].jsonl`

---

## Phase 3 — CFM (NiFi) Setup

CFM/NiFi is a pipeline that collects data and delivers it to Kafka.  
Configure it in the browser.

```
Browser → https://<NIFI_HOST>:8443/nifi
```

**Detailed setup:** See `nifi/SETUP_GUIDE.md`

**5 Processors used:**

```
GetFile → SplitText → UpdateAttribute → PublishKafka2CDP → LogMessage
 Read file  Split lines  Set attributes    Send to Kafka(TLS+Kerberos)  Log
```

> **SplitText is required:** The JSONL file contains one transaction per line.  
> SplitText (Line Split Count=1) splits each line into a separate FlowFile,  
> so each Kafka message = 1 transaction. Without it, the entire file becomes 1 message and SSB parsing fails.

**Controller Services:**

| Service | Role |
|--------|------|
| `StandardSSLContextService` | Auto-TLS (truststore.jks) |
| `KerberosUserService` | Kerberos auth (NiFi 2.x method) |
| `JsonRecordSetWriter` | JSON output format |

> **CFM 4.12 Note:** NiFi 2.x uses `KerberosUserService` instead of `KerberosCredentialsService`.  
> Do not confuse them.

---

## Phase 4 — CSA/SSB AML Detection Setup

SSB (SQL Stream Builder) enables real-time data analytics using only SQL.

### Method A: SSB Web UI (Recommended — for demo)

```bash
# Render SQL templates (substitute env vars)
source config/env.conf
bash ssb/render_sql.sh
```

Paste the rendered SQL files in order into SSB UI (`https://<SSB_HOST>:18121`):

```
[Step 1] 01_kafka_source_table.sql → Execute
[Step 2] 02_large_cash_job.sql     → Execute  (starts Large Cash detection job)
[Step 3] 03_smurfing_job.sql       → Execute  (starts Smurfing detection job)
```

### Method B: Python Script (for customer tech team handover)

> CSA 1.9.0.1 does not support PyFlink.  
> To control Flink jobs using Python, use the SSB REST API.

```bash
source /tmp/aml-venv/bin/activate
source config/env.conf
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE

# Submit all jobs
python ssb/ssb_rest_client.py

# Submit specific job
python ssb/ssb_rest_client.py --job large_cash
python ssb/ssb_rest_client.py --job smurfing

# List running jobs
python ssb/ssb_rest_client.py --list
```

> **Demo talking point:**  
> "You can run the same SQL from the Web UI or a Python script.  
> Your Python team can integrate this into existing automation pipelines."

---

## Phase 5 — Verification & Demo Execution

### 5-1. Verify Results with Impala

```bash
source config/env.conf
bash scripts/run_impala.sh
```

Or run queries directly in Hue (`https://<hue-host>:8889`) from `impala/demo_queries.sql`.

**Expected results:**
```
Alert Type   | Count | Total (INR)
LARGE_CASH  |   5   | 23,500,000
SMURFING    |   3   | 12,800,000
```

### 5-2. Demo Scenario (in front of customer)

```
[1] Cloudera Manager → Verify all services are Green
[2] NiFi Canvas      → Visualize real-time data flow
[3] SMM              → sbi-aml-transactions message rate graph
[4] SSB Web UI       → Confirm Large Cash / Smurfing jobs are RUNNING
[5] Hue (Impala)     → Run query → See alerts increasing in real time
[6] (Optional) Python script demo → "Controllable from Python too"
```

### 5-3. Live Demo (real-time detection)

```bash
# Terminal 1: Continuously generate data for NiFi to read
source /tmp/aml-venv/bin/activate && source config/env.conf
python data_gen/generate_aml_data.py --rows 500

# Terminal 2: Send directly to Kafka (optional, without NiFi)
python data_gen/kafka_producer.py --rows 500 --rate 3

# Browser: Run Hue query → Watch alert count increase
bash scripts/run_impala.sh
```

---

## Environment Switching (Internal → SBI Customer)

```bash
# Switch to SBI customer environment
ln -sf config/env.customer.conf config/env.conf

# Enter customer cluster info (CHANGE_ME items)
vi config/env.customer.conf

# Verify and run same steps
source config/env.conf
bash scripts/verify_env.sh
bash infra/01_kafka_setup.sh
bash infra/02_run_kudu_ddl.sh
# NiFi: Update Parameter Context values for customer env
# SSB: Re-run render_sql.sh and paste into Web UI
```

### Switch Back to Internal Environment

```bash
ln -sf config/env.internal.conf config/env.conf
```

---

## Kerberos Authentication

All components in this project use the **kinit + OS TGT** method.

```
kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
  ↓
OS Kerberos ticket cache (ccache) stores TGT
  ↓
Each component uses GSSAPI to reference the TGT for automatic auth
```

| Component | Authentication |
|---------|---------|
| kafka_producer.py | `kinit` called automatically inside the script |
| Kafka CLI (infra/*.sh) | `kinit` called automatically inside the script |
| NiFi (CFM) | `KerberosUserService` handles keytab auth |
| SSB / Flink | SSB server handles automatically |
| Impala Shell | Uses TGT via `-k` flag |
| ssb_rest_client.py | Manual `kinit` required before running |

> **TGT validity:** Default 10 hours. No re-auth needed if demo is under 10 hours.  
> For extended testing: `kinit -kt ... -r 7d` (set renewable period)

---

## Troubleshooting

### Python Package Install Failed (Air-gapped)

```
Symptom: No matching distribution found for confluent-kafka
Fix: requirements.txt does not use confluent-kafka.
     kafka-python is used instead. Verify kafka_python-*.whl is in wheels/
```

### Kerberos Authentication Failed

```
Symptom: kinit: Password incorrect
Cause: Missing or wrong path to keytab file
Fix:
  ls -la /opt/cloudera/systest.keytab
  klist -kt /opt/cloudera/systest.keytab
```

### Auto-TLS Certificate File Not Found

```
Symptom: [FAIL] TRUSTSTORE_JKS file not found
Cause: Running on a node without CDP agent installed
Fix: ls /var/lib/cloudera-scm-agent/agent-cert/
     Run on a node with CDP agent installed
```

### Kafka Connection Failed

```
Symptom: SASL authentication failed
Cause 1: Kerberos TGT expired → re-run: source config/env.conf && bash scripts/verify_env.sh
Cause 2: Ranger Kafka policy not applied → check Ranger UI
Cause 3: Wrong KAFKA_BROKERS hostname → check config/env.conf
```

### kafka_producer.py Runtime Error

```
Symptom: ModuleNotFoundError: No module named 'kafka'
Fix: venv is not activated
     source /tmp/aml-venv/bin/activate
```

### SSB Not Receiving Kafka Data

```
Cause 1: NiFi Flow is stopped
Cause 2: No .jsonl files in /tmp/aml-data/
Check:   SMM UI → sbi-aml-transactions topic → verify message count
Fix:     python data_gen/generate_aml_data.py
         python data_gen/kafka_producer.py
```

### Kudu Table Creation Failed

```
Symptom: Table already exists
Fix: 02_kudu_ddl.sql includes DROP TABLE IF EXISTS
     Simply re-run: bash infra/02_run_kudu_ddl.sh
```

---

## FAQ

**Q: I'm new to Cloudera — what does each product do?**

| Product | Plain explanation | Analogy |
|------|----------|------|
| CFM/NiFi | A pipeline that moves data from A to B | Mail carrier |
| Kafka | A queue that temporarily holds data | Mailbox |
| CSA/SSB/Flink | Real-time data analytics using SQL | Data analyst |
| Kudu | Fast-read/write storage | Organized filing cabinet |
| Impala | Query stored data using SQL | Librarian |
| Ranger | Controls who can access what | Security guard |

**Q: Why can't I run Flink directly from Python?**

CSA 1.9.0.1 does not support PyFlink (Python Flink API).  
PyFlink is supported from CSA 1.10+.  
For Python-based control on this version, use `ssb_rest_client.py` (SSB REST API).

**Q: Why is kafka-python used instead of confluent-kafka?**

`confluent-kafka` requires `librdkafka` (a C library) internally.  
Building C libraries is difficult in air-gapped RHEL environments and may fail.  
`kafka-python` is pure Python and installs reliably via `pip download` → `--no-index`.

**Q: Is the demo data real transaction data?**

No. It is synthetic data generated by the SDV (Synthetic Data Vault) library.  
The statistical distribution is realistic, but no real customer information is included.

**Q: How do I clean up after the demo?**

```bash
source config/env.conf

# Delete Kafka topic messages
kafka-topics --bootstrap-server ${KAFKA_BROKERS} \
  --command-config /tmp/kafka-client.properties \
  --delete --topic sbi-aml-transactions

# Truncate Kudu table data (run in Hue)
# TRUNCATE TABLE default.aml_alerts;
# TRUNCATE TABLE default.aml_risk_score;

# Delete local files
rm -rf /tmp/aml-data/
```

---

## Tech Stack Details

| Item | Value |
|------|-----|
| CFM Version | 4.12.0 (Apache NiFi 2.6.0) |
| CSA Version | 1.9.0.1 (Apache Flink 1.15.1) |
| Python | **3.9.x** (built into RHEL 9.6, verify with `python3 --version`) |
| Kafka Library | kafka-python 2.0+ (pure Python, air-gapped compatible) |
| SDV | 1.9.0+ (GaussianCopulaSynthesizer) |
| Security | Kerberos + Auto-TLS + Ranger (all enabled) |
| Kerberos Method | kinit + OS TGT (GSSAPI) — same across all components |
| Run-as Account | systest (single account) |
| Keytab Path | /opt/cloudera/systest.keytab |
| Kafka Port | 9093 (SASL_SSL) |
| Impala Port | 21050 |
| SSB Port | 18121 |
| NiFi Port | 8443 |

---

*This demo is part of the Cloudera SBI AML Detection PoC project.*  
*Contact: Cloudera Solutions Engineering Team*
