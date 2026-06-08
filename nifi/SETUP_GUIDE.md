# CFM 4.12 (NiFi 2.6) — Flow Setup Guide

## Overview

This guide explains how to configure a NiFi 2.x Flow in CFM (Cloudera Flow Management) 4.12  
to collect AML transaction data and send it to Kafka.

**Flow structure (5 processors):**
```
GetFile → SplitText → UpdateAttribute → PublishKafka2CDP → LogMessage
```

> **Why SplitText is required:**  
> A JSONL file contains one transaction per line. SplitText (Line Split Count=1) splits  
> each line into a separate FlowFile, ensuring each Kafka message = 1 transaction.  
> Without SplitText, the entire file is sent as a single Kafka message, causing SSB parsing to fail.

**URL:** `https://<NIFI_HOST>:8443/nifi`

---

## Step 1: Create Parameter Context (Centralize Environment Settings)

> Parameter Context is a NiFi 2.x feature for centralized configuration management.  
> Managing values in one place means only this screen needs to be updated when switching environments.

1. Click **≡ menu** (top right in NiFi UI) → **Parameter Contexts**
2. Click **+** to create a new Parameter Context
3. **Name:** `AML-Environment`
4. Under the **Parameters** tab, add each of the following:

| Parameter Name | Example Value (Internal) | Description |
|---|---|---|
| `kafka.brokers` | `ccycloud-1.jshin.root.comops.site:9093,...` | Kafka broker addresses |
| `kafka.topic.txn` | `sbi-aml-transactions` | Transaction Kafka topic |
| `kerberos.keytab` | `/opt/cloudera/systest.keytab` | Kerberos keytab path |
| `kerberos.principal` | `systest@ROOT.COMOPS.SITE` | Kerberos principal |
| `ssl.truststore.path` | `/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks` | TLS truststore path |
| `ssl.truststore.password` | *(truststore password)* | Check "Sensitive" |
| `data.input.dir` | `/tmp/aml-data` | Data file directory |

5. Click **Apply**

---

## Step 2: Configure Controller Services

> Controller Services are shared connection settings used by multiple processors.  
> Configure SSL and Kerberos once, then reuse across all processors.

### 2-1. Create Process Group

1. Right-click on NiFi Canvas → **Add Process Group**
2. Name: `AML-Ingest-Flow`
3. Parameter Context: Select `AML-Environment`
4. Click **Add**

### 2-2. Enter Process Group and Configure Controller Services

1. Double-click `AML-Ingest-Flow` to enter
2. Click the **Configure** icon in the top menu
3. Select the **Controller Services** tab

**Controller Service 1: StandardSSLContextService**

| Property | Value |
|---|---|
| Truststore Filename | `#{ssl.truststore.path}` |
| Truststore Password | `#{ssl.truststore.password}` |
| Truststore Type | `JKS` |

→ Click **Enable** (lightning bolt icon)

**Controller Service 2: KerberosUserService**

> NiFi 2.x uses `KerberosUserService` instead of `KerberosCredentialsService`.

| Property | Value |
|---|---|
| Kerberos Keytab | `#{kerberos.keytab}` |
| Kerberos Principal | `#{kerberos.principal}` |

→ Click **Enable**

**Controller Service 3: JsonRecordSetWriter**

| Property | Value |
|---|---|
| Schema Access Strategy | `Infer Schema` |

→ Click **Enable**

---

## Step 3: Add Processors

Add each processor by dragging onto the Canvas.

### Processor 1: GetFile

**How to add:** Right-click Canvas → Add Processor → Search `GetFile` → Add

| Property | Value | Description |
|---|---|---|
| Input Directory | `#{data.input.dir}` | Data file directory |
| File Filter | `.*\.jsonl` | Read only JSONL files |
| Polling Interval | `5 sec` | Check for new files every 5 seconds |
| Keep Source File | `false` | Delete file after processing |

### Processor 2: SplitText

> **Key processor:** Splits the JSONL file line-by-line.  
> One line (1 transaction) = 1 FlowFile = 1 Kafka message → SSB parses correctly.

| Property | Value | Description |
|---|---|---|
| Line Split Count | `1` | 1 line = 1 FlowFile |
| Header Line Count | `0` | No header |
| Remove Trailing Newlines | `true` | Strip newline characters |

### Processor 3: UpdateAttribute

| Property | Value | Description |
|---|---|---|
| `mime.type` | `application/json` | Set Content-Type |

### Processor 4: PublishKafka2CDP

> `PublishKafka2CDP` is a Cloudera-exclusive Kafka processor in CFM 4.x.  
> Works with NiFi 2.x's `KerberosUserService`.

| Property | Value | Description |
|---|---|---|
| Kafka Brokers | `#{kafka.brokers}` | Broker addresses |
| Topic Name | `#{kafka.topic.txn}` | Topic name |
| SSL Context Service | `StandardSSLContextService` | TLS settings |
| Kerberos User Service | `KerberosUserService` | Kerberos auth |
| Record Writer | `JsonRecordSetWriter` | JSON output format |
| Delivery Guarantee | `Best Effort` | Performance-first for demo |

### Processor 5: LogMessage

| Property | Value |
|---|---|
| Log Level | `info` |
| Log Prefix | `[AML-SENT]` |
| Log payload | `true` |

---

## Step 4: Connect Processors

Drag between processors to create connections.

```
GetFile ──[success]──► SplitText ──[splits]──► UpdateAttribute ──[success]──► PublishKafka2CDP ──[success]──► LogMessage
                           │                                                           │
                       [original]                                                 [failure]
                           │                                                           │
                       (terminate)                                                (terminate)
```

**How to connect:**
1. GetFile → SplitText: Select relationship `success`
2. SplitText → UpdateAttribute: Select relationship `splits`
3. SplitText `original`: Select **Terminate** (ends reference to original file)
4. UpdateAttribute → PublishKafka2CDP: Select relationship `success`
5. PublishKafka2CDP → LogMessage: Select relationship `success`
6. PublishKafka2CDP `failure`: Select **Terminate**

---

## Step 5: Start Flow

1. Click empty Canvas area → **Ctrl+A** (select all)
2. Right-click → **Start**
3. Verify all processors are green (running)

**Verify:**
- SplitText: Confirm In/Out counters show 1 file → N records
- PublishKafka2CDP: Confirm send success counter increasing
- SMM UI: Confirm message count in `sbi-aml-transactions` topic increasing

---

## Flow Export (for Environment Switching Backup)

Saving the current Flow as JSON allows immediate import in the customer environment.

1. Right-click `AML-Ingest-Flow` Process Group → **Download Flow Definition**
2. Save as `aml_ingest_flow.json`
3. Keep this file in the `nifi/` folder

**Import in customer environment:**
1. Right-click NiFi Canvas → **Upload Flow Definition**
2. Upload the saved JSON file
3. Update only the Parameter Context values for the customer environment

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `GetFile` not reading files | Directory permission issue | `chmod 755 /tmp/aml-data` |
| SSB Kafka message parse failure | SplitText not configured → entire file sent as 1 message | Add SplitText, set Line Split Count=1 |
| `PublishKafka2CDP` red warning | Kerberos auth failed | Check `KerberosUserService` status |
| SSL connection error | Wrong truststore path or password | Re-check `StandardSSLContextService` |
| `KerberosUserService` activation failed | Keytab file missing | `ls -la /opt/cloudera/systest.keytab` |
