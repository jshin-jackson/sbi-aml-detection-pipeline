"""
kafka_producer.py — Send AML transaction data directly to Kafka (without NiFi)

Security:
  - SASL_SSL + GSSAPI (Kerberos)
  - Uses kafka-python (pure Python — air-gapped compatible)
  - kinit acquires OS TGT → kafka-python uses it via GSSAPI

Prerequisites:
    source config/env.conf    # Load environment variables
    source /tmp/aml-venv/bin/activate

Usage:
    # Auto-select the most recent JSONL file from DATA_OUTPUT_DIR
    python data_gen/kafka_producer.py

    # Send a specific file
    python data_gen/kafka_producer.py --input /tmp/aml-data/aml_transactions_20260607.jsonl

    # Generate inline and send immediately (includes SDV training)
    python data_gen/kafka_producer.py --rows 2000 --rate 10
"""

import argparse
import glob
import json
import os
import ssl
import sys
import subprocess
import tempfile
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ---------------------------------------------------------------------------
# Environment variables (loaded via: source config/env.conf)
# ---------------------------------------------------------------------------
KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS",   "localhost:9093")
KAFKA_TOPIC_TXN = os.environ.get("KAFKA_TOPIC_TXN", "sbi-aml-transactions")
KEYTAB          = os.environ.get("KEYTAB",           "/opt/cloudera/systest.keytab")
PRINCIPAL       = os.environ.get("PRINCIPAL",        "systest@ROOT.COMOPS.SITE")
CA_PEM          = os.environ.get("CA_PEM",
                  "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem")
DATA_OUTPUT_DIR = os.environ.get("DATA_OUTPUT_DIR",  "/tmp/aml-data")
DEMO_RATE       = int(os.environ.get("DEMO_RATE", "5"))


# ---------------------------------------------------------------------------
# Kerberos authentication (same as sibling project)
# ---------------------------------------------------------------------------

def kinit() -> None:
    """
    Acquire Kerberos TGT using keytab.
    kafka-python uses the OS-level Kerberos ticket cache (GSSAPI),
    so kinit must be called before creating the producer.
    """
    if not os.path.exists(KEYTAB):
        print(f"[WARNING] Keytab not found ({KEYTAB}), skipping kinit.", file=sys.stderr)
        return
    try:
        subprocess.run(
            ["kinit", "-kt", KEYTAB, PRINCIPAL],
            check=True,
            capture_output=True,
        )
        print(f"[Kerberos] kinit succeeded: {PRINCIPAL}")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"[WARNING] kinit failed (attempting with existing ticket): {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Kafka Producer (same as sibling project)
# ---------------------------------------------------------------------------

def build_producer() -> KafkaProducer:
    """
    Create SASL_SSL + GSSAPI KafkaProducer.
    SSL  : ssl.create_default_context() + CA PEM (Auto-TLS environment)
    Kerberos: Uses TGT acquired by kinit via GSSAPI
    """
    kinit()

    ssl_context = ssl.create_default_context()
    if os.path.exists(CA_PEM):
        ssl_context.load_verify_locations(cafile=CA_PEM)
        print(f"[SSL] CA certificate loaded: {CA_PEM}")
    else:
        print(f"[WARNING] CA PEM not found ({CA_PEM}), disabling SSL verification", file=sys.stderr)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS.split(","),
        security_protocol="SASL_SSL",
        sasl_mechanism="GSSAPI",
        sasl_kerberos_service_name="kafka",
        ssl_context=ssl_context,
        client_id="sbi-aml-producer",
        acks="all",
        retries=3,
        linger_ms=10,
        batch_size=65536,
        compression_type="snappy",
    )


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------

def produce_from_jsonl(file_path: str, rate: int) -> int:
    """Read JSONL file line-by-line and send each line as a Kafka message."""
    producer = build_producer()
    sleep_interval = 1.0 / rate if rate > 0 else 0
    sent = 0

    print(f"\n[Kafka Send]")
    print(f"  Broker : {KAFKA_BROKERS}")
    print(f"  Topic  : {KAFKA_TOPIC_TXN}")
    print(f"  File   : {file_path}")
    print(f"  Rate   : {rate} msg/sec")
    print()

    def _on_error(e: KafkaError) -> None:
        print(f"[ERROR] Send failed: {e}", file=sys.stderr)

    try:
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"[WARNING] JSON parse failed, skipping: {e}", file=sys.stderr)
                    continue

                key   = str(record.get("account_id", "unknown")).encode("utf-8")
                value = json.dumps(record, ensure_ascii=False,
                                   default=str).encode("utf-8")

                producer.send(KAFKA_TOPIC_TXN, key=key,
                              value=value).add_errback(_on_error)
                sent += 1

                if sent % 500 == 0:
                    print(f"  Sent: {sent} records...")

                if sleep_interval > 0:
                    time.sleep(sleep_interval)

        producer.flush(timeout=30)
        print(f"\n  Done: {sent} records → {KAFKA_TOPIC_TXN}")

    except KafkaError as e:
        print(f"[ERROR] Kafka send error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n  Interrupted. {sent} records sent.")
        producer.flush(timeout=10)
    finally:
        producer.close()

    return sent


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SBI AML Transaction Data Kafka Producer")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--input", type=str,
                        help="Path to JSONL file to send")
    source.add_argument("--rows",  type=int,
                        help="Generate inline and send (includes SDV training)")

    parser.add_argument("--rate", type=int, default=DEMO_RATE,
                        help=f"Messages per second (default: {DEMO_RATE}, 0=max speed)")
    args = parser.parse_args()

    print(f"[Kafka Producer] ENV={os.environ.get('ENV_NAME', 'unknown')}")

    if args.rows:
        # Inline generation + send
        sys.path.insert(0, str(Path(__file__).parent))
        from generate_aml_data import (
            generate_normal_transactions,
            inject_large_cash,
            inject_smurfing,
            save_as_jsonl,
        )

        print(f"\n[Inline Generation] Generating {args.rows} records and sending")
        df = generate_normal_transactions(args.rows)
        df = inject_large_cash(df)
        df = inject_smurfing(df)
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)

        tmp_file = Path(tempfile.mktemp(suffix=".jsonl"))
        save_as_jsonl(df, tmp_file)
        produce_from_jsonl(str(tmp_file), args.rate)
        tmp_file.unlink(missing_ok=True)

    elif args.input:
        produce_from_jsonl(args.input, args.rate)

    else:
        # Auto-select most recent JSONL file from DATA_OUTPUT_DIR
        pattern = str(Path(DATA_OUTPUT_DIR) / "*.jsonl")
        files = sorted(glob.glob(pattern), reverse=True)
        if not files:
            print(f"[ERROR] No .jsonl files found in {DATA_OUTPUT_DIR}.")
            print(f"        Run first: python data_gen/generate_aml_data.py")
            sys.exit(1)
        latest = files[0]
        print(f"\n[Auto-selected most recent file] {latest}")
        produce_from_jsonl(latest, args.rate)


if __name__ == "__main__":
    main()
