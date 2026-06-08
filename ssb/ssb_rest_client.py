#!/usr/bin/env python3
"""
ssb_rest_client.py — SSB REST API Python Client (Secondary method)

Used instead of PyFlink on CSA 1.9.0.1.
The SQL logic is identical to the SSB Web UI method.

Usage:
  # Load env, authenticate, then run
  source config/env.conf
  kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
  python ssb/ssb_rest_client.py

  # Submit a specific job only
  python ssb/ssb_rest_client.py --job large_cash
  python ssb/ssb_rest_client.py --job smurfing

  # List running jobs
  python ssb/ssb_rest_client.py --list

  # Stop a job
  python ssb/ssb_rest_client.py --stop aml_large_cash
"""

import os
import sys
import json
import subprocess
import argparse
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

# ----------------------------------------------------------------
# Environment variables (loaded via: source config/env.conf)
# No python-dotenv used — consistent with generate_aml_data.py / kafka_producer.py
# ----------------------------------------------------------------
SSB_HOST     = os.environ.get("SSB_HOST",     "https://localhost:18121")
SSB_USER     = os.environ.get("SSB_USER",     "systest")
SSB_PASSWORD = os.environ.get("SSB_PASSWORD", "")
CA_PEM       = os.environ.get("CA_PEM",
               "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem")
KEYTAB       = os.environ.get("KEYTAB",   "/opt/cloudera/systest.keytab")
PRINCIPAL    = os.environ.get("PRINCIPAL", "systest@ROOT.COMOPS.SITE")


def kinit():
    """Acquire Kerberos TGT."""
    result = subprocess.run(
        ["kinit", "-kt", KEYTAB, PRINCIPAL],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[ERROR] kinit failed: {result.stderr}")
        sys.exit(1)
    print(f"[Kerberos] Authentication succeeded: {PRINCIPAL}")


def render_sql(tpl_path: Path) -> str:
    """Render SQL template by substituting environment variables (pure Python — no envsubst)."""
    text = tpl_path.read_text(encoding="utf-8")
    vars_to_replace = [
        "KAFKA_BROKERS", "KAFKA_TOPIC_TXN",
        "INCLUSTER_TRUSTSTORE_JKS", "TRUSTSTORE_PW",
        "LARGE_CASH_THRESHOLD", "SMURFING_WINDOW_MIN", "SMURFING_TXN_COUNT",
    ]
    for var in vars_to_replace:
        value = os.environ.get(var, "")
        text = text.replace(f"${{{var}}}", value)
    return text


class SSBClient:
    """Cloudera SQL Stream Builder REST API client."""

    def __init__(self):
        self.base_url = SSB_HOST.rstrip("/")
        self.session = requests.Session()
        self.session.verify = CA_PEM
        self.session.auth = HTTPBasicAuth(SSB_USER, SSB_PASSWORD)
        self.session.headers.update({"Content-Type": "application/json"})

    def _request(self, method: str, path: str, **kwargs) -> dict:
        url = f"{self.base_url}/api/v1{path}"
        resp = self.session.request(method, url, **kwargs)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            print(f"[ERROR] HTTP {resp.status_code}: {resp.text}")
            raise
        return resp.json() if resp.content else {}

    def list_jobs(self) -> list:
        """List running jobs."""
        return self._request("GET", "/jobs")

    def submit_job(self, name: str, sql: str, parallelism: int = 2) -> dict:
        """Submit and start a SQL job."""
        payload = {
            "name":        name,
            "sql":         sql,
            "parallelism": parallelism,
        }
        return self._request("POST", "/jobs", json=payload)

    def stop_job(self, job_name: str) -> dict:
        """Stop a job by name."""
        jobs = self.list_jobs()
        target = next((j for j in jobs if j.get("name") == job_name), None)
        if not target:
            print(f"[WARNING] Job not found: {job_name}")
            return {}
        job_id = target["id"]
        return self._request("DELETE", f"/jobs/{job_id}")


def submit_all(client: SSBClient, tpl_dir: Path):
    """Submit all 3 SQL jobs."""
    jobs = [
        ("aml_kafka_source",  tpl_dir / "01_kafka_source_table.sql.tpl", 1),
        ("aml_large_cash",    tpl_dir / "02_large_cash_job.sql.tpl",     2),
        ("aml_smurfing",      tpl_dir / "03_smurfing_job.sql.tpl",       2),
    ]

    for name, tpl_path, parallelism in jobs:
        if not tpl_path.exists():
            print(f"[ERROR] Template not found: {tpl_path}")
            continue

        print(f"\n[Submit] {name} ...")
        sql = render_sql(tpl_path)
        result = client.submit_job(name, sql, parallelism)
        print(f"  Job ID : {result.get('id', 'N/A')}")
        print(f"  Status : {result.get('status', 'N/A')}")


def main():
    parser = argparse.ArgumentParser(
        description="SSB REST API Python Client — AML Job Management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--job",  choices=["large_cash", "smurfing", "all"],
                        default="all", help="Job to run (default: all)")
    parser.add_argument("--list", action="store_true", help="List running jobs")
    parser.add_argument("--stop", type=str, metavar="JOB_NAME", help="Stop a job")
    args = parser.parse_args()

    tpl_dir = Path(__file__).parent

    print(f"\n[SSB REST Client] ENV={os.getenv('ENV_NAME', 'unknown')}")
    print(f"  SSB Host : {SSB_HOST}")
    print(f"  SSB User : {SSB_USER}")

    # Kerberos authentication
    kinit()

    client = SSBClient()

    if args.list:
        print("\n[Running Jobs]")
        jobs = client.list_jobs()
        if not jobs:
            print("  (no running jobs)")
        else:
            for job in jobs:
                print(f"  - {job.get('name')} [{job.get('status')}] id={job.get('id')}")
        return

    if args.stop:
        print(f"\n[Stop Job] {args.stop}")
        result = client.stop_job(args.stop)
        print(f"  Result: {result}")
        return

    # Submit jobs
    print(f"\n================================================================")
    print(f" Submitting SSB Jobs (--job={args.job})")
    print(f"================================================================")

    if args.job == "all":
        submit_all(client, tpl_dir)
    elif args.job == "large_cash":
        sql = render_sql(tpl_dir / "02_large_cash_job.sql.tpl")
        result = client.submit_job("aml_large_cash", sql, 2)
        print(f"  Submitted: {result}")
    elif args.job == "smurfing":
        sql = render_sql(tpl_dir / "03_smurfing_job.sql.tpl")
        result = client.submit_job("aml_smurfing", sql, 2)
        print(f"  Submitted: {result}")

    print(f"\n[DONE] Job submission complete!")
    print(f"  Verify in SSB Web UI: {SSB_HOST}")
    print(f"  Or run: python ssb/ssb_rest_client.py --list")


if __name__ == "__main__":
    main()
