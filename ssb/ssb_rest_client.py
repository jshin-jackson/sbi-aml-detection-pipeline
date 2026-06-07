#!/usr/bin/env python3
"""
ssb_rest_client.py — SSB REST API Python 클라이언트 (Secondary 방식)

CSA 1.9.0.1에서 PyFlink 대신 사용합니다.
SQL 내용은 SSB Web UI 방식과 완전히 동일합니다.

사용법:
  # 환경 설정 후 실행
  source config/env.conf
  kinit -kt /opt/cloudera/systest.keytab systest@ROOT.COMOPS.SITE
  python ssb/ssb_rest_client.py

  # 특정 Job만 실행
  python ssb/ssb_rest_client.py --job large_cash
  python ssb/ssb_rest_client.py --job smurfing

  # 실행 중인 Job 목록 확인
  python ssb/ssb_rest_client.py --list

  # Job 중지
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
# 환경 변수 (source config/env.conf 후 os.environ에서 읽음)
# python-dotenv 미사용 — generate_aml_data.py / kafka_producer.py와 동일 방식
# ----------------------------------------------------------------
SSB_HOST     = os.environ.get("SSB_HOST",     "https://localhost:18121")
SSB_USER     = os.environ.get("SSB_USER",     "systest")
SSB_PASSWORD = os.environ.get("SSB_PASSWORD", "")
CA_PEM       = os.environ.get("CA_PEM",
               "/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem")
KEYTAB       = os.environ.get("KEYTAB",   "/opt/cloudera/systest.keytab")
PRINCIPAL    = os.environ.get("PRINCIPAL", "systest@ROOT.COMOPS.SITE")


def kinit():
    """Kerberos TGT 발급"""
    result = subprocess.run(
        ["kinit", "-kt", KEYTAB, PRINCIPAL],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[ERROR] kinit 실패: {result.stderr}")
        sys.exit(1)
    print(f"[Kerberos] 인증 완료: {PRINCIPAL}")


def render_sql(tpl_path: Path) -> str:
    """SQL 템플릿의 환경 변수를 실제 값으로 치환"""
    env = os.environ.copy()
    # TRUSTSTORE_PW는 파일에서 읽어 환경 변수로 추가
    pw_file = env.get("TRUSTSTORE_PW_FILE", "")
    if pw_file and Path(pw_file).exists():
        env["TRUSTSTORE_PW"] = Path(pw_file).read_text().strip()

    result = subprocess.run(
        ["envsubst"],
        input=tpl_path.read_text(encoding="utf-8"),
        capture_output=True,
        text=True,
        env=env,
    )
    return result.stdout


class SSBClient:
    """Cloudera SQL Stream Builder REST API 클라이언트"""

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
        """실행 중인 Job 목록 조회"""
        return self._request("GET", "/jobs")

    def submit_job(self, name: str, sql: str, parallelism: int = 2) -> dict:
        """SQL Job 제출 및 실행"""
        payload = {
            "name":        name,
            "sql":         sql,
            "parallelism": parallelism,
        }
        return self._request("POST", "/jobs", json=payload)

    def stop_job(self, job_name: str) -> dict:
        """Job 중지"""
        jobs = self.list_jobs()
        target = next((j for j in jobs if j.get("name") == job_name), None)
        if not target:
            print(f"[WARN] Job을 찾을 수 없습니다: {job_name}")
            return {}
        job_id = target["id"]
        return self._request("DELETE", f"/jobs/{job_id}")


def submit_all(client: SSBClient, tpl_dir: Path):
    """3개 SQL Job 모두 제출"""
    jobs = [
        ("aml_kafka_source",  tpl_dir / "01_kafka_source_table.sql.tpl", 1),
        ("aml_large_cash",    tpl_dir / "02_large_cash_job.sql.tpl",     2),
        ("aml_smurfing",      tpl_dir / "03_smurfing_job.sql.tpl",       2),
    ]

    for name, tpl_path, parallelism in jobs:
        if not tpl_path.exists():
            print(f"[ERROR] 템플릿 파일 없음: {tpl_path}")
            continue

        print(f"\n[Submit] {name} ...")
        sql = render_sql(tpl_path)
        result = client.submit_job(name, sql, parallelism)
        print(f"  Job ID   : {result.get('id', 'N/A')}")
        print(f"  Status   : {result.get('status', 'N/A')}")


def main():
    parser = argparse.ArgumentParser(
        description="SSB REST API Python 클라이언트 — AML Job 관리",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--job",  choices=["large_cash", "smurfing", "all"],
                        default="all", help="실행할 Job (기본: all)")
    parser.add_argument("--list", action="store_true", help="실행 중인 Job 목록 출력")
    parser.add_argument("--stop", type=str, metavar="JOB_NAME", help="Job 중지")
    args = parser.parse_args()

    tpl_dir = Path(__file__).parent

    print(f"\n[SSB REST Client] ENV={os.getenv('ENV_NAME', 'unknown')}")
    print(f"  SSB Host : {SSB_HOST}")
    print(f"  SSB User : {SSB_USER}")

    # Kerberos 인증
    kinit()

    client = SSBClient()

    if args.list:
        print("\n[실행 중인 Jobs]")
        jobs = client.list_jobs()
        if not jobs:
            print("  (실행 중인 Job 없음)")
        else:
            for job in jobs:
                print(f"  - {job.get('name')} [{job.get('status')}] id={job.get('id')}")
        return

    if args.stop:
        print(f"\n[Job 중지] {args.stop}")
        result = client.stop_job(args.stop)
        print(f"  결과: {result}")
        return

    # Job 제출
    print(f"\n================================================================")
    print(f" SSB Job 제출 시작 (--job={args.job})")
    print(f"================================================================")

    if args.job == "all":
        submit_all(client, tpl_dir)
    elif args.job == "large_cash":
        sql = render_sql(tpl_dir / "02_large_cash_job.sql.tpl")
        result = client.submit_job("aml_large_cash", sql, 2)
        print(f"  제출 완료: {result}")
    elif args.job == "smurfing":
        sql = render_sql(tpl_dir / "03_smurfing_job.sql.tpl")
        result = client.submit_job("aml_smurfing", sql, 2)
        print(f"  제출 완료: {result}")

    print(f"\n[완료] Job 제출 완료!")
    print(f"  실행 확인: {SSB_HOST} (SSB Web UI)")
    print(f"  또는: python ssb/ssb_rest_client.py --list")


if __name__ == "__main__":
    main()
