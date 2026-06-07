"""
kafka_producer.py — AML 거래 데이터를 Kafka로 직접 전송 (NiFi 없이)

sibling 프로젝트(sbi-realtime-fraud-detection)의 kafka_producer.py 패턴을
AML 프로젝트에 맞게 적용합니다.

보안:
  - SASL_SSL + GSSAPI (Kerberos)
  - kafka-python 사용 (순수 Python — air-gapped 환경 호환)
  - kinit으로 OS TGT 획득 → kafka-python이 GSSAPI로 참조

사전 조건:
    source config/env.conf    # 환경 변수 로드
    # venv 활성화
    source /tmp/aml-venv/bin/activate

사용법:
    # DATA_OUTPUT_DIR의 최신 파일 자동 선택하여 전송
    python data_gen/kafka_producer.py

    # 특정 파일 전송
    python data_gen/kafka_producer.py --input /tmp/aml-data/aml_transactions_20260607.jsonl

    # 인라인 생성 후 바로 전송 (SDV 학습 포함)
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
# 환경 변수 (source config/env.conf 후 os.environ에서 읽음)
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
# Kerberos 인증 (sibling 패턴 그대로)
# ---------------------------------------------------------------------------

def kinit() -> None:
    """
    Kerberos TGT를 keytab으로 갱신합니다.
    kafka-python은 OS 수준 Kerberos 티켓 캐시(GSSAPI)를 사용하므로
    Producer 생성 전 kinit이 반드시 필요합니다.
    """
    if not os.path.exists(KEYTAB):
        print(f"[경고] keytab 파일 없음({KEYTAB}), kinit 생략합니다.", file=sys.stderr)
        return
    try:
        subprocess.run(
            ["kinit", "-kt", KEYTAB, PRINCIPAL],
            check=True,
            capture_output=True,
        )
        print(f"[Kerberos] kinit 성공: {PRINCIPAL}")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"[경고] kinit 실패 (기존 티켓 사용 시도): {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Kafka Producer 생성 (sibling 패턴 그대로)
# ---------------------------------------------------------------------------

def build_producer() -> KafkaProducer:
    """
    SASL_SSL + GSSAPI KafkaProducer 생성
    SSL  : ssl.create_default_context() + CA PEM (Auto-TLS 환경)
    Kerberos: kinit으로 획득한 OS TGT를 GSSAPI가 참조
    """
    kinit()

    ssl_context = ssl.create_default_context()
    if os.path.exists(CA_PEM):
        ssl_context.load_verify_locations(cafile=CA_PEM)
        print(f"[SSL] CA 인증서 로드: {CA_PEM}")
    else:
        print(f"[경고] CA PEM 없음({CA_PEM}), SSL 검증 비활성화", file=sys.stderr)
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
# 전송
# ---------------------------------------------------------------------------

def produce_from_jsonl(file_path: str, rate: int) -> int:
    """JSONL 파일을 한 줄씩 읽어 Kafka로 전송"""
    producer = build_producer()
    sleep_interval = 1.0 / rate if rate > 0 else 0
    sent = 0

    print(f"\n[Kafka 전송]")
    print(f"  Broker : {KAFKA_BROKERS}")
    print(f"  Topic  : {KAFKA_TOPIC_TXN}")
    print(f"  File   : {file_path}")
    print(f"  Rate   : {rate}건/초")
    print()

    def _on_error(e: KafkaError) -> None:
        print(f"[오류] 전송 실패: {e}", file=sys.stderr)

    try:
        with open(file_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"[경고] JSON 파싱 실패, 건너뜀: {e}", file=sys.stderr)
                    continue

                key   = str(record.get("account_id", "unknown")).encode("utf-8")
                value = json.dumps(record, ensure_ascii=False,
                                   default=str).encode("utf-8")

                producer.send(KAFKA_TOPIC_TXN, key=key,
                              value=value).add_errback(_on_error)
                sent += 1

                if sent % 500 == 0:
                    print(f"  전송: {sent}건...")

                if sleep_interval > 0:
                    time.sleep(sleep_interval)

        producer.flush(timeout=30)
        print(f"\n  전송 완료: {sent}건 → {KAFKA_TOPIC_TXN}")

    except KafkaError as e:
        print(f"[오류] Kafka 전송 중 오류: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n  중단됨. {sent}건 전송 완료.")
        producer.flush(timeout=10)
    finally:
        producer.close()

    return sent


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SBI AML 거래 데이터 Kafka Producer")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--input", type=str,
                        help="전송할 JSONL 파일 경로")
    source.add_argument("--rows",  type=int,
                        help="인라인 SDV 생성 후 전송할 건수")

    parser.add_argument("--rate", type=int, default=DEMO_RATE,
                        help=f"초당 전송 건수 (기본: {DEMO_RATE}, 0=최대 속도)")
    args = parser.parse_args()

    print(f"[Kafka Producer] ENV={os.environ.get('ENV_NAME', 'unknown')}")

    if args.rows:
        # 인라인 생성 후 전송
        sys.path.insert(0, str(Path(__file__).parent))
        from generate_aml_data import (
            generate_normal_transactions,
            inject_large_cash,
            inject_smurfing,
            save_as_jsonl,
        )

        print(f"\n[인라인 생성] {args.rows}건 생성 후 전송")
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
        # DATA_OUTPUT_DIR에서 가장 최신 JSONL 파일 자동 선택
        pattern = str(Path(DATA_OUTPUT_DIR) / "*.jsonl")
        files = sorted(glob.glob(pattern), reverse=True)
        if not files:
            print(f"[오류] {DATA_OUTPUT_DIR}에 .jsonl 파일이 없습니다.")
            print(f"       먼저 실행: python data_gen/generate_aml_data.py")
            sys.exit(1)
        latest = files[0]
        print(f"\n[최신 파일 자동 선택] {latest}")
        produce_from_jsonl(latest, args.rate)


if __name__ == "__main__":
    main()
