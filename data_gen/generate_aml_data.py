"""
generate_aml_data.py — SBI AML 탐지 데모용 합성 거래 데이터 생성

sibling 프로젝트(sbi-realtime-fraud-detection)의 SDV 패턴을 기반으로
AML 특화 패턴(Large Cash / Smurfing)을 추가합니다.

사전 조건:
    source config/env.conf    # 환경 변수 로드

사용법:
    python data_gen/generate_aml_data.py
    python data_gen/generate_aml_data.py --rows 5000 --output /tmp/aml-data
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer


# ---------------------------------------------------------------------------
# 환경 변수 (source config/env.conf 후 os.environ에서 읽음)
# ---------------------------------------------------------------------------
LARGE_CASH_THRESHOLD = int(os.environ.get("LARGE_CASH_THRESHOLD", "1000000"))
SMURFING_TXN_COUNT   = int(os.environ.get("SMURFING_TXN_COUNT", "5"))
SMURFING_WINDOW_MIN  = int(os.environ.get("SMURFING_WINDOW_MIN", "30"))
DEMO_ROWS            = int(os.environ.get("DEMO_ROWS", "2000"))
DATA_OUTPUT_DIR      = os.environ.get("DATA_OUTPUT_DIR", "/tmp/aml-data")
LARGE_CASH_ACCOUNTS  = os.environ.get(
    "LARGE_CASH_ACCOUNTS", "ACC_AML_LARGE_001,ACC_AML_LARGE_002"
).split(",")
SMURFING_ACCOUNTS    = os.environ.get(
    "SMURFING_ACCOUNTS", "ACC_AML_SMURF_001,ACC_AML_SMURF_002,ACC_AML_SMURF_003"
).split(",")

# ---------------------------------------------------------------------------
# 상수 (sibling 프로젝트와 동일)
# ---------------------------------------------------------------------------
CHANNELS = ["ATM", "NEFT", "IMPS", "UPI", "RTGS", "BRANCH"]

MERCHANT_CATS = [
    "GROCERY", "FUEL", "RESTAURANT", "TRAVEL", "ELECTRONICS",
    "PHARMACY", "JEWELLERY", "TRANSFER", "ATM_WITHDRAWAL", "ECOMMERCE",
]

# 인도 주요 도시 좌표 (sibling 프로젝트와 동일)
CITY_COORDS = [
    (28.6139, 77.2090),  # New Delhi
    (19.0760, 72.8777),  # Mumbai
    (12.9716, 77.5946),  # Bangalore
    (22.5726, 88.3639),  # Kolkata
    (13.0827, 80.2707),  # Chennai
    (17.3850, 78.4867),  # Hyderabad
    (23.0225, 72.5714),  # Ahmedabad
    (18.5204, 73.8567),  # Pune
]


# ---------------------------------------------------------------------------
# SDV 기반 정상 거래 생성 (sibling 패턴 그대로)
# ---------------------------------------------------------------------------

def build_seed_dataframe(n: int = 2000) -> pd.DataFrame:
    """SDV 학습용 시드 데이터프레임 생성"""
    random.seed(42)
    np.random.seed(42)

    base_time = datetime(2024, 1, 1)
    records = []

    for _ in range(n):
        city = random.choice(CITY_COORDS)
        amount = round(random.uniform(100, 500000), 2)
        offset_seconds = random.randint(0, 60 * 24 * 365)
        ts = base_time + timedelta(seconds=offset_seconds)

        records.append({
            "account_id":   f"ACC{random.randint(10000, 99999)}",
            "timestamp":    ts.isoformat(),
            "amount":       amount,
            "merchant_id":  f"MER{random.randint(1000, 9999)}",
            "merchant_cat": random.choice(MERCHANT_CATS),
            "location_lat": round(city[0] + np.random.uniform(-0.5, 0.5), 6),
            "location_lon": round(city[1] + np.random.uniform(-0.5, 0.5), 6),
            "channel":      random.choice(CHANNELS),
        })

    return pd.DataFrame(records)


def train_synthesizer(seed_df: pd.DataFrame) -> GaussianCopulaSynthesizer:
    """SDV GaussianCopula 합성기 학습 (sibling 패턴 그대로)"""
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(seed_df)

    metadata.update_column("account_id",   sdtype="id")
    metadata.update_column("merchant_id",  sdtype="id")
    metadata.update_column("timestamp",    sdtype="datetime",
                           datetime_format="%Y-%m-%dT%H:%M:%S")
    metadata.update_column("channel",      sdtype="categorical")
    metadata.update_column("merchant_cat", sdtype="categorical")

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(seed_df)
    return synthesizer


def generate_normal_transactions(n_rows: int) -> pd.DataFrame:
    """SDV로 정상 거래 생성"""
    print("[1/3] 시드 데이터 2000건 생성 중...")
    seed_df = build_seed_dataframe(2000)

    print("[2/3] SDV 합성기 학습 중...")
    synthesizer = train_synthesizer(seed_df)

    print(f"[3/3] 합성 데이터 {n_rows}건 생성 중...")
    df = synthesizer.sample(num_rows=n_rows)
    df.insert(0, "transaction_id", [str(uuid.uuid4()) for _ in range(len(df))])
    return df


# ---------------------------------------------------------------------------
# AML 패턴 주입
# ---------------------------------------------------------------------------

def inject_large_cash(df: pd.DataFrame) -> pd.DataFrame:
    """
    Large Cash 패턴 주입:
    지정 계좌에서 임계값(LARGE_CASH_THRESHOLD) 이상 거래 2~3건 생성
    """
    rows = []
    for account_id in LARGE_CASH_ACCOUNTS:
        for _ in range(random.randint(2, 3)):
            city = random.choice(CITY_COORDS)
            amount = round(
                random.uniform(LARGE_CASH_THRESHOLD, LARGE_CASH_THRESHOLD * 5), 2
            )
            ts = datetime.utcnow() - timedelta(minutes=random.randint(1, 30))
            rows.append({
                "transaction_id": str(uuid.uuid4()),
                "account_id":     account_id,
                "timestamp":      ts.isoformat(),
                "amount":         amount,
                "merchant_id":    "MER_AML_LC",
                "merchant_cat":   "BRANCH",
                "location_lat":   round(city[0], 6),
                "location_lon":   round(city[1], 6),
                "channel":        "BRANCH",
            })

    lc_df = pd.DataFrame(rows)
    print(f"  Large Cash 주입: {len(lc_df)}건 "
          f"(계좌: {LARGE_CASH_ACCOUNTS}, 기준: ₹{LARGE_CASH_THRESHOLD:,})")
    return pd.concat([df, lc_df], ignore_index=True)


def inject_smurfing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Smurfing 패턴 주입:
    지정 계좌에서 SMURFING_WINDOW_MIN분 내 SMURFING_TXN_COUNT회 이상 거래
    각 건은 임계값의 50~90% 사이 (의도적으로 임계값 회피)
    """
    max_single = int(LARGE_CASH_THRESHOLD * 0.9)
    min_single = int(LARGE_CASH_THRESHOLD * 0.5)

    rows = []
    for account_id in SMURFING_ACCOUNTS:
        base_time = datetime.utcnow() - timedelta(minutes=random.randint(5, 60))
        count = random.randint(SMURFING_TXN_COUNT, SMURFING_TXN_COUNT + 3)
        interval = SMURFING_WINDOW_MIN / count

        for i in range(count):
            city = random.choice(CITY_COORDS)
            txn_time = base_time + timedelta(minutes=i * interval)
            amount = round(random.uniform(min_single, max_single), 2)
            rows.append({
                "transaction_id": str(uuid.uuid4()),
                "account_id":     account_id,
                "timestamp":      txn_time.isoformat(),
                "amount":         amount,
                "merchant_id":    f"MER_AML_SM_{i}",
                "merchant_cat":   "ATM_WITHDRAWAL",
                "location_lat":   round(city[0], 6),
                "location_lon":   round(city[1], 6),
                "channel":        "ATM",
            })

    sm_df = pd.DataFrame(rows)
    print(f"  Smurfing 주입: {len(sm_df)}건 "
          f"(계좌: {SMURFING_ACCOUNTS}, {SMURFING_WINDOW_MIN}분/{SMURFING_TXN_COUNT}회 기준)")
    return pd.concat([df, sm_df], ignore_index=True)


# ---------------------------------------------------------------------------
# 저장 (JSONL — NiFi GetFile이 읽는 형식)
# ---------------------------------------------------------------------------

def save_as_jsonl(df: pd.DataFrame, output_path: Path) -> None:
    """
    JSON Lines 형식으로 저장.
    timestamp → txn_time(epoch ms) 변환: SSB Flink DDL의 TO_TIMESTAMP_LTZ(txn_time, 3) 사용
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            ts_val = record.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(str(ts_val)) if isinstance(ts_val, str) else ts_val
                record["txn_time"] = int(dt.timestamp() * 1000)
            except Exception:
                record["txn_time"] = int(datetime.utcnow().timestamp() * 1000)
            record.pop("timestamp", None)  # timestamp 제거, txn_time으로 통일
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SBI AML 데모 데이터 생성")
    parser.add_argument("--rows",   type=int, default=DEMO_ROWS,
                        help=f"정상 거래 건수 (기본: {DEMO_ROWS})")
    parser.add_argument("--output", type=str, default=DATA_OUTPUT_DIR,
                        help=f"출력 디렉토리 (기본: {DATA_OUTPUT_DIR})")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[AML 데이터 생성] ENV={os.environ.get('ENV_NAME', 'unknown')}")
    print(f"  출력 디렉토리  : {output_dir}")
    print(f"  정상 거래      : {args.rows}건")
    print(f"  Large Cash 기준: ₹{LARGE_CASH_THRESHOLD:,}")
    print(f"  Smurfing 기준  : {SMURFING_WINDOW_MIN}분 내 {SMURFING_TXN_COUNT}회")
    print()

    # 1. SDV 정상 거래 생성
    df = generate_normal_transactions(args.rows)

    # 2. AML 패턴 주입
    print("\n[AML 패턴 주입]")
    df = inject_large_cash(df)
    df = inject_smurfing(df)

    # 3. 셔플 (패턴 노출 방지)
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # 4. JSONL 저장
    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"aml_transactions_{timestamp_str}.jsonl"
    save_as_jsonl(df, output_file)

    lc_count = df[df["account_id"].isin(LARGE_CASH_ACCOUNTS)].shape[0]
    sm_count = df[df["account_id"].isin(SMURFING_ACCOUNTS)].shape[0]

    print(f"\n[완료] 총 {len(df)}건 저장 → {output_file}")
    print(f"  정상 거래   : {len(df) - lc_count - sm_count}건")
    print(f"  Large Cash  : {lc_count}건")
    print(f"  Smurfing    : {sm_count}건")
    print(f"\n다음 단계:")
    print(f"  [NiFi 방식] NiFi GetFile이 {output_dir} 를 자동으로 읽어 Kafka 전송")
    print(f"  [직접 방식] python data_gen/kafka_producer.py")


if __name__ == "__main__":
    main()
