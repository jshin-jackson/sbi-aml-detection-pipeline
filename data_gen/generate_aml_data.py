"""
generate_aml_data.py — Generate synthetic transaction data for SBI AML detection demo

Based on the sibling project (sbi-realtime-fraud-detection) SDV pattern,
with added AML-specific patterns (Large Cash / Smurfing).

Prerequisites:
    source config/env.conf    # Load environment variables

Usage:
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
# Environment variables (loaded via: source config/env.conf)
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
# Constants (same as sibling project)
# ---------------------------------------------------------------------------
CHANNELS = ["ATM", "NEFT", "IMPS", "UPI", "RTGS", "BRANCH"]

MERCHANT_CATS = [
    "GROCERY", "FUEL", "RESTAURANT", "TRAVEL", "ELECTRONICS",
    "PHARMACY", "JEWELLERY", "TRANSFER", "ATM_WITHDRAWAL", "ECOMMERCE",
]

# Major Indian city coordinates (same as sibling project)
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
# SDV-based normal transaction generation (same as sibling project)
# ---------------------------------------------------------------------------

def build_seed_dataframe(n: int = 2000) -> pd.DataFrame:
    """Build seed dataframe for SDV training."""
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
    """Train SDV GaussianCopula synthesizer (same as sibling project)."""
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
    """Generate normal transactions using SDV."""
    print("[1/3] Building seed dataset (2000 rows)...")
    seed_df = build_seed_dataframe(2000)

    print("[2/3] Training SDV synthesizer...")
    synthesizer = train_synthesizer(seed_df)

    print(f"[3/3] Generating {n_rows} synthetic transactions...")
    df = synthesizer.sample(num_rows=n_rows)
    df.insert(0, "transaction_id", [str(uuid.uuid4()) for _ in range(len(df))])
    return df


# ---------------------------------------------------------------------------
# AML pattern injection
# ---------------------------------------------------------------------------

def inject_large_cash(df: pd.DataFrame) -> pd.DataFrame:
    """
    Inject Large Cash pattern:
    Generate 2–3 transactions above LARGE_CASH_THRESHOLD for designated accounts.
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
    print(f"  Large Cash injected: {len(lc_df)} transactions "
          f"(accounts: {LARGE_CASH_ACCOUNTS}, threshold: ₹{LARGE_CASH_THRESHOLD:,})")
    return pd.concat([df, lc_df], ignore_index=True)


def inject_smurfing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Inject Smurfing pattern:
    Generate SMURFING_TXN_COUNT+ transactions within SMURFING_WINDOW_MIN minutes
    for designated accounts. Each transaction is below the threshold (intentional evasion).
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
    print(f"  Smurfing injected: {len(sm_df)} transactions "
          f"(accounts: {SMURFING_ACCOUNTS}, "
          f"rule: {SMURFING_TXN_COUNT}+ within {SMURFING_WINDOW_MIN} min)")
    return pd.concat([df, sm_df], ignore_index=True)


# ---------------------------------------------------------------------------
# Save as JSONL (NiFi GetFile format)
# ---------------------------------------------------------------------------

def save_as_jsonl(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save as JSON Lines format.
    Converts timestamp → txn_time (epoch ms) for SSB Flink DDL: TO_TIMESTAMP_LTZ(txn_time, 3)
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
            record.pop("timestamp", None)  # Remove timestamp; use txn_time uniformly
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate SBI AML demo transaction data")
    parser.add_argument("--rows",   type=int, default=DEMO_ROWS,
                        help=f"Number of normal transactions (default: {DEMO_ROWS})")
    parser.add_argument("--output", type=str, default=DATA_OUTPUT_DIR,
                        help=f"Output directory (default: {DATA_OUTPUT_DIR})")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[AML Data Generation] ENV={os.environ.get('ENV_NAME', 'unknown')}")
    print(f"  Output directory   : {output_dir}")
    print(f"  Normal transactions: {args.rows}")
    print(f"  Large Cash threshold: ₹{LARGE_CASH_THRESHOLD:,}")
    print(f"  Smurfing rule       : {SMURFING_TXN_COUNT}+ within {SMURFING_WINDOW_MIN} min")
    print()

    # 1. Generate normal transactions via SDV
    df = generate_normal_transactions(args.rows)

    # 2. Inject AML patterns
    print("\n[AML Pattern Injection]")
    df = inject_large_cash(df)
    df = inject_smurfing(df)

    # 3. Shuffle (to hide pattern positions)
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    # 4. Save as JSONL
    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"aml_transactions_{timestamp_str}.jsonl"
    save_as_jsonl(df, output_file)

    lc_count = df[df["account_id"].isin(LARGE_CASH_ACCOUNTS)].shape[0]
    sm_count = df[df["account_id"].isin(SMURFING_ACCOUNTS)].shape[0]

    print(f"\n[DONE] Saved {len(df)} records → {output_file}")
    print(f"  Normal transactions: {len(df) - lc_count - sm_count}")
    print(f"  Large Cash         : {lc_count}")
    print(f"  Smurfing           : {sm_count}")
    print(f"\nNext steps:")
    print(f"  [NiFi method]   NiFi GetFile will automatically read {output_dir} and send to Kafka")
    print(f"  [Direct method] python data_gen/kafka_producer.py")


if __name__ == "__main__":
    main()
