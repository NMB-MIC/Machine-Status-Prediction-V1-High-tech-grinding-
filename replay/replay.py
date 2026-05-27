# replay/replay_parquet_to_kafka.py
import argparse
import time
import json
import os
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, timezone


def to_iso(ts):
    """Convert any timestamp to UTC ISO-8601 string."""
    if isinstance(ts, str):
        return ts
    ts = pd.to_datetime(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.isoformat()


def load_data(path: str) -> pd.DataFrame:
    """Auto-detect file format and load."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".parquet":
        df = pd.read_parquet(path)
    elif ext in (".csv", ".tsv"):
        df = pd.read_csv(path)
    else:
        # Try CSV as fallback
        df = pd.read_csv(path)

    # Normalize column names to lowercase
    df.columns = [c.strip().lower() for c in df.columns]

    # Validate required columns
    required = {"mc_no", "occurred", "mc_status"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Got: {list(df.columns)}")

    # Normalize status
    df["mc_status"] = df["mc_status"].astype(str).str.strip().str.lower()

    # Parse timestamps
    df["occurred"] = pd.to_datetime(df["occurred"])

    # Sort chronologically per machine
    df = df.sort_values(["mc_no", "occurred"]).reset_index(drop=True)

    return df


def main():
    ap = argparse.ArgumentParser(description="Replay CSV/Parquet data to Kafka")
    ap.add_argument("--input", required=False, help="Path to CSV or Parquet file")
    ap.add_argument("--parquet", required=False, help="(Backward compat) Same as --input")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="iot.machine.status.raw")
    ap.add_argument("--sleep", type=float, default=0.0,
                    help="Seconds between messages (0 = full speed)")
    ap.add_argument("--limit", type=int, default=0,
                    help="Max rows to send (0 = all)")
    args = ap.parse_args()

    # Resolve input path (--input takes priority over --parquet)
    path = args.input or args.parquet
    if not path:
        ap.error("Must provide --input or --parquet")

    print(f"Loading data from: {path}")
    df = load_data(path)
    print(f"Loaded {len(df)} rows, {df['mc_no'].nunique()} machines, "
          f"statuses: {sorted(df['mc_status'].unique())}")

    if args.limit > 0:
        df = df.head(args.limit)
        print(f"Limited to {len(df)} rows")

    prod = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    sent = 0
    t0 = time.time()
    for i, row in df.iterrows():
        rec = {
            "event_id": f"{row['mc_no']}-{i}",
            "mc_no": row["mc_no"],
            "occurred_ts": to_iso(row["occurred"]),
            "mc_status": row["mc_status"],
            "ingest_ts": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
        }
        prod.send(args.topic, key=row["mc_no"], value=rec)
        sent += 1

        if args.sleep > 0:
            time.sleep(args.sleep)

        # Progress every 50k rows
        if sent % 50000 == 0:
            elapsed = time.time() - t0
            rate = sent / max(elapsed, 0.001)
            print(f"  Sent {sent}/{len(df)} ({rate:.0f} msg/s)")

    prod.flush()
    elapsed = time.time() - t0
    print(f"Replay finished: {sent} messages in {elapsed:.1f}s "
          f"({sent/max(elapsed,0.001):.0f} msg/s)")


if __name__ == "__main__":
    main()