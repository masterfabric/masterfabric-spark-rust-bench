"""Generate synthetic telco usage records (calls) and a cell dimension as Parquet.

No real subscribers: ids are integers, traffic follows simple skewed distributions.
Usage: gen_data.py ROWS_IN_MILLIONS [OUT_DIR]
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

REGIONS = ["Marmara", "Aegean", "Mediterranean", "Central Anatolia", "Black Sea", "Eastern Anatolia", "Southeastern Anatolia"]
N_CELLS, N_SUBS, CHUNK = 20_000, 2_000_000, 5_000_000
EPOCH = np.datetime64("2026-10-01T00:00:00", "us")


def cells(rng) -> pa.Table:
    ids = np.arange(1, N_CELLS + 1, dtype=np.int32)
    return pa.table({"cell_id": ids,
                     "region": pa.array(rng.choice(REGIONS, N_CELLS, p=[.3, .14, .13, .15, .1, .09, .09])),
                     "technology": pa.array(rng.choice(["4G", "5G"], N_CELLS, p=[.7, .3]))})


def calls(rng, start: int, n: int) -> pa.Table:
    sub = (rng.zipf(1.3, n) % N_SUBS + 1).astype(np.int32)  # skewed: a few heavy users
    kind = rng.choice(np.array(["data", "voice", "sms"]), n, p=[.6, .3, .1])
    dur = np.where(kind == "sms", 0, rng.exponential(120, n)).astype(np.int32)
    mb = np.where(kind == "data", rng.lognormal(2.0, 1.2, n), 0.0).round(3)
    return pa.table({
        "call_id": np.arange(start, start + n, dtype=np.int64),
        "subscriber_id": sub,
        "cell_id": rng.integers(1, N_CELLS + 1, n, dtype=np.int32),
        "start_ts": pa.array(EPOCH + rng.integers(0, 30 * 86400 * 10**6, n).astype("timedelta64[us]")),
        "call_type": pa.array(kind),
        "duration_s": dur,
        "bytes_mb": mb,
        "revenue_try": (0.02 * dur / 60 + 0.01 * mb + np.where(kind == "sms", 0.15, 0)).round(4),
    })


def main(millions: int, out: Path):
    rng = np.random.default_rng(42)
    d = out / f"calls_{millions}m"
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(cells(rng), out / "cells.parquet")
    total = millions * 1_000_000
    for i, start in enumerate(range(0, total, CHUNK)):
        n = min(CHUNK, total - start)
        pq.write_table(calls(rng, start, n), d / f"part-{i:03d}.parquet", row_group_size=1_000_000, compression="snappy")
    size = sum(f.stat().st_size for f in d.glob("*.parquet"))
    print(f"{total:,} rows -> {d} ({size / 2**20:,.0f} MiB)")


if __name__ == "__main__":
    main(int(sys.argv[1]), Path(sys.argv[2] if len(sys.argv) > 2 else "data"))
