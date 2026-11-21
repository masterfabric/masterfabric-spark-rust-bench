"""Shared by the Python runners: query loading, timing loop and result output."""
from __future__ import annotations

import json
import re
import statistics
import time
from pathlib import Path

REPEATS = 3


def queries(path: str = "bench/queries.sql") -> dict[str, str]:
    text = Path(path).read_text()
    out = {}
    for block in re.split(r"^-- name: ", text, flags=re.M)[1:]:
        name, sql = block.split("\n", 1)
        out[name.strip()] = sql.strip().rstrip(";")
    return out


def bench(run, sql: str) -> tuple[dict, list]:
    t = time.perf_counter(); rows = run(sql); first = time.perf_counter() - t  # cold-ish: first run in this process
    times = []
    for _ in range(REPEATS):
        t = time.perf_counter(); rows = run(sql); times.append(time.perf_counter() - t)
    return {"first_s": round(first, 4), "median_s": round(statistics.median(times), 4), "runs_s": [round(x, 4) for x in times]}, rows


def iceberg_metadata(data: str, table: str) -> str:
    """Current metadata file of a Hadoop-catalog Iceberg table written by make_iceberg.py."""
    meta = Path(data) / "iceberg" / "bench" / table / "metadata"
    hint = meta / "version-hint.text"
    if hint.exists():  # Hadoop catalog (Spark): vN.metadata.json named by the hint
        return str(meta / f"v{hint.read_text().strip()}.metadata.json")
    return str(max(meta.glob("*.metadata.json")))  # other writers (PyIceberg in the tests): 0000N-<uuid>


def write(out: str, payload: dict) -> None:
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(json.dumps(payload, indent=1, default=str))
