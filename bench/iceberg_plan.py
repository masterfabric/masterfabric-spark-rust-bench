"""How much of an Iceberg table a query has to read: plan the full scan and the Q6 one-week scan with
PyIceberg and count data files and bytes. Usage: iceberg_plan.py DATA_DIR SIZE OUT_JSON"""
from __future__ import annotations

import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import iceberg_metadata, write  # noqa: E402
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan  # noqa: E402
from pyiceberg.table import StaticTable  # noqa: E402

WEEK = And(GreaterThanOrEqual("start_ts", "2026-10-08T00:00:00"), LessThan("start_ts", "2026-10-15T00:00:00"))


def plan(table, row_filter=None):
    t = time.perf_counter()
    tasks = list(table.scan(row_filter=row_filter).plan_files() if row_filter is not None else table.scan().plan_files())
    return {"files": len(tasks), "bytes": sum(x.file.file_size_in_bytes for x in tasks),
            "rows": sum(x.file.record_count for x in tasks), "plan_s": round(time.perf_counter() - t, 4)}


def main(data, size, out):
    table = StaticTable.from_metadata(iceberg_metadata(data, f"calls_{size}"))
    res = {"size": size, "partition_spec": str(table.spec()), "full_scan": plan(table), "q6_one_week": plan(table, WEEK)}
    print(res)
    write(out, res)


if __name__ == "__main__":
    main(*sys.argv[1:4])
