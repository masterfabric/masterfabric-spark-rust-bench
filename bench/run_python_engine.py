"""Run every query with Polars, DataFusion or DuckDB.

Usage: run_python_engine.py ENGINE DATA_DIR SIZE OUT_JSON [FORMAT]
FORMAT is "parquet" (default: a folder of Parquet files) or "iceberg" (the Iceberg tables written by
bench/make_iceberg.py under DATA_DIR/iceberg; only readable inside the containers, where the paths
recorded in the Iceberg metadata exist).
"""
from __future__ import annotations

import os
import sys
import time

T0 = time.perf_counter()
sys.path.insert(0, os.path.dirname(__file__))
from common import bench, iceberg_metadata, queries, write  # noqa: E402

os.environ.setdefault("POLARS_ICEBERG_PLANNER", "pyiceberg")


def polars_engine(data, size, fmt="parquet"):
    import polars as pl
    if fmt == "iceberg":
        calls = pl.scan_iceberg(iceberg_metadata(data, f"calls_{size}"))
        cells = pl.scan_iceberg(iceberg_metadata(data, "cells"))
    else:
        calls, cells = pl.scan_parquet(f"{data}/calls_{size}/*.parquet"), pl.scan_parquet(f"{data}/cells.parquet")
    ctx = pl.SQLContext(calls=calls, cells=cells)
    return pl.__version__, lambda sql: ctx.execute(sql).collect().rows()


def _datafusion_iceberg(ctx, name, metadata):
    """DataFusion 55 has no Iceberg reader we can load (pyiceberg-core 0.11 is built against DataFusion 54),
    so PyIceberg plans the current snapshot and DataFusion reads exactly those Parquet files."""
    from pyiceberg.table import StaticTable
    tasks = list(StaticTable.from_metadata(metadata).scan().plan_files())
    if any(t.delete_files for t in tasks):
        raise RuntimeError(f"{name}: delete files present; this reader only handles append-only tables")
    frames = [ctx.read_parquet(t.file.file_path.removeprefix("file:")) for t in tasks]
    df = frames[0]
    for f in frames[1:]:
        df = df.union(f)
    ctx.register_table(name, df.into_view())
    return len(tasks)


def datafusion_engine(data, size, fmt="parquet"):
    import datafusion
    from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext
    runtime = None
    if os.environ.get("DF_MEMORY_POOL_GIB"):  # bounded pool: operators that can spill write to disk instead of growing
        runtime = (RuntimeEnvBuilder().with_disk_manager_os()
                   .with_fair_spill_pool(int(float(os.environ["DF_MEMORY_POOL_GIB"]) * 2**30)))
    ctx = SessionContext(SessionConfig().with_target_partitions(os.cpu_count()), runtime)
    if fmt == "iceberg":
        _datafusion_iceberg(ctx, "calls", iceberg_metadata(data, f"calls_{size}"))
        _datafusion_iceberg(ctx, "cells", iceberg_metadata(data, "cells"))
    else:
        ctx.register_parquet("calls", f"{data}/calls_{size}/")
        ctx.register_parquet("cells", f"{data}/cells.parquet")
    return datafusion.__version__, lambda sql: [tuple(r.values()) for r in ctx.sql(sql).to_arrow_table().to_pylist()]


def duckdb_engine(data, size, fmt="parquet"):
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads = {os.cpu_count()}")
    if fmt == "iceberg":
        con.execute("LOAD iceberg")
        src = {t: f"iceberg_scan('{iceberg_metadata(data, t)}')" for t in (f"calls_{size}", "cells")}
        calls, cells = src[f"calls_{size}"], src["cells"]
    else:
        calls, cells = f"read_parquet('{data}/calls_{size}/*.parquet')", f"read_parquet('{data}/cells.parquet')"
    con.execute(f"CREATE VIEW calls AS SELECT * FROM {calls}")
    con.execute(f"CREATE VIEW cells AS SELECT * FROM {cells}")
    return duckdb.__version__, lambda sql: con.execute(sql).fetchall()


ENGINES = {"polars": polars_engine, "datafusion": datafusion_engine, "duckdb": duckdb_engine}


def main(engine, data, size, out, fmt="parquet"):
    version, run = ENGINES[engine](data, size, fmt)
    startup = time.perf_counter() - T0
    res = {"engine": engine, "version": version, "size": size, "format": fmt, "cpus": os.cpu_count(),
           "startup_s": round(startup, 3), "queries": {}}
    for name, sql in queries(os.path.join(os.path.dirname(__file__), "queries.sql")).items():
        timing, rows = bench(run, sql)
        res["queries"][name] = timing | {"rows": rows}
        print(engine, fmt, size, name, timing["median_s"], flush=True)
    write(out, res)


if __name__ == "__main__":
    main(*sys.argv[1:6])
