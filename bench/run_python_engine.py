"""Run every query with Polars or DataFusion. Usage: run_python_engine.py ENGINE DATA_DIR SIZE OUT_JSON"""
from __future__ import annotations

import os
import sys
import time

T0 = time.perf_counter()
sys.path.insert(0, os.path.dirname(__file__))
from common import bench, queries, write  # noqa: E402


def polars_engine(data, size):
    import polars as pl
    ctx = pl.SQLContext(calls=pl.scan_parquet(f"{data}/calls_{size}/*.parquet"), cells=pl.scan_parquet(f"{data}/cells.parquet"))
    return pl.__version__, lambda sql: ctx.execute(sql).collect().rows()


def datafusion_engine(data, size):
    import datafusion
    from datafusion import SessionConfig, SessionContext
    ctx = SessionContext(SessionConfig().with_target_partitions(os.cpu_count()))
    ctx.register_parquet("calls", f"{data}/calls_{size}/")
    ctx.register_parquet("cells", f"{data}/cells.parquet")
    return datafusion.__version__, lambda sql: [tuple(r.values()) for r in ctx.sql(sql).to_arrow_table().to_pylist()]


def main(engine, data, size, out):
    version, run = {"polars": polars_engine, "datafusion": datafusion_engine}[engine](data, size)
    startup = time.perf_counter() - T0
    res = {"engine": engine, "version": version, "size": size, "cpus": os.cpu_count(), "startup_s": round(startup, 3), "queries": {}}
    for name, sql in queries(os.path.join(os.path.dirname(__file__), "queries.sql")).items():
        timing, rows = bench(run, sql)
        res["queries"][name] = timing | {"rows": rows}
        print(engine, size, name, timing["median_s"], flush=True)
    write(out, res)


if __name__ == "__main__":
    main(*sys.argv[1:5])
