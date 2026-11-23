"""Why DataFusion's memory grows across repeated queries: run the six queries 4 times each (as the benchmark
does) in one session and record the container's memory after every run.

Usage: df_memory.py DATA_DIR SIZE OUT_JSONL VARIANT
Variants (set by bench/df_memory.sh through environment variables, recorded in the output):
  SESSION_PER_RUN=1     a new SessionContext for every query run instead of one per process
  DF_MEMORY_POOL_GIB=N  DataFusion fair spill pool of N GiB with an OS disk manager
  MIMALLOC_*            options of the mimalloc allocator that the datafusion wheel is built with
  DF_SETTINGS="k=v;k=v" DataFusion configuration settings applied to every session
  DF_SET="k=v;k=v"      settings applied with SQL SET after the session exists (runtime settings)
  CONSUME=arrow|collect|stream  how results are pulled into Python (default arrow, as the benchmark)
  PAUSE_S=12            sleep between runs (not timed), long enough for tokio's idle blocking threads to exit
  ONLY=q1_filter_agg RUNS=6  restrict the queries and change the number of runs (diagnosis only)
One JSON line per run is appended as soon as it finishes, so a run killed for memory keeps its history.
"""
from __future__ import annotations

import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from common import queries  # noqa: E402


def cg(name):
    try:
        return int(open(f"/sys/fs/cgroup/{name}").read().split()[0])
    except OSError:
        return -1


def anon():
    for line in open("/sys/fs/cgroup/memory.stat"):
        k, v = line.split()
        if k == "anon":
            return int(v)
    return -1


def rss():
    for line in open("/proc/self/status"):
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return -1


def main(data, size, out, variant):
    from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

    def make():
        runtime = None
        if os.environ.get("DF_MEMORY_POOL_GIB"):
            runtime = (RuntimeEnvBuilder().with_disk_manager_os()
                       .with_fair_spill_pool(int(float(os.environ["DF_MEMORY_POOL_GIB"]) * 2**30)))
        cfg = SessionConfig().with_target_partitions(os.cpu_count())
        for kv in filter(None, os.environ.get("DF_SETTINGS", "").split(";")):
            k, v = kv.split("=", 1)
            cfg = cfg.set(k, v)
        ctx = SessionContext(cfg, runtime)
        for kv in filter(None, os.environ.get("DF_SET", "").split(";")):
            k, v = kv.split("=", 1)
            ctx.sql(f"SET {k} = '{v}'").collect()
        ctx.register_parquet("calls", f"{data}/calls_{size}/")
        ctx.register_parquet("cells", f"{data}/cells.parquet")
        return ctx

    env = {k: v for k, v in os.environ.items() if k.startswith(("MIMALLOC_", "DF_", "SESSION_", "ONLY", "RUNS", "CONSUME", "PAUSE_S"))}
    runs = int(os.environ.get("RUNS", "4"))
    per_run = os.environ.get("SESSION_PER_RUN") == "1"
    ctx = None if per_run else make()
    with open(out, "a") as f:
        f.write(json.dumps({"variant": variant, "env": env, "start_rss": rss(), "start_cg": cg("memory.current")}) + "\n")
        for name, sql in queries(os.path.join(os.path.dirname(__file__), "queries.sql")).items():
            if os.environ.get("ONLY") and name not in os.environ["ONLY"].split(","):
                continue
            for i in range(runs):
                c = make() if per_run else ctx
                t = time.perf_counter()
                mode = os.environ.get("CONSUME", "arrow")
                if mode == "collect":
                    rows = sum(b.num_rows for b in c.sql(sql).collect())
                elif mode == "stream":
                    rows = sum(b.to_pyarrow().num_rows for b in c.sql(sql).execute_stream())
                else:
                    rows = c.sql(sql).to_arrow_table().num_rows
                secs = time.perf_counter() - t
                if per_run:
                    del c
                if os.environ.get("PAUSE_S"):
                    time.sleep(float(os.environ["PAUSE_S"]))
                f.write(json.dumps({"variant": variant, "query": name, "run": i + 1, "seconds": round(secs, 3),
                                    "rows": rows, "rss": rss(), "cgroup_current": cg("memory.current"),
                                    "cgroup_anon": anon(), "cgroup_peak": cg("memory.peak"),
                                    "threads": len(os.listdir("/proc/self/task"))}) + "\n")
                f.flush()
                print(variant, name, i + 1, round(secs, 3), round(rss() / 2**30, 2), "GiB rss", flush=True)


if __name__ == "__main__":
    main(*sys.argv[1:5])
