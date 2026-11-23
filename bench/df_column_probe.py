"""Which bytes accumulate: run single-column aggregates 4 times each in one DataFusion process (default
allocator settings) and compare the memory added per run with the compressed size of the column read.

Usage (in the engines container): df_column_probe.py DATA_DIR SIZE OUT_JSON
"""
import glob
import json
import sys

import pyarrow.parquet as pq
from datafusion import SessionConfig, SessionContext


def rss_gib():
    for line in open("/proc/self/status"):
        if line.startswith("VmRSS"):
            return int(line.split()[1]) / 2**20


data, size, out = sys.argv[1:4]
compressed = {}
for f in glob.glob(f"{data}/calls_{size}/*.parquet"):
    md = pq.ParquetFile(f).metadata
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            compressed[col.path_in_schema] = compressed.get(col.path_in_schema, 0) + col.total_compressed_size
ctx = SessionContext(SessionConfig().with_target_partitions(6))
ctx.register_parquet("calls", f"{data}/calls_{size}/")
res = {"size": size, "columns": {}}
for col in ("duration_s", "bytes_mb", "call_id"):
    after = []
    for _ in range(4):
        ctx.sql(f"SELECT sum({col}) FROM calls").collect()
        after.append(round(rss_gib(), 3))
    res["columns"][col] = {"rss_after_runs_gib": after,
                           "added_per_run_gib": round((after[-1] - after[0]) / 3, 3),
                           "compressed_column_gib": round(compressed[col] / 2**30, 3)}
    print(col, res["columns"][col], flush=True)
json.dump(res, open(out, "w"), indent=1)
