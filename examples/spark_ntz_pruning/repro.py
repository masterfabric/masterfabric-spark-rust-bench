"""Minimal reproduction: a zoned TIMESTAMP literal turns off Iceberg partition pruning on a TIMESTAMP_NTZ column.

Self-contained: builds a small day-partitioned Iceberg table in a temporary warehouse (30 days, one file per
day, no time zone on the column, as Parquet files written without a zone are read by Spark) and runs the same
one-week filter three ways:
  A  TIMESTAMP '2026-10-08 00:00:00'                     (portable SQL, what the benchmark's Q6 uses)
  B  TIMESTAMP_NTZ '2026-10-08 00:00:00'                 (typed to match the column)
  C  TIMESTAMP '...' with spark.sql.timestampType=TIMESTAMP_NTZ  (session setting, SQL text unchanged)
For each it prints the filter Spark hands to the Iceberg scan, the data files Iceberg planned and skipped
(the scan's own metrics), the rows returned and the median of 5 runs. AQE is off so the scan node is easy to
find in the executed plan.

Run (about a minute; needs only Docker and the Spark image the benchmark already uses):
  docker run --rm -v "$PWD":/w -w /w --entrypoint python3 tabulario/spark-iceberg:3.5.5_1.8.1 examples/spark_ntz_pruning/repro.py
Writes examples/spark_ntz_pruning/result.json.
"""
from __future__ import annotations

import json
import os
import statistics
import tempfile
import time

from pyspark.sql import SparkSession

DAYS, ROWS_PER_DAY = 30, 200_000
HERE = os.path.dirname(os.path.abspath(__file__))
WH = tempfile.mkdtemp(prefix="ntz_wh_")

spark = (SparkSession.builder.master("local[4]")
         .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.lab.type", "hadoop")
         .config("spark.sql.catalog.lab.warehouse", WH)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.defaultCatalog", "spark_catalog")  # the image defaults to a REST catalog we do not run
         .config("spark.sql.adaptive.enabled", "false")
         .config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

spark.sql("CREATE TABLE lab.db.calls (call_id BIGINT, start_ts TIMESTAMP_NTZ, revenue DOUBLE) "
          "USING iceberg PARTITIONED BY (days(start_ts))")
spark.sql(f"""
  INSERT INTO lab.db.calls
  SELECT id, CAST(TIMESTAMP_NTZ '2026-10-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, (id * 86400.0 * {DAYS}) / {DAYS * ROWS_PER_DAY}) AS TIMESTAMP_NTZ),
         (id % 97) / 10.0
  FROM range({DAYS * ROWS_PER_DAY})
""")
files = spark.sql("SELECT count(*) FROM lab.db.calls.files").first()[0]

WHERE = "start_ts >= {lit} '2026-10-08 00:00:00' AND start_ts < {lit} '2026-10-15 00:00:00'"
SQL = "SELECT count(*) AS n, round(sum(revenue), 1) AS revenue FROM lab.db.calls WHERE " + WHERE


def scan_info(df):
    plan = df._jdf.queryExecution().executedPlan()
    leaves = plan.collectLeaves()
    out = {}
    for i in range(leaves.size()):
        node = leaves.apply(i)
        if "BatchScan" in node.nodeName():
            metrics = node.metrics()
            for k in ("resultDataFiles", "skippedDataFiles", "totalDataFileSize"):
                m = metrics.get(k)
                if m.isDefined():
                    out[k] = int(m.get().value())
            desc = node.simpleString(500)
            out["scan_filters"] = desc[desc.find("filters="):].split(", groupedBy")[0][:300] if "filters=" in desc else desc[:300]
    return out


def run(name, lit, ntz_session=False):
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ" if ntz_session else "TIMESTAMP_LTZ")
    sql = SQL.format(lit=lit)
    times, row, info = [], None, {}
    for _ in range(6):
        df = spark.sql(sql)
        t = time.perf_counter()
        row = df.collect()[0]
        times.append(time.perf_counter() - t)
        info = scan_info(df)
    filt = [ln.strip() for ln in spark.sql("EXPLAIN " + sql).first()[0].splitlines() if "Filter" in ln][:1]
    r = {"variant": name, "sql_literal": lit, "session_timestampType": "TIMESTAMP_NTZ" if ntz_session else "TIMESTAMP_LTZ",
         "rows": int(row["n"]), "revenue": float(row["revenue"]), "median_s_of_5": round(statistics.median(times[1:]), 4),
         "spark_filter": filt[0] if filt else ""} | info
    print(json.dumps(r), flush=True)
    return r


res = {"spark": spark.version, "table_files": files, "days": DAYS, "rows": DAYS * ROWS_PER_DAY,
       "variants": [run("A zoned literal", "TIMESTAMP"), run("B NTZ literal", "TIMESTAMP_NTZ"),
                    run("C zoned literal, session timestampType=NTZ", "TIMESTAMP", ntz_session=True)]}
with open(os.path.join(HERE, "result.json"), "w") as f:
    json.dump(res, f, indent=1)
spark.stop()
