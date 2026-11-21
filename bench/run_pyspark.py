"""Run every query with PySpark (Spark SQL), plus a UDF query to show where Python costs.

Usage (inside the Spark container): run_pyspark.py DATA_DIR SIZE OUT_JSON
"""
from __future__ import annotations

import os
import sys
import time

T0 = time.perf_counter()
sys.path.insert(0, os.path.dirname(__file__))
from common import bench, queries, write  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import StringType  # noqa: E402

UDF_SQL = "SELECT tier, count(*) AS n FROM (SELECT {fn}(revenue_try) AS tier FROM calls) GROUP BY tier ORDER BY tier"


def tier(r):
    return "high" if r is not None and r >= 1.0 else ("mid" if r is not None and r >= 0.1 else "low")


def main(data, size, out):
    cores = int(os.environ.get("SPARK_CORES", "6"))
    fmt = os.environ.get("FORMAT", "parquet")
    builder = SparkSession.builder
    if fmt == "iceberg":  # Hadoop-catalog tables written by make_iceberg.py
        builder = (builder.config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
                   .config("spark.sql.catalog.lab.type", "hadoop")
                   .config("spark.sql.catalog.lab.warehouse", f"{data}/iceberg"))
    spark = (builder.master(f"local[{cores}]").appName("mf-bench")
             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
             .config("spark.sql.shuffle.partitions", str(cores * 2))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.defaultCatalog", "spark_catalog")
             .config("spark.eventLog.enabled", "false")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    if fmt == "iceberg":
        spark.table(f"lab.bench.calls_{size}").createOrReplaceTempView("calls")
        spark.table("lab.bench.cells").createOrReplaceTempView("cells")
    else:
        spark.read.parquet(f"{data}/calls_{size}").createOrReplaceTempView("calls")
        spark.read.parquet(f"{data}/cells.parquet").createOrReplaceTempView("cells")
    startup = time.perf_counter() - T0
    run = lambda sql: [tuple(r) for r in spark.sql(sql).collect()]  # noqa: E731
    res = {"engine": "pyspark", "version": spark.version, "size": size, "format": fmt, "cpus": cores, "startup_s": round(startup, 3), "queries": {}}
    for name, sql in queries(os.path.join(os.path.dirname(__file__), "queries.sql")).items():
        timing, rows = bench(run, sql)
        res["queries"][name] = timing | {"rows": rows}
        print("pyspark", fmt, size, name, timing["median_s"], flush=True)
    # Spark-only variant of Q6: start_ts is TIMESTAMP_NTZ (Parquet timestamps without a zone), and the
    # portable TIMESTAMP literal is zoned, so Spark casts the column and cannot push the filter into
    # the scan. A TIMESTAMP_NTZ literal keeps the filter on the column, so Iceberg can skip partitions.
    sql = queries(os.path.join(os.path.dirname(__file__), "queries.sql"))["q6_one_week"].replace("TIMESTAMP '", "TIMESTAMP_NTZ '")
    timing, rows = bench(run, sql)
    res["queries"]["q6_one_week_ntz"] = timing | {"rows": rows}
    print("pyspark", fmt, size, "q6_one_week_ntz", timing["median_s"], flush=True)
    if os.environ.get("UDF", "1") == "0":
        write(out, res); spark.stop(); return
    # UDF variants of the same query: built-in expression, Python UDF, Arrow (pandas) UDF
    spark.udf.register("tier_py", tier, StringType())
    try:
        import pandas as pd  # noqa: F401
        from pyspark.sql.functions import pandas_udf

        @pandas_udf(StringType())
        def tier_arrow(r):
            import numpy as np
            return pd.Series(np.where(r >= 1.0, "high", np.where(r >= 0.1, "mid", "low")))
        spark.udf.register("tier_arrow", tier_arrow)
        variants = ["builtin", "tier_py", "tier_arrow"]
    except Exception as e:  # noqa: BLE001
        print("no pandas/pyarrow for Arrow UDF:", e)
        variants = ["builtin", "tier_py"]
    builtin = "CASE WHEN revenue_try >= 1.0 THEN 'high' WHEN revenue_try >= 0.1 THEN 'mid' ELSE 'low' END"
    for v in variants:
        sql = (UDF_SQL.replace("{fn}(revenue_try)", builtin) if v == "builtin" else UDF_SQL.format(fn=v))
        timing, rows = bench(run, sql)
        res["queries"][f"udf_{v}"] = timing | {"rows": rows}
        print("pyspark", size, f"udf_{v}", timing["median_s"], flush=True)
    write(out, res)
    spark.stop()


if __name__ == "__main__":
    main(*sys.argv[1:4])
