"""Write the Parquet datasets as Iceberg tables (Hadoop catalog under DATA_DIR/iceberg) with Spark.

Usage (inside the Spark container): make_iceberg.py DATA_DIR SIZE [SIZE ...]
calls_<size> is partitioned by day(start_ts) so a time filter can skip whole partitions; cells is
unpartitioned. Tables are replaced if they exist. Prints file and partition counts per table.
"""
from __future__ import annotations

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import days


def spark_session(data):
    return (SparkSession.builder.master("local[6]").appName("mf-iceberg")
            .config("spark.driver.memory", "6g")
            .config("spark.sql.shuffle.partitions", "12")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.defaultCatalog", "spark_catalog")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lab.type", "hadoop")
            .config("spark.sql.catalog.lab.warehouse", f"{data}/iceberg")
            .config("spark.eventLog.enabled", "false")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def main(data, *sizes):
    spark = spark_session(data)
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.bench")
    spark.read.parquet(f"{data}/cells.parquet").writeTo("lab.bench.cells").createOrReplace()
    for size in sizes:
        t = time.perf_counter()
        (spark.read.parquet(f"{data}/calls_{size}")
         .writeTo(f"lab.bench.calls_{size}")
         .partitionedBy(days("start_ts"))
         .tableProperty("write.parquet.compression-codec", "snappy")
         .createOrReplace())
        files = spark.sql(f"SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes FROM lab.bench.calls_{size}.files").first()
        parts = spark.sql(f"SELECT count(*) FROM lab.bench.calls_{size}.partitions").first()[0]
        print(f"iceberg calls_{size}: {files.files} files, {parts} partitions, {files.bytes / 2**20:.0f} MiB, written in {time.perf_counter() - t:.1f} s", flush=True)
    spark.stop()


if __name__ == "__main__":
    main(*sys.argv[1:])
