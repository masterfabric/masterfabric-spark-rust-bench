# ADR 0005: Reading Iceberg tables from single-node engines

- Status: accepted
- Date: 2026-11-21

## Context

In the lakehouse (see masterfabric-lakehouse-lab) data lives in Iceberg tables written by Spark.
A small-job path is only useful if single-node engines can read those tables directly, with the
snapshot and partition pruning Iceberg provides, instead of a copied folder of Parquet.

Spark wrote the 50M and 200M-row datasets as Iceberg tables partitioned by `day(start_ts)` in a
Hadoop catalog (metadata files only, no catalog service): 30 files (1,076 MiB, 17.1 s) and 92
files (4,307 MiB, 59.1 s). Q6 asks for one week of the 30 days; PyIceberg's plan for it keeps 7 of
30 files at 50M rows and 20 of 92 at 200M (23% of the bytes), in 3 to 4 ms.

How each engine read the table:

| Engine | Reader |
|---|---|
| PySpark 3.5.5 | Iceberg 1.8.1 Spark runtime, Hadoop catalog |
| Polars 2.1.0 | `scan_iceberg` on the metadata file (PyIceberg 0.12.0 plans the scan) |
| DuckDB 2.0.1 | `iceberg_scan` from the iceberg extension, installed into the image at build time |
| DataFusion 55.0.0 | PyIceberg plans the snapshot, DataFusion reads exactly those Parquet files |

The native route for DataFusion, the table provider in `pyiceberg-core` 0.11.0, is built against
DataFusion 54 and refuses to load into DataFusion 55 ("Incompatible DataFusion table provider
major version 54; expected 55").

Measured, median seconds:

| | Parquet folder | Iceberg |
|---|---:|---:|
| Q6, 200M rows, DuckDB | 0.693 | 0.314 |
| Q6, 200M rows, Polars | 0.534 | 0.349 |
| Q6, 200M rows, PySpark, portable `TIMESTAMP` literal | 3.383 | 2.989 |
| Q6, 200M rows, PySpark, `TIMESTAMP_NTZ` literal | 1.336 | 0.742 |
| Q1–Q5 sum, 200M rows, DuckDB | 4.084 | 4.068 |
| Q1–Q5 sum, 200M rows, Polars | 3.791 | 4.138 |
| Q1–Q5 sum, 200M rows, PySpark | 12.592 | 12.410 |

The Parquet files have timestamps without a zone, so Spark reads `start_ts` as `TIMESTAMP_NTZ`.
The portable `TIMESTAMP '...'` literal is zoned; Spark casts the column to compare, and EXPLAIN
shows the range never reaches the Iceberg scan (only `start_ts IS NOT NULL` is pushed), so all 30
partitions are read. With a `TIMESTAMP_NTZ` literal the range is pushed down and Q6 at 200M rows
takes 0.742 s instead of 2.989 s.

## Decision

- Single-node jobs read the lakehouse's Iceberg tables directly: DuckDB with `iceberg_scan`,
  Polars with `scan_iceberg`. Full scans cost about the same as plain Parquet (within 10% for Polars, DuckDB
  and Spark; DataFusion was 21% faster at 50M rows); selective filters on the partition column
  are 1.5 to 2.2 times faster.
- DataFusion reads Iceberg through PyIceberg's plan until a `pyiceberg-core` built for the same
  DataFusion major version is available. This path only handles append-only tables (it refuses
  tables with delete files) and uses more memory (7.3 GiB peak at 50M rows against 3.5 GiB from
  the Parquet folder), so it is not the recommended route.
- In Spark SQL over timestamp-without-zone columns, write filter literals as `TIMESTAMP_NTZ`
  (or cast the literal), and check with EXPLAIN that the filter reaches the scan. A filter that
  is correct but not pushed down reads every partition.

## Consequences

- Single-node engines need read access to the table's metadata and data files; with a REST or
  Hive catalog, PyIceberg or the DuckDB extension also needs the catalog endpoint and its
  credentials. Not measured here: only the Hadoop catalog layout was used.
- Writes and table maintenance stay with Spark (or another Iceberg writer); none of the
  single-node readers here wrote to the table.
- Version pinning matters: the Iceberg readers are newer and less stable than the Parquet readers,
  as the DataFusion mismatch shows.
