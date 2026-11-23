# Spark: a zoned TIMESTAMP literal turns off Iceberg partition pruning

A self-contained reproduction of a finding from the main benchmark (Q6 on the Iceberg table,
[ADR 0005](../../docs/adr/0005-iceberg-reads-from-single-node-engines.md)).

Parquet timestamps written without a time zone are read by Spark 3.5 as `TIMESTAMP_NTZ`. A filter
written with the portable literal `TIMESTAMP '2026-10-08 00:00:00'` has the session's zoned type,
so Spark casts the column instead of the literal. The cast hides the column from the Iceberg scan,
and only `start_ts IS NOT NULL` is pushed down: every partition is read.

```bash
make ntz-repro
# or: docker run --rm -v "$PWD":/w -w /w --entrypoint python3 tabulario/spark-iceberg:3.5.5_1.8.1 examples/spark_ntz_pruning/repro.py
```

It builds a 30-day, 6M-row Iceberg table (Hadoop catalog in a temporary folder, partitioned by
`day(start_ts)`, one file per day) and runs the same one-week filter three ways. Measured on the
M4 Pro in Colima on 2026-11-23 ([`result.json`](result.json)):

| Variant | Filter pushed to the Iceberg scan | Files read | Files skipped | Median of 5 |
|---|---|---:|---:|---:|
| A `TIMESTAMP '...'` (portable) | `start_ts IS NOT NULL` | 30 | 0 | 188 ms |
| B `TIMESTAMP_NTZ '...'` | `start_ts >= …, start_ts < …` | 7 | 23 | 44 ms |
| C `TIMESTAMP '...'` with `spark.sql.timestampType=TIMESTAMP_NTZ` | `start_ts >= …, start_ts < …` | 7 | 23 | 42 ms |

All three return the same 1,400,000 rows and the same revenue sum. File counts come from the
Iceberg scan's own metrics (`resultDataFiles`, `skippedDataFiles`), not from timing.

## What to do with it

- In review, run `EXPLAIN` on time-filtered queries against Iceberg tables and check that the
  range reaches the scan's `filters=`.
- Type literals to match the column (`TIMESTAMP_NTZ '...'`), or set
  `spark.sql.timestampType=TIMESTAMP_NTZ` for jobs whose tables are zone-less. The session setting
  changes how every zone-less literal in that session is typed, so it is a job-level decision.

## Limits

One small table on one laptop, Spark 3.5.5 with Iceberg 1.8.1. The time difference here is small
in absolute terms (the table is 21 MB); the file counts are the point. At 200M rows in the main
benchmark the same fix took Q6 from 2.989 s to 0.742 s.
