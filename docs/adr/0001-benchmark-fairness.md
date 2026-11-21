# ADR 0001: How the benchmark stays fair

- Status: accepted
- Date: 2026-11-21

## Context

Engine comparisons are easy to skew: one engine gets more cores, another gets a hand-tuned query,
a third is timed without its start-up cost, and nobody checks that they return the same answer.
This repository is meant to support architecture decisions, so the method matters more than the
headline number.

## Decision

1. **Same input.** One synthetic Parquet dataset (Snappy, 5M-row files) generated with a fixed
   seed. Subscriber IDs are Zipf-skewed so group-by and distinct are not trivially uniform.
2. **Same SQL.** All six queries are read from `bench/queries.sql` and run unchanged by Spark,
   Polars, DataFusion and DuckDB. The one Spark-only variant (Q6 with `TIMESTAMP_NTZ` literals,
   ADR 0005) is reported separately and checked against Q6.
3. **Same limits.** Each engine runs alone in its own container with `--cpus 6 --memory 8g`.
   Spark uses `local[6]` with a 6 GiB driver and 12 shuffle partitions; DataFusion uses
   target partitions equal to the CPU count it sees; Polars and DuckDB use their defaults
   (DuckDB sizes its memory limit from the container's cgroup).
4. **What is timed.** Each query runs once (recorded as the first run), then three more times;
   the median of those three is reported. Start-up and whole-process wall time (from the host,
   including container start) are reported separately, not hidden.
5. **Memory.** Peak memory is the container cgroup's `memory.peak`, which covers the JVM or the
   Python process and everything it allocated.
6. **Correctness gate.** `collect.py` compares every engine's rows with PySpark's (floats to a
   relative 1e-6, dates normalised). Result: 234 of 234 checks identical over 38 runs (10M to 200M
   rows, Parquet and Iceberg).
7. **Failures stay visible.** A run killed for memory is recorded with its exit code and peak
   memory in `summary.json` and marked in the charts, not dropped (ADR 0006).

## Consequences

- The numbers describe single-node behaviour on one laptop (6 vCPUs in a Colima VM). They do not
  say how a Spark cluster behaves at terabyte scale.
- The OS page cache is warm after the first run, so the medians measure compute, not disk.
- Three timed runs per query keep the full benchmark under ten minutes; small differences
  (under about 10%) should not drive a decision.
