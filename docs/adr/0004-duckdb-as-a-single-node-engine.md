# ADR 0004: DuckDB as a single-node engine next to Polars and DataFusion

- Status: accepted
- Date: 2026-11-21

## Context

ADR 0002 recommends a single-node engine for jobs that fit on one machine, but only two were
measured, both Rust. DuckDB (C++) is the engine many analysts already use, it reads Parquet and
Iceberg directly, and it has a memory limit and spilling built in. It was added with the same SQL,
the same container limits and the same correctness gate.

Sum of the six query medians (seconds), each engine in a 6 CPU / 8 GiB container:

| Rows | Polars 2.1.0 | DuckDB 2.0.1 | DataFusion 55.0.0 | PySpark 3.5.5 |
|---|---:|---:|---:|---:|
| 10M | 0.260 | 0.310 | 0.293 | 1.545 |
| 50M | 1.103 | 1.217 | 1.632 | 4.660 |
| 100M | 2.146 | 2.373 | 2.896 | 9.067 |
| 200M | 4.325 | 4.777 | killed at 8 GiB | 15.975 |

Peak container memory at 200M rows: DuckDB 3.2 GiB, Polars 3.6 GiB, PySpark 7.6 GiB. DuckDB
detected the container limit by itself (`memory_limit` 6.3 GiB inside an 8 GiB container).

## Decision

- Treat DuckDB and Polars as equivalent first choices for single-node SQL jobs: within 10 to 20%
  of each other on every size, and per query each wins some (DuckDB Q1 at 200M rows, Polars Q2
  and Q5). Choose by the team's interface (DuckDB for SQL-first and notebook users, Polars for
  dataframe code) rather than by these timings.
- Prefer DuckDB when the job may approach the machine's memory: it sizes itself to the cgroup
  limit and had the lowest peak memory at every size (0.4 GiB at 10M rows, 3.2 GiB at 200M).
- Keep DataFusion for teams that build their own engine or service on it (it is a library, and
  the base of Comet and Ballista); as a stand-alone tool it was slower here and ran out of memory
  at 200M rows (ADR 0006).

## Consequences

- The small-job path in the platform can offer DuckDB and Polars in one image (`mf-srb/engines:2`).
- One more engine in every chart and check: 38 runs, 234 of 234 query results identical to Spark's.
