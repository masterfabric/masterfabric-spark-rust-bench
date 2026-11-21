# ADR 0002: When to use Spark and when a single-node Rust engine

- Status: accepted
- Date: 2026-11-21

## Context

Telco analytics teams run a mix of jobs: very large batch over a whole data lake, and many
small-to-medium jobs (a day or a month of one table, a feature table, a report) that also end
up on the cluster. The measured results here put numbers on the second group.

At 50M rows (1 GiB of Parquet), in equal 6 CPU / 8 GiB containers:

| | Polars 2.1.0 | DataFusion 55.0.0 | PySpark 3.5.5 | Scala Spark 3.5.5 |
|---|---:|---:|---:|---:|
| Sum of 5 query medians | 0.97 s | 1.45 s | 3.73 s | 3.93 s |
| Peak container memory | 1.7 GiB | 3.5 GiB | 3.5 GiB | 4.9 GiB |
| Engine start-up (in process) | 0.10 s | 0.06 s | 2.1 s | 0.95 s + JVM |

## Decision

- **Single-node engine (Polars or DataFusion)** when the input for one job fits on one machine
  after column and partition pruning, roughly up to tens of GiB of Parquet per job. That covers
  most ad-hoc analysis, feature engineering and data-quality jobs. Here they ran the same SQL
  2.6 to 4 times faster with up to 2.9 times less memory and almost no start-up time.
- **Spark** when data or shuffle state does not fit on one node, when the job needs the cluster's
  fault tolerance, or when it must sit inside an existing platform (Cloudera Data Engineering,
  Iceberg tables with Spark-managed maintenance, Kerberos/Ranger-governed access).
- **Keep the SQL portable.** All five queries ran unchanged on all three engines, so a job can
  start on a single node and move to Spark without a rewrite. Iceberg tables let both read the
  same data.
- **Rust inside Spark** is the path when Spark has to stay: native execution plug-ins such as
  Apache DataFusion Comet or Apache Gluten (Velox) replace Spark's JVM operators. Ballista is
  the distributed DataFusion option. They were not measured here.

## Consequences

- The platform should offer a small-job path (a container with Polars or DataFusion reading
  the lakehouse) next to Spark, instead of sending every job to the cluster.
- Spark remains the default for jobs above the single-node limit; this benchmark does not
  argue otherwise because it never runs a cluster.
