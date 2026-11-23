# ADR 0006: What changes between 10M and 200M rows on one node

- Status: accepted
- Date: 2026-11-21
- Updated: 2026-11-23 by [ADR 0007](0007-datafusion-memory-and-mimalloc.md) (DataFusion's memory growth)

## Context

ADRs 0002 and 0004 rest on 10M and 50M rows. The question for a platform is where the
single-node path stops. The datasets were extended to 100M (2.0 GiB of Parquet) and 200M rows
(4.1 GiB), with the same containers (6 CPU, 8 GiB) and the same six queries.

Sum of the six query medians (seconds) and how much it grows:

| Rows | PySpark | Scala Spark | Polars | DuckDB | DataFusion | PySpark / Polars |
|---|---:|---:|---:|---:|---:|---:|
| 10M | 1.545 | 1.613 | 0.260 | 0.310 | 0.301 | 5.9x |
| 50M | 4.660 | 4.769 | 1.103 | 1.217 | 1.647 | 4.2x |
| 100M | 9.067 | 8.793 | 2.146 | 2.373 | 2.838 | 4.2x |
| 200M | 15.975 | 15.922 | 4.325 | 4.777 | 5.532 | 3.7x |

From 100M to 200M rows the single-node engines take 2.0 times as long; Spark 1.8 times. Spark's
fixed cost per query (scheduling, JVM) is a smaller share of a larger job, so its lead shrinks,
but slowly: at 200M rows it is still 3.7 times slower than Polars on six cores.

Memory at 200M rows: DuckDB 3.2 GiB, Polars 3.6 GiB, Scala Spark 7.1 GiB, PySpark 7.6 GiB (each
container is limited to 8 GiB). DataFusion was killed by the kernel (exit 137) at the 8 GiB limit,
from Parquet and from Iceberg; at 100M rows it had peaked at 6.5 GiB.

Running a single query repeatedly in one DataFusion session showed why: resident memory grows
with each run instead of returning to a baseline (Q1 at 200M rows: 0.9, 1.7, 2.5, 3.3 GiB after
runs 1 to 4; Q3 reached 7.2 to 7.4 GiB on its fourth run). Setting `MALLOC_ARENA_MAX=2` or
`MALLOC_TRIM_THRESHOLD_=0`, or a 4 GiB fair spill pool, did not change it. The cause was not
isolated further in the first round.

Update 2026-11-23: the growth comes from the mimalloc allocator bundled with datafusion-python
(per-thread heaps of tokio's I/O threads keep the freed read buffers). With
`MIMALLOC_TARGET_SEGMENTS_PER_THREAD=2`, now the benchmark default, DataFusion finishes 200M rows
in 5.532 s with a 2.7 GiB peak; the DataFusion column above is from that re-run (10M to 100M within
noise of the first round). See [ADR 0007](0007-datafusion-memory-and-mimalloc.md).

## Decision

- A single node with 6 cores and 8 GiB handles 200M rows (4 GiB of compressed Parquet) of this
  kind of workload with DuckDB or Polars in under 5 seconds for all six queries, using less than
  half the memory. Size the single-node path by the data a job reads after pruning, with at
  least the measured peak plus 50% headroom.
- Do not run long-lived DataFusion sessions near their container limit without watching resident
  memory. (Superseded by ADR 0007: a new session per job does not help; cap mimalloc's per-thread
  segments instead.)
- This does not move the point at which Spark is needed: that depends on data that does not fit
  one node, shuffles across nodes and fault tolerance, none of which a single machine tests. The
  roadmap item (a 3-node cluster at 500M+ rows) remains open.

## Consequences

- `make gen-large` adds 6.2 GiB of data; the full benchmark (five engines, four sizes, the
  Iceberg writes and reads) took about 17 minutes on this laptop.
- Results beyond 200M rows on one node are not claimed; the trend above should not be extrapolated
  to a cluster.
