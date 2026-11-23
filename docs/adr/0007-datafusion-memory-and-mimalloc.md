# ADR 0007: DataFusion's memory growth comes from mimalloc's per-thread segments

- Status: accepted
- Date: 2026-11-23
- Updates: [ADR 0006](0006-scaling-to-200m-rows.md)

## Context

In the first round DataFusion 55 was killed (exit 137) at the 8 GiB container limit at 200M rows,
from Parquet and from Iceberg. Resident memory grew with every repeat of a query in one session.
ADR 0006 recorded this as not isolated.

`make df-memory` (`bench/df_memory.py`) runs the six queries four times each in one process at
200M rows, in the same container (6 CPU, 8 GiB), and records resident memory and thread count after
every run. Eight variants, results in `results/df_memory_200m.json`:

| Variant | Outcome | Highest resident memory |
|---|---|---:|
| Default (as in the benchmark) | killed on Q3, run 3 | 6.1 GiB before the kill |
| `MALLOC_ARENA_MAX=2` (glibc) | killed on Q3, run 3 | 7.0 GiB |
| 4 GiB fair spill pool | killed on Q3, run 3 | 6.9 GiB |
| New `SessionContext` for every run | killed on Q3, run 3 | 6.8 GiB |
| `MIMALLOC_PURGE_DELAY=0` | killed on Q3, run 3 | 6.5 GiB |
| `MIMALLOC_PURGE_DELAY=0` + 4 GiB pool | killed on Q3, run 3 | 6.5 GiB |
| `MIMALLOC_TARGET_SEGMENTS_PER_THREAD=2` | all 24 runs finish | 3.0 GiB |
| 12 s pause between runs | all 24 runs finish, 7 threads instead of 44 | 1.4 GiB |

What the evidence points to:

- glibc settings change nothing because the datafusion-python wheel bundles **mimalloc**; glibc's
  allocator is not the one in use.
- A spill pool and a new session change nothing because the memory is not held by DataFusion's
  operators or session state.
- Memory added per run matches the compressed bytes of the column read
  (`bench/df_column_probe.py`, `results/df_column_probe_200m.json`): `sum(duration_s)` adds
  0.29 GiB per run for 0.25 GiB of compressed column, `sum(bytes_mb)` 0.48 for 0.41, `sum(call_id)`
  0.81 for 0.80. That is the I/O buffer traffic.
- The Parquet reads run on tokio's blocking I/O threads. Each new thread gets its own mimalloc
  heap with its own segments; freed pages stay in those segments while the threads are alive.
  After about 10 s idle tokio stops the threads, which is why a 12 s pause keeps memory flat
  (threads 44 → 7). A 50M-row plateau test showed the memory is eventually returned.
- Capping mimalloc's cached segments per thread (`MIMALLOC_TARGET_SEGMENTS_PER_THREAD=2`) makes
  each thread return memory sooner, without waiting for the thread to exit.

The same growth was seen with datafusion-python 48, 52 and 54.1. The mechanism above (thread
heaps holding freed I/O buffers) is the explanation that fits all eight variants; it was not
traced inside mimalloc itself.

## Decision

- DataFusion runs in this benchmark with `MIMALLOC_TARGET_SEGMENTS_PER_THREAD=2` by default
  (`bench/run_all.sh`; `DF_MIMALLOC_SEGMENTS=0` restores the old behaviour). It is an environment
  variable read by the bundled allocator, not a query or engine setting, so the fairness rules in
  ADR 0001 still hold: no engine gets per-query tuning.
- For DataFusion services that run many queries in one process, set this variable (or an
  equivalent cap) and watch resident memory under a realistic query mix before sizing the
  container. A new session per job does not help.

## Consequences

- DataFusion finishes 200M rows: sum of six medians 5.532 s, peak container memory 2,776 MiB
  (it was killed at 8,192 MiB). Peak memory at smaller sizes drops too: 10M 679 vs 1,653 MiB,
  50M 1,080 vs 3,575 MiB, 100M 1,841 vs 6,652 MiB. Speed is unchanged within noise
  (0.301 / 1.647 / 2.838 s vs 0.293 / 1.632 / 2.896 s).
- All 240 query results now match Spark's over 39 runs.
- The Iceberg read at 200M rows with DataFusion was not re-run (the 200M Iceberg tables were
  removed to save disk) and stays recorded as killed.
- The first-round raw files are kept in `results/raw/default_allocator/`.
