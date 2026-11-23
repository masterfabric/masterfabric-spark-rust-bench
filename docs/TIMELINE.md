# Build timeline

Build history of MasterFabric Spark Rust Bench, generated from the git log. Times are Europe/Istanbul (UTC+3).

Every step below is one commit, in order. Times are Europe/Istanbul (UTC+3).

| # | Commit | Time | Change |
|---|---|---|---|
| 1 | `b14b736` | 2026-11-21 05:14 | chore: scaffold repository with AGPL-3.0 license and community files |
| 2 | `131b9cc` | 2026-11-21 05:18 | feat(data): synthetic telco usage generator (Parquet) |
| 3 | `2af7b98` | 2026-11-21 05:18 | feat(bench): five SQL queries shared by every engine and a timing helper |
| 4 | `c57f84c` | 2026-11-21 05:18 | feat(bench): Polars and DataFusion runners and their container image |
| 5 | `77d443b` | 2026-11-21 05:18 | feat(bench): PySpark and Scala Spark runners with UDF variants |
| 6 | `3f15b6f` | 2026-11-21 05:18 | feat(bench): run all engines in containers with equal limits |
| 7 | `c5445f6` | 2026-11-21 05:21 | feat(bench): collect runs and check every engine returns the same rows |
| 8 | `57f0bf4` | 2026-11-21 05:21 | test: Polars and DataFusion agree on every query; comparison tolerances |
| 9 | `cb38bce` | 2026-11-21 05:22 | results: 10M and 50M rows, four engines in containers plus host reference |
| 10 | `a674d40` | 2026-11-21 05:22 | docs: charts for query times, run overhead and memory, and Spark UDFs |
| 11 | `fca1ccf` | 2026-11-21 05:23 | docs: benchmark setup diagram |
| 12 | `ecf5739` | 2026-11-21 05:24 | docs: hero image built from the measured results |
| 13 | `f52e27f` | 2026-11-21 05:24 | build: pinned host requirements and Makefile |
| 14 | `4e668ca` | 2026-11-21 05:25 | docs: contributing guide |
| 15 | `8eee374` | 2026-11-21 05:25 | docs(adr): 0001 how the benchmark stays fair |
| 16 | `23f8084` | 2026-11-21 05:25 | docs(adr): 0002 when to use Spark and when a single-node Rust engine |
| 17 | `6f1f78b` | 2026-11-21 05:25 | docs(adr): 0003 Python in Spark is fine; Python UDFs are the cost |
| 18 | `2ebb847` | 2026-11-21 05:25 | docs(adr): correct two figures in ADR 0002 |
| 19 | `382a17c` | 2026-11-21 05:26 | docs: README with results, method, ADR links, enterprise mapping and limitations |

The final commit adds this file and the README; see `git log` for the full messages, which explain
the why of each step (including the fixes found by running the system).


## Addition: DuckDB, Iceberg reads and scaling to 200M rows (2026-11-21)

Started from `aeac1b1`. Times are Europe/Istanbul. New dependencies: DuckDB 2.0.1 and its iceberg
extension (MIT), PyIceberg 0.12.0 (Apache 2.0) with its dependencies, SQLAlchemy (MIT, tests only);
all listed in NOTICE.

| # | Commit | Time | Change |
|---|---|---|---|
| 1 | `88fe31d` | 2026-11-21 14:15 | feat(bench): DuckDB as a fifth engine and an Iceberg read mode for the single-node engines |
| 2 | `11f745d` | 2026-11-21 14:15 | feat(bench): Q6, one week of call records joined to cells |
| 3 | `4465930` | 2026-11-21 14:15 | feat(bench): write the datasets as Iceberg tables with Spark, and read them from PySpark |
| 4 | `2992b75` | 2026-11-21 14:17 | feat(bench): engines image with DuckDB and PyIceberg; run_all keeps going after a failed run |
| 5 | `e301989` | 2026-11-21 14:18 | test: Polars, DataFusion and DuckDB agree on all six queries from Parquet and from Iceberg |
| 6 | `b2b0607` | 2026-11-21 14:41 | feat(bench): optional bounded memory pool for DataFusion (DF_MEMORY_POOL_GIB) |
| 7 | `9a9a338` | 2026-11-21 14:41 | feat(bench): Spark Q6 with TIMESTAMP_NTZ literals, checked against Q6 |
| 8 | `ad7bb98` | 2026-11-21 14:52 | fix(collect): keep only the table lines of the Iceberg write log |
| 9 | `a878446` | 2026-11-21 14:52 | results: five engines from 10M to 200M rows, Iceberg reads at 50M and 200M |
| 10 | `0f8017d` | 2026-11-21 14:52 | docs: charts for scaling to 200M rows and Iceberg reads; DuckDB in every chart |
| 11 | `f18c4be` | 2026-11-21 14:52 | docs: setup diagram with DuckDB, the Iceberg tables and run counts read from the results |
| 12 | `a2be82a` | 2026-11-21 14:54 | docs: hero with 200M-row results, the Iceberg Q6 numbers and DuckDB |
| 13 | `3de5a4b` | 2026-11-21 14:55 | docs(adr): 0004 DuckDB, 0005 Iceberg reads from single-node engines, 0006 scaling to 200M rows |
| 14 | `d637823` | 2026-11-21 14:55 | docs: NOTICE lists DuckDB, its iceberg extension, PyIceberg with its dependencies, Iceberg and SQLAlchemy; ADR 0001 updated for six queries, DuckDB and recorded failures |
| 15 | `f0da6bf` | 2026-11-21 14:57 | README: DuckDB, Iceberg reads and scaling to 200M rows, with figures 1-6, failures and limitations |
| 16 | (this commit) | | docs(timeline): this section |

### Verification (measured on the M4 Pro, Colima 6 vCPU / 10 GiB)

| Check | Result |
|---|---|
| Unit tests | 9 passed: three engines agree on six queries from Parquet and from a two-snapshot Iceberg table |
| Correctness gate | 234 of 234 query results identical to Spark's over 38 runs (10M to 200M rows, Parquet and Iceberg) |
| Full benchmark | 14:20 to 14:37 (five engines, four sizes, Iceberg writes and reads); PySpark repeated 14:41 to 14:50 for the NTZ Q6 |
| 200M rows, sum of six medians | Polars 4.325 s, DuckDB 4.777 s, PySpark 15.975 s, Scala 15.922 s; DataFusion killed (exit 137) |
| Iceberg Q6, 200M rows | plan keeps 20 of 92 files; DuckDB 0.693 → 0.314 s; PySpark NTZ literal 2.989 → 0.742 s |
| Machine state | containers removed after each run (`--rm`); Colima left running, k3d `mf-llm` left stopped; ports 3000, 8081, 11234 untouched |

### Found while running it

- `pyiceberg-core` 0.11.0, the native Iceberg table provider for DataFusion, is built against
  DataFusion 54 and refuses to load into DataFusion 55. DataFusion reads the files PyIceberg plans
  instead, at twice the peak memory of the Parquet folder at 50M rows.
- DataFusion was killed at the 8 GiB limit at 200M rows. Repeating one query in one session
  showed resident memory growing run after run (Q1: 0.9, 1.7, 2.5, 3.3 GiB); `MALLOC_ARENA_MAX=2`,
  `MALLOC_TRIM_THRESHOLD_=0` and a 4 GiB fair spill pool changed nothing. The pool option stays in
  the runner; the cause is not isolated.
- Spark did not prune the Iceberg partitions for Q6. The Parquet timestamps have no zone, so
  Spark reads `start_ts` as `TIMESTAMP_NTZ`; the portable `TIMESTAMP` literal made it cast the
  column, and EXPLAIN showed only `start_ts IS NOT NULL` pushed to the scan. A `TIMESTAMP_NTZ`
  literal fixes it; Spark now also runs that variant and it is checked against Q6.
- DuckDB read the container's cgroup limit by itself (`memory_limit` 6.3 GiB in an 8 GiB
  container, 7.7 GiB without a limit).
- Polars printed a warning that its faster Iceberg planner needs the separate `polars_iceberg`
  package; the runner sets `POLARS_ICEBERG_PLANNER=pyiceberg` and uses PyIceberg's planner.
- The tabulario Spark image's defaults point at a REST catalog and MinIO that do not exist here;
  the Iceberg runs use their own Hadoop catalog (`lab`) and keep `spark_catalog` as the default.
- The Polars/DataFusion image is now `mf-srb/engines:2` (Dockerfile.engines), since DuckDB is
  C++, not Rust. The old `mf-srb/rust-engines:1` image was left in place.

## Addition: DataFusion's memory at 200M rows explained and fixed; NTZ pruning reproduction (2026-11-23)

Two open items from the 2026-11-21 round: why DataFusion was killed at 200M rows, and a
standalone reproduction of the Spark Iceberg pruning finding. Times are Europe/Istanbul.

| Commit | Time | What |
|---|---|---|
| `6160e9e` | 2026-11-23 13:17 | diagnosis: DataFusion memory across repeated queries at 200M rows; spill pool, memory limit, glibc and per-run sessions do not stop it, mimalloc segment target and idle-thread pauses do |
| `8e0e2b8` | 2026-11-23 13:17 | bench: DOCKER_ENV passes extra environment to the benchmark containers |
| `70e62e1` | 2026-11-23 13:17 | results: DataFusion re-run at all sizes with MIMALLOC_TARGET_SEGMENTS_PER_THREAD=2; 200M finishes (5.532 s, 2.7 GiB), all results match |
| `ef5b40e` | 2026-11-23 13:17 | example: self-contained reproduction of the Spark TIMESTAMP vs TIMESTAMP_NTZ pruning finding (30 vs 7 files) |
| `dc5ceb4` | 2026-11-23 13:20 | diagnosis: memory added per DataFusion run matches the compressed bytes of the columns read (0.29/0.48/0.81 GiB vs 0.25/0.41/0.80) |
| `78175b3` | 2026-11-23 13:20 | bench: DataFusion runs with MIMALLOC_TARGET_SEGMENTS_PER_THREAD=2 by default (DF_MIMALLOC_SEGMENTS=0 for the old behaviour); make df-memory, make ntz-repro |
| `51e0123` | 2026-11-23 13:20 | charts: DataFusion memory per run and per column, NTZ pruning repro; query, scaling, overhead and Iceberg charts regenerated with DataFusion at 200M |
| `2170ae3` | 2026-11-23 13:23 | docs(adr): 0007 DataFusion memory growth comes from mimalloc's per-thread segments; 0006, 0004, 0001 updated with the 200M re-run; NTZ example README |
| `a839b43` | 2026-11-23 13:23 | README: DataFusion at 200M rows (figure 7), the NTZ pruning reproduction (figure 8), updated tables and limitations; hero and setup diagram from the new results |
| (this commit) | 2026-11-23 | docs(timeline): DataFusion memory and the NTZ reproduction |

### Verification

| Check | Result |
|---|---|
| Diagnosis (`make df-memory`) | 8 variants × 24 query runs at 200M rows in the 6 CPU / 8 GiB container; 6 killed on Q3 run 3 (glibc arena, spill pool, session per run, mimalloc purge delay), 2 finish (`MIMALLOC_TARGET_SEGMENTS_PER_THREAD=2` at ≤ 3.0 GiB, 12 s pause at ≤ 1.4 GiB) |
| Column probe | memory added per run vs compressed column: 0.294 vs 0.249, 0.483 vs 0.408, 0.806 vs 0.797 GiB |
| DataFusion re-run, all sizes | 200M: 5.532 s, peak 2,776 MiB (was killed at 8,192); 10M/50M/100M peak 679/1,080/1,841 MiB (was 1,653/3,575/6,652); sums within 2% of the first round |
| Correctness gate | 240 of 240 query results identical to Spark's over 39 runs; one recorded failure left (DataFusion on the 200M Iceberg table, not re-run) |
| NTZ reproduction (`make ntz-repro`) | 30-file table: TIMESTAMP literal reads 30 files (188 ms); TIMESTAMP_NTZ literal and session `timestampType=TIMESTAMP_NTZ` read 7, skip 23 (44 / 42 ms); 1,400,000 rows each time |
| Unit tests | 9 passed |
| Machine state | containers run with `--rm`; data for 100M/200M deleted after the run; k3d `mf-llm` stopped as found; ports 3000, 8081, 11234 untouched |

### Found while running it

- glibc allocator settings could never have helped: the datafusion-python wheel bundles
  mimalloc. The first round's `MALLOC_ARENA_MAX` test was testing the wrong allocator.
- A new `SessionContext` per run does not help either, which rules out the session and its
  caches. ADR 0006's advice to prefer a new session per job is superseded by ADR 0007.
- The growth per run tracks the compressed bytes read, and thread counts fall from 44 to 7 when
  runs are 12 s apart (tokio retires idle blocking threads after about 10 s). Freed read buffers
  stay in the per-thread heaps of live I/O threads. A 50M-row plateau test showed they are
  eventually returned, so it is retention, not a leak.
- The same growth appears with datafusion-python 48, 52 and 54.1, so it is not new in 55.
- The diagnosis files (`results/df_memory_*`) were measured on 100M/200M data generated inside
  the engines container. Its Q4 result differed from the benchmark data in a handful of rows
  (different numpy/pyarrow), which does not affect memory; the benchmark re-run used data
  regenerated on the host with the pinned versions, which matches the original files.
- The Spark image defaults to a REST catalog that is not running here; the reproduction sets
  `spark.sql.defaultCatalog=spark_catalog` and uses `--entrypoint python3`.
- A background job on this laptop removed every `.venv` under `~/Documents/GitHub` during the
  work; the runs used a temporary venv outside the repository instead.
- The DataFusion Iceberg read at 200M rows was not re-run: the 200M Iceberg tables had been
  removed to save disk, and rebuilding them takes Spark about 4 minutes and 4.3 GiB.
