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
