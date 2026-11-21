# ADR 0003: Python in Spark is fine; Python UDFs are the cost

- Status: accepted
- Date: 2026-11-21

## Context

A common claim is that PySpark is slow and production jobs should be rewritten in Scala. The
benchmark measured the same five SQL queries from both languages, and one derived column
(a revenue tier) computed four ways.

## Measurements (median seconds, Spark 3.5.5, 6 CPU / 8 GiB)

| | 10M rows | 50M rows |
|---|---:|---:|
| SQL queries, PySpark (sum of 5) | 1.32 | 3.73 |
| SQL queries, Scala (sum of 5) | 1.44 | 3.93 |
| Revenue tier as a built-in `CASE` expression | 0.10 | 0.27 |
| ... as a Scala UDF | 0.19 | 0.66 |
| ... as an Arrow (pandas) UDF | 0.49 | 1.86 |
| ... as a row-at-a-time Python UDF | 1.46 | 6.61 |

## Decision

1. Write Spark jobs in whichever language the team maintains best; for DataFrame and SQL code
   the plan runs in the JVM either way, and PySpark matched Scala here.
2. Express logic with built-in functions first.
3. If custom code is needed in Python, use Arrow (pandas) UDFs, never row-at-a-time UDFs:
   3.6 times faster than the Python UDF at 50M rows.
4. Move a UDF to Scala/Java only when it is hot and cannot be expressed with built-ins.

## Consequences

- Code review should flag `@udf` without Arrow in PySpark jobs.
- Rewrites from Python to Scala need a measured reason; the language alone is not one.
