-- One SQL text per query, run unchanged by Spark, Polars, DataFusion and DuckDB.
-- Q6 filters one week of 30: on an Iceberg table partitioned by day it can skip 23 of 30 partitions.
-- name: q1_filter_agg
SELECT call_type, count(*) AS n, sum(bytes_mb) AS mb, avg(duration_s) AS avg_dur
FROM calls WHERE duration_s > 30 GROUP BY call_type ORDER BY call_type;
-- name: q2_high_card_groupby
SELECT subscriber_id, sum(revenue_try) AS revenue, count(*) AS n
FROM calls GROUP BY subscriber_id ORDER BY revenue DESC, subscriber_id LIMIT 100;
-- name: q3_join_daily
SELECT c.region, c.technology, CAST(f.start_ts AS DATE) AS day, sum(f.bytes_mb) AS mb, count(*) AS n
FROM calls f JOIN cells c ON f.cell_id = c.cell_id
GROUP BY c.region, c.technology, CAST(f.start_ts AS DATE) ORDER BY c.region, c.technology, day;
-- name: q4_distinct
SELECT c.region, count(DISTINCT f.subscriber_id) AS subscribers
FROM calls f JOIN cells c ON f.cell_id = c.cell_id GROUP BY c.region ORDER BY c.region;
-- name: q5_window_topn
SELECT region, cell_id, mb FROM (
  SELECT c.region, t.cell_id, t.mb,
         row_number() OVER (PARTITION BY c.region ORDER BY t.mb DESC, t.cell_id) AS rn
  FROM (SELECT cell_id, sum(bytes_mb) AS mb FROM calls GROUP BY cell_id) t
  JOIN cells c ON t.cell_id = c.cell_id) r
WHERE rn <= 3 ORDER BY region, mb DESC;
-- name: q6_one_week
SELECT c.region, count(*) AS n, sum(f.revenue_try) AS revenue
FROM calls f JOIN cells c ON f.cell_id = c.cell_id
WHERE f.start_ts >= TIMESTAMP '2026-10-08 00:00:00' AND f.start_ts < TIMESTAMP '2026-10-15 00:00:00'
GROUP BY c.region ORDER BY c.region;
