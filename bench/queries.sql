-- One SQL text per query, run unchanged by Spark, DataFusion and Polars.
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
