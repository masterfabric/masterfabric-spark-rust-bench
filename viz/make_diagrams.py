"""Benchmark setup diagram (Pillow). Counts are read from results/summary.json."""
import json

from mfviz import GREEN, GREY, ICE, IMG, ORANGE, PURPLE, RED, RESULTS, WHITE, YELLOW, arrow, box, canvas, font


def setup():
    s = json.loads((RESULTS / "summary.json").read_text())
    runs = len(s["checks"])
    ok_runs = sum(all(v.values()) for v in s["checks"].values())
    fails = len(s.get("failures", {}))
    im, d = canvas(1600, 960)
    d.text((60, 40), "MasterFabric Spark Rust Bench · setup", font=font(34, 800), fill=WHITE)
    d.text((60, 86), "Same data (Parquet folder or Iceberg table), same SQL text, same container limits; every run's rows checked against Spark's.",
           font=font(20, 450), fill=GREY)
    box(d, (40, 250, 390, 560), "Synthetic Parquet", ["calls: 10M / 50M / 100M / 200M", "  0.2 to 4.1 GiB, Snappy", "cells: 20,000 rows",
                                                       "skewed subscribers"], GREEN, line_size=18)
    box(d, (40, 610, 390, 860), "Iceberg tables (Spark)", ["calls 50M / 200M, by day(start_ts)", "30 partitions", "Hadoop catalog: plain files",
                                                            "read by every engine"], ICE, line_size=18)
    arrow(d, (215, 560), (215, 610), ICE)
    box(d, (470, 330, 770, 700), "queries.sql", ["Q1 filter + aggregate", "Q2 high-card group-by", "Q3 join + daily",
                                                  "Q4 count distinct", "Q5 window top-N", "Q6 one week of 30 days", "+ Spark UDF variants"],
        YELLOW, line_size=18)
    y = 135
    for title, lines, col in (("Spark 3.5.5 · PySpark", ["local[6], driver 6 GiB; Iceberg 1.8.1"], ORANGE),
                              ("Spark 3.5.5 · Scala spark-shell", ["same session settings"], YELLOW),
                              ("Polars 2.1.0 (Rust)", ["lazy scan / scan_iceberg + SQLContext"], ICE),
                              ("DataFusion 55.0.0 (Rust)", ["Parquet listing; Iceberg via PyIceberg plan"], PURPLE),
                              ("DuckDB 2.0.1 (C++)", ["read_parquet / iceberg_scan views"], GREEN)):
        box(d, (860, y, 1320, y + 118), title, lines, col, title_size=22, line_size=17)
        arrow(d, (770, 515), (860, y + 59), col)
        y += 140
    for i, (t, w) in enumerate((("each in its own", 600), ("container:", 600), ("--cpus 6", 700), ("--memory 8g", 700))):
        d.text((1345, 150 + i * 30), t, font=font(18 if w == 600 else 20, w), fill=GREY if w == 600 else WHITE)
    d.text((1345, 290), "peak memory:", font=font(18, 600), fill=GREY)
    d.text((1345, 316), "cgroup memory.peak", font=font(18, 600), fill=GREY)
    d.text((1345, 352), "a killed run is", font=font(18, 600), fill=GREY)
    d.text((1345, 378), "recorded, not hidden", font=font(18, 600), fill=GREY)
    box(d, (1345, 600, 1570, 840), "collect.py", ["rows equal?", f"{ok_runs}/{runs} runs", "6/6 queries each", f"{fails} run{'' if fails == 1 else 's'} killed"],
        RED, title_size=22, line_size=17)
    arrow(d, (1320, 720), (1345, 720), RED)
    arrow(d, (390, 405), (470, 470), GREEN)
    arrow(d, (390, 735), (470, 600), ICE)
    d.text((60, 900), "Reference rows: Polars, DataFusion and DuckDB also run on the host with all 14 cores and no limits.", font=font(18, 500), fill=GREY)
    im.save(IMG / "setup.png", optimize=True)


if __name__ == "__main__":
    setup()
    print("diagram written")
