"""Benchmark setup diagram (Pillow)."""
from mfviz import GREEN, GREY, ICE, IMG, ORANGE, PURPLE, RED, WHITE, YELLOW, arrow, box, canvas, font


def setup():
    im, d = canvas(1600, 900)
    d.text((60, 40), "MasterFabric Spark Rust Bench · setup", font=font(34, 800), fill=WHITE)
    d.text((60, 86), "Same Parquet files, same SQL text, same container limits; every engine's rows checked against Spark's.",
           font=font(20, 450), fill=GREY)
    box(d, (50, 330, 380, 600), "Synthetic Parquet", ["calls: 10M / 50M rows", "  207 / 1,038 MiB, Snappy", "cells: 20,000 rows",
                                                       "skewed subscribers"], GREEN, line_size=18)
    box(d, (470, 300, 770, 630), "queries.sql", ["Q1 filter + aggregate", "Q2 high-card group-by", "Q3 join + daily",
                                                  "Q4 count distinct", "Q5 window top-N", "+ Spark UDF variants"], YELLOW, line_size=18)
    y = 140
    for title, lines, col in (("Spark 3.5.5 · PySpark", ["local[6], driver 6 GiB, JDK 17"], ORANGE),
                              ("Spark 3.5.5 · Scala spark-shell", ["same session settings"], YELLOW),
                              ("Polars 2.1.0 (Rust)", ["lazy scan + SQLContext"], ICE),
                              ("DataFusion 55.0.0 (Rust)", ["SessionContext, partitions = CPUs"], PURPLE)):
        box(d, (880, y, 1320, y + 130), title, lines, col, title_size=22, line_size=17)
        arrow(d, (770, 465), (880, y + 65), col)
        y += 160
    d.text((1345, 160), "each in its own", font=font(18, 600), fill=GREY)
    d.text((1345, 186), "container:", font=font(18, 600), fill=GREY)
    d.text((1345, 222), "--cpus 6", font=font(20, 700), fill=WHITE)
    d.text((1345, 252), "--memory 8g", font=font(20, 700), fill=WHITE)
    d.text((1345, 300), "peak memory:", font=font(18, 600), fill=GREY)
    d.text((1345, 326), "cgroup", font=font(18, 600), fill=GREY)
    d.text((1345, 352), "memory.peak", font=font(18, 600), fill=GREY)
    box(d, (1345, 560, 1560, 770), "collect.py", ["rows equal?", "12/12 runs", "5/5 queries"], RED, title_size=22, line_size=17)
    arrow(d, (1320, 690), (1345, 690), RED)
    arrow(d, (380, 465), (470, 465), GREEN)
    d.text((60, 840), "Reference rows: Polars and DataFusion also run on the host with all 14 cores and no limits.", font=font(18, 500), fill=GREY)
    im.save(IMG / "setup.png", optimize=True)


if __name__ == "__main__":
    setup()
    print("diagram written")
