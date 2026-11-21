"""Charts from results/summary.json."""
from __future__ import annotations

import json

from mfviz import HEX, IMG, RESULTS, mpl_setup

plt = mpl_setup()
S = json.loads((RESULTS / "summary.json").read_text())
E = S["engines"]
Q = ["q1_filter_agg", "q2_high_card_groupby", "q3_join_daily", "q4_distinct", "q5_window_topn"]
QL = ["Q1 filter+agg", "Q2 high-card\ngroup-by", "Q3 join by day", "Q4 distinct", "Q5 window top-N"]
ENG = [("pyspark", "Spark (PySpark)", HEX["ORANGE"]), ("scala", "Spark (Scala)", HEX["YELLOW"]),
       ("polars", "Polars", HEX["ICE"]), ("datafusion", "DataFusion", HEX["PURPLE"])]


def query_times():
    fig, axes = plt.subplots(1, 2, figsize=(16, 5))
    for a, size in zip(axes, ("10m", "50m")):
        for j, (e, lab, col) in enumerate(ENG):
            r = E.get(f"{e}_{size}")
            if not r:
                continue
            xs = [i + (j - 1.5) * 0.2 for i in range(len(Q))]
            vals = [r["queries"][q]["median_s"] * 1000 for q in Q]
            a.bar(xs, vals, 0.19, color=col, label=lab)
        a.set_yscale("log"); a.set_xticks(range(len(Q)), QL, fontsize=9)
        a.set_ylabel("median query time (ms, log)")
        a.set_title(f"{size.upper().replace('M', 'M rows')} · each engine in a container, 6 CPU / 8 GiB", loc="left")
    axes[0].legend(fontsize=9)
    fig.tight_layout(); fig.savefig(IMG / "query_times.png"); plt.close(fig)


def overhead():
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(15, 4.6))
    for j, size in enumerate(("10m", "50m")):
        names = [lab for _, lab, _ in ENG]
        xs = [i + (j - 0.5) * 0.38 for i in range(len(ENG))]
        wall = [E[f"{e}_{size}"]["process_wall_s"] for e, _, _ in ENG]
        mem = [E[f"{e}_{size}"]["peak_mem_mib"] / 1024 for e, _, _ in ENG]
        cols = [c for _, _, c in ENG]
        a1.bar(xs, wall, 0.36, color=cols, alpha=0.55 if j == 0 else 1.0, label=size)
        a2.bar(xs, mem, 0.36, color=cols, alpha=0.55 if j == 0 else 1.0, label=size)
        for x, v in zip(xs, wall):
            a1.text(x, v + 0.3, f"{v:.1f}", ha="center", fontsize=8)
        for x, v in zip(xs, mem):
            a2.text(x, v + 0.05, f"{v:.1f}", ha="center", fontsize=8)
    for a in (a1, a2):
        a.set_xticks(range(len(ENG)), names)
    a1.set_ylabel("seconds"); a1.set_title("Whole run incl. container and engine start (light 10M, solid 50M); Spark runs include UDF queries", loc="left", fontsize=10)
    a2.set_ylabel("GiB"); a2.set_title("Peak container memory (cgroup memory.peak)", loc="left", fontsize=10)
    fig.tight_layout(); fig.savefig(IMG / "overhead.png"); plt.close(fig)


def udf():
    fig, a = plt.subplots(figsize=(11, 4.6))
    rows = [("udf_builtin", "pyspark", "built-in CASE", HEX["GREEN"]), ("udf_tier_scala", "scala", "Scala UDF", HEX["YELLOW"]),
            ("udf_tier_arrow", "pyspark", "Arrow (pandas) UDF", HEX["ICE"]), ("udf_tier_py", "pyspark", "Python UDF", HEX["RED"])]
    for j, size in enumerate(("10m", "50m")):
        for i, (q, e, lab, col) in enumerate(rows):
            v = E[f"{e}_{size}"]["queries"][q]["median_s"]
            x = i + (j - 0.5) * 0.38
            a.bar(x, v, 0.36, color=col, alpha=0.55 if j == 0 else 1.0)
            a.text(x, v + 0.05, f"{v:.2f}s\n{size}", ha="center", fontsize=8)
    a.set_xticks(range(len(rows)), [r[2] for r in rows]); a.set_ylabel("median seconds")
    a.set_title("Spark: the same revenue-tier column four ways (light 10M, solid 50M rows)", loc="left")
    fig.tight_layout(); fig.savefig(IMG / "udf.png"); plt.close(fig)


if __name__ == "__main__":
    IMG.mkdir(parents=True, exist_ok=True)
    for f in (query_times, overhead, udf):
        f()
    print("charts written")
