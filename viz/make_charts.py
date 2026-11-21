"""Charts from results/summary.json."""
from __future__ import annotations

import json

from mfviz import HEX, IMG, RESULTS, mpl_setup

plt = mpl_setup()
S = json.loads((RESULTS / "summary.json").read_text())
E = S["engines"]
F = S.get("failures", {})
Q = ["q1_filter_agg", "q2_high_card_groupby", "q3_join_daily", "q4_distinct", "q5_window_topn", "q6_one_week"]
QL = ["Q1 filter+agg", "Q2 high-card\ngroup-by", "Q3 join by day", "Q4 distinct", "Q5 window top-N", "Q6 one week"]
ENG = [("pyspark", "Spark (PySpark)", HEX["ORANGE"]), ("scala", "Spark (Scala)", HEX["YELLOW"]),
       ("polars", "Polars", HEX["ICE"]), ("datafusion", "DataFusion", HEX["PURPLE"]), ("duckdb", "DuckDB", HEX["GREEN"])]
SIZES = S["sizes"]
ROWS_M = {s: int(s.rstrip("m")) for s in SIZES}


def query_times():
    fig, axes = plt.subplots(1, 2, figsize=(17, 5))
    for a, size in zip(axes, ("50m", "200m")):
        for j, (e, lab, col) in enumerate(ENG):
            r = E.get(f"{e}_{size}")
            xs = [i + (j - 2) * 0.16 for i in range(len(Q))]
            if not r:
                if f"{e}_{size}" in F:
                    a.text(0.99, 0.97, f"{lab}: killed at the 8 GiB limit", transform=a.transAxes, fontsize=9, color=col, ha="right", va="top")
                continue
            vals = [r["queries"][q]["median_s"] * 1000 for q in Q]
            a.bar(xs, vals, 0.15, color=col, label=lab)
        a.set_yscale("log"); a.set_xticks(range(len(Q)), QL, fontsize=9)
        a.set_ylabel("median query time (ms, log)")
        a.set_title(f"{size.upper().replace('M', 'M rows')} · each engine in a container, 6 CPU / 8 GiB", loc="left")
    h, lab = axes[0].get_legend_handles_labels()
    fig.legend(h, lab, loc="upper right", ncol=5, fontsize=9, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.93)); fig.savefig(IMG / "query_times.png"); plt.close(fig)


def overhead():
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 4.6))
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


def scaling():
    """Sum of the six query medians and peak memory from 10M to 200M rows; failed runs marked."""
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(16, 5.2))
    for e, lab, col in ENG:
        xs, tot, mem = [], [], []
        for s in SIZES:
            r = E.get(f"{e}_{s}")
            if r:
                xs.append(ROWS_M[s]); tot.append(r["sql_total_median_s"]); mem.append(r["peak_mem_mib"] / 1024)
            elif f"{e}_{s}" in F:
                f = F[f"{e}_{s}"]
                a2.scatter([ROWS_M[s]], [(f["peak_mem_mib"] or 0) / 1024], marker="X", s=140, color=col, zorder=5)
                a1.annotate(f"{lab}: failed\n(exit {f['exit_code']})", (ROWS_M[s], 0.3), color=col, fontsize=9, ha="center")
        a1.plot(xs, tot, "o-", color=col, label=lab, lw=2)
        a2.plot(xs, mem, "o-", color=col, label=lab, lw=2)
        if xs:
            a1.text(xs[-1] * 1.06, tot[-1], f"{tot[-1]:.1f} s", color=col, fontsize=9, va="center")
    for a in (a1, a2):
        a.set_xscale("log"); a.set_xticks([ROWS_M[s] for s in SIZES], [f"{ROWS_M[s]}M" for s in SIZES])
        a.set_xlabel("rows in calls"); a.set_xlim(8, 300)
    a1.set_yscale("log"); a1.set_ylabel("sum of six query medians (s, log)")
    a1.set_title("Query time as data grows · each engine in a container, 6 CPU / 8 GiB", loc="left", fontsize=11)
    a2.axhline(8, color=HEX["RED"], ls="--", lw=1); a2.text(9, 7.75, "container limit 8 GiB", color=HEX["RED"], fontsize=9, va="top")
    a2.set_ylabel("GiB"); a2.set_title("Peak container memory (cgroup memory.peak); X = run failed", loc="left", fontsize=11)
    a1.legend(fontsize=9)
    fig.tight_layout(); fig.savefig(IMG / "scaling.png"); plt.close(fig)


def iceberg():
    """The same queries on plain Parquet and on the Iceberg table (partitioned by day)."""
    sizes = [s for s in SIZES if any(f"iceberg_{e}_{s}" in E for e, _, _ in ENG)]
    eng = [e for e in ENG if e[0] != "scala"]
    fig, axes = plt.subplots(1, 2 * len(sizes), figsize=(8.5 * len(sizes), 5), gridspec_kw={"width_ratios": [4, 5] * len(sizes)})
    for k, s in enumerate(sizes):
        label = s.upper().replace("M", "M rows")
        for a, qs, title in ((axes[2 * k], Q[:5], f"Q1–Q5 sum (full scans), {label}"),
                             (axes[2 * k + 1], Q[5:], f"Q6 one week of 30 days, {label}")):
            cats = eng + ([("pyspark", "Spark, NTZ\nliteral", HEX["ORANGE"])] if qs == Q[5:] else [])
            for i, (e, lab, col) in enumerate(cats):
                qq = ["q6_one_week_ntz"] if "NTZ" in lab else qs
                for j, (pre, alpha) in enumerate((("", 0.45), ("iceberg_", 1.0))):
                    x = i + (j - 0.5) * 0.38
                    r = E.get(f"{pre}{e}_{s}")
                    if not r:
                        if f"{pre}{e}_{s}" in F:
                            a.text(x, 0.02 * a.get_ylim()[1], "killed at 8 GiB", ha="center", va="bottom", fontsize=8.5, color=HEX["RED"], rotation=90)
                        continue
                    v = sum(r["queries"][q]["median_s"] for q in qq)
                    a.bar(x, v, 0.36, color=col, alpha=alpha)
                    a.text(x, v * 1.01, f"{v:.2f}", ha="center", va="bottom", fontsize=7.5)
            a.set_xticks(range(len(cats)), [c[1] for c in cats], fontsize=8.5)
            a.set_title(title, loc="left", fontsize=10)
            a.set_ylabel("median seconds")
            a.margins(y=0.12)
    fig.suptitle("Light: folder of Parquet files · solid: Iceberg table partitioned by day(start_ts) · each engine in a 6 CPU / 8 GiB container",
                 x=0.01, ha="left", fontsize=11, color=HEX["GREY"])
    fig.tight_layout(); fig.savefig(IMG / "iceberg.png"); plt.close(fig)


if __name__ == "__main__":
    IMG.mkdir(parents=True, exist_ok=True)
    for f in (query_times, overhead, udf, scaling, iceberg):
        f()
    print("charts written")
