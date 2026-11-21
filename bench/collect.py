"""Collect raw runs into results/summary.json and check that every engine returned the same rows."""
from __future__ import annotations

import json
import math
from pathlib import Path

RAW, OUT = Path("results/raw"), Path("results")
ENGINES = ["polars", "datafusion", "pyspark", "scala", "host_polars", "host_datafusion"]
SQL_QUERIES = ["q1_filter_agg", "q2_high_card_groupby", "q3_join_daily", "q4_distinct", "q5_window_topn", "q6_one_week"]


def norm(v):
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    return str(v)[:10] if str(v)[:4].isdigit() and "-" in str(v) else str(v)  # dates as YYYY-MM-DD


def same(a, b, rel=1e-6):
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return False
        for x, y in zip(map(norm, ra), map(norm, rb)):
            if isinstance(x, float) and isinstance(y, float):
                if not math.isclose(x, y, rel_tol=rel, abs_tol=1e-6):
                    return False
            elif x != y:
                return False
    return True


def main():
    summary = {"engines": {}, "checks": {}}
    summary["failures"] = {}
    for f in sorted(RAW.glob("*.exit")):
        name = f.stem
        mem, wall = RAW / f"{name}.mem", RAW / f"{name}.wall"
        summary["failures"][name] = {"exit_code": int(f.read_text()),
                                     "peak_mem_mib": round(int(mem.read_text()) / 2**20) if mem.exists() else None,
                                     "process_wall_s": float(wall.read_text()) if wall.exists() else None}
    summary["iceberg"] = {f.stem.removeprefix("iceberg_plan_"): json.loads(f.read_text()) for f in sorted(RAW.glob("iceberg_plan_*.json"))}
    for f in sorted(RAW.glob("make_iceberg_*.log")):
        summary["iceberg"].setdefault(f.stem.removeprefix("make_iceberg_"), {})["write_log"] = f.read_text().strip()
    for f in sorted(RAW.glob("*.json")):
        name = f.stem
        if name.startswith("iceberg_plan_") or name in summary["failures"]:
            continue
        d = json.loads(f.read_text())
        mem = RAW / f"{name}.mem"
        wall = RAW / f"{name}.wall"
        summary["engines"][name] = {
            "engine": d["engine"], "version": d["version"], "size": d["size"], "format": d.get("format", "parquet"), "cpus": d["cpus"],
            "where": "host (no limits)" if name.startswith("host_") else "container (6 CPU, 8 GiB)",
            "startup_s": round(float(d["startup_s"]), 3),
            "process_wall_s": float(wall.read_text()) if wall.exists() else None,
            "peak_mem_mib": round(int(mem.read_text()) / 2**20) if mem.exists() else None,
            "queries": {q: {"median_s": round(float(v["median_s"]), 4), "first_s": round(float(v["first_s"]), 4)}
                        for q, v in d["queries"].items()},
            "sql_total_median_s": round(sum(float(d["queries"][q]["median_s"]) for q in SQL_QUERIES), 4),
        }
        summary["engines"][name]["_rows"] = {q: d["queries"][q]["rows"] for q in SQL_QUERIES}
    sizes = sorted({v["size"] for v in summary["engines"].values()}, key=lambda s: int(s.rstrip("m")))
    summary["sizes"] = sizes
    for size in sizes:
        ref_name = f"pyspark_{size}"
        ref = summary["engines"].get(ref_name)
        for name, e in summary["engines"].items():
            if e["size"] != size or ref is None:
                continue
            summary["checks"][name] = {q: same(e["_rows"][q], ref["_rows"][q]) for q in SQL_QUERIES}
        if ref is None:  # no Spark run at this size: check the others against each other instead
            names = [n for n, e in summary["engines"].items() if e["size"] == size]
            if names:
                base = summary["engines"][names[0]]
                for n in names:
                    summary["checks"][n] = {q: same(summary["engines"][n]["_rows"][q], base["_rows"][q]) for q in SQL_QUERIES}
    for e in summary["engines"].values():
        e.pop("_rows")
    summary["all_results_match"] = all(all(v.values()) for v in summary["checks"].values())
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2))
    for n, e in summary["engines"].items():
        print(f"{n:22} startup {e['startup_s']:7.2f}s wall {e['process_wall_s']}s mem {e['peak_mem_mib']} MiB sql_total {e['sql_total_median_s']:.3f}s")
    print("checks:", {k: all(v.values()) for k, v in summary["checks"].items()})
    print("failures:", summary["failures"])
    print("all results match:", summary["all_results_match"])


if __name__ == "__main__":
    main()
