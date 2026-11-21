"""Compose docs/hero.png (1600x900) from real charts and measured numbers.

Every number on the hero is read from results/summary.json at render time.
"""

from __future__ import annotations

import json

from PIL import Image, ImageDraw, ImageFilter

from mfviz import BG, BG2, DOCS, GREEN, GREY, ICE, IMG, ORANGE, PURPLE, RED, RESULTS, WHITE, YELLOW, font

W, H = 1600, 900


def load(n):
    return json.loads((RESULTS / f"{n}.json").read_text())


def background():
    im = Image.new("RGB", (W, H), BG)
    px = im.load()
    for y in range(H):
        for x in range(W):
            t = min(1.0, (x / W) * 0.75 + (y / H) * 0.35)
            px[x, y] = tuple(int(BG[i] * (1 - t) + BG2[i] * t) for i in range(3))
    return im


def card(base, path, box, label, color, crop=None, fit=False):
    x0, y0, x1, y1 = box
    shot = Image.open(path).convert("RGB")
    if crop:
        shot = shot.crop(crop)
    if fit and shot.height * (x1 - x0) / shot.width > y1 - y0:  # whole image, scaled to the box height and centred
        w = round(shot.width * (y1 - y0) / shot.height)
        x0 += (x1 - x0 - w) // 2
        x1 = x0 + w
    shot = shot.resize((x1 - x0, round(shot.height * (x1 - x0) / shot.width)), Image.LANCZOS)
    shot = shot.crop((0, 0, x1 - x0, min(shot.height, y1 - y0)))
    shadow = Image.new("RGBA", (shot.width + 40, shot.height + 40), (0, 0, 0, 0))
    ImageDraw.Draw(shadow).rounded_rectangle((20, 20, shot.width + 20, shot.height + 20), radius=16, fill=(0, 0, 0, 170))
    shadow = shadow.filter(ImageFilter.GaussianBlur(12))
    base.paste(shadow, (x0 - 20, y0 - 12), shadow)
    mask = Image.new("L", shot.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, shot.width, shot.height), radius=14, fill=255)
    base.paste(shot, (x0, y0), mask)
    d = ImageDraw.Draw(base)
    f = font(20, 800)
    w = d.textlength(label, font=f)
    d.rounded_rectangle((x0 - 10, y0 - 22, x0 + w + 18, y0 + 14), radius=18, fill=color)
    d.text((x0 + 4, y0 - 18), label, font=f, fill=(255, 255, 255))


def half(path, right=True):
    w, h = Image.open(path).size
    return (w // 2, 0, w, h) if right else (0, 0, w // 2, h)


def main():
    s = load("summary")
    e = s["engines"]
    n = sum(len(v) for v in s["checks"].values())
    ok = sum(sum(v.values()) for v in s["checks"].values())
    im = background()
    card(im, IMG / "query_times.png", (960, 50, 1570, 380), "Per-query medians, 200M rows", ICE, crop=half(IMG / "query_times.png"))
    card(im, IMG / "scaling.png", (960, 430, 1570, 630), "10M to 200M rows: time and memory", PURPLE)
    card(im, IMG / "iceberg.png", (960, 680, 1570, 885), "Parquet vs Iceberg, 200M rows", GREEN, crop=half(IMG / "iceberg.png"), fit=True)
    d = ImageDraw.Draw(im)
    d.text((80, 70), "MASTERFABRIC", font=font(36, 800), fill=WHITE)
    d.text((76, 120), "Spark Rust Bench", font=font(80, 800), fill=WHITE)
    x, y = 80, 240
    for word, col in (("PySpark", ORANGE), (" · ", GREY), ("Scala", YELLOW), (" · ", GREY), ("Polars", ICE), (" · ", GREY),
                      ("DataFusion", PURPLE), (" · ", GREY), ("DuckDB", GREEN)):
        f = font(34, 800)
        d.text((x, y), word, font=f, fill=col)
        x += d.textlength(word, font=f)
    d.text((80, 312), "Same SQL; Parquet or Iceberg; equal 6 CPU / 8 GiB containers; 10M to 200M rows.", font=font(22, 500), fill=GREY)
    d.text((80, 344), f"{ok}/{n} query results identical to Spark's. Measured on one machine.", font=font(22, 500), fill=GREY)
    p, sc, po, du = e["pyspark_200m"], e["scala_200m"], e["polars_200m"], e["duckdb_200m"]
    ip, idu = e["iceberg_pyspark_200m"], e["iceberg_duckdb_200m"]
    plan = s["iceberg"]["200m"]
    q = lambda r, k: r["queries"][k]["median_s"]  # noqa: E731
    ratio = lambda size: e[f"pyspark_{size}"]["sql_total_median_s"] / e[f"polars_{size}"]["sql_total_median_s"]  # noqa: E731
    df_killed = "datafusion_200m" in s.get("failures", {})
    facts = [
        (f"200M rows, 6 queries: Polars {po['sql_total_median_s']:.2f} s · DuckDB {du['sql_total_median_s']:.2f} s",
         f"Spark {p['sql_total_median_s']:.1f} s (PySpark) / {sc['sql_total_median_s']:.1f} s (Scala)"
         + (" · DataFusion killed at 8 GiB" if df_killed else ""), ICE),
        (f"Spark's gap narrows with size: {ratio('10m'):.1f}x at 10M, {ratio('200m'):.1f}x at 200M",
         "PySpark vs Polars, sum of query medians; single-node Spark, no cluster", ORANGE),
        (f"Iceberg Q6 reads {plan['q6_one_week']['files']} of {plan['full_scan']['files']} files: DuckDB {q(du, 'q6_one_week'):.2f} → {q(idu, 'q6_one_week'):.2f} s",
         f"Spark prunes only with a TIMESTAMP_NTZ literal: {q(ip, 'q6_one_week'):.2f} → {q(ip, 'q6_one_week_ntz'):.2f} s", GREEN),
        (f"Peak memory at 200M: DuckDB {du['peak_mem_mib'] / 1024:.1f} · Polars {po['peak_mem_mib'] / 1024:.1f} · Spark {p['peak_mem_mib'] / 1024:.1f} GiB",
         "cgroup memory.peak of each 8 GiB container", PURPLE),
        (f"Python UDF {q(p, 'udf_tier_py'):.1f} s · Arrow {q(p, 'udf_tier_arrow'):.1f} s · built-in {q(p, 'udf_builtin'):.2f} s",
         f"same column on 200M rows · Scala UDF {q(sc, 'udf_tier_scala'):.2f} s", RED),
    ]
    y = 404
    for title, sub, col in facts:
        d.rounded_rectangle((80, y + 6, 88, y + 58), radius=4, fill=col)
        d.text((106, y), title, font=font(26, 750), fill=WHITE)
        d.text((106, y + 34), sub, font=font(19, 450), fill=GREY)
        y += 80
    d.text((80, 828), "github.com/masterfabric/masterfabric-spark-rust-bench", font=font(28, 700), fill=WHITE)
    im.save(DOCS / "hero.png", optimize=True)
    print("docs/hero.png written")


if __name__ == "__main__":
    main()
