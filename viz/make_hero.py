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


def card(base, path, box, label, color, crop=None):
    x0, y0, x1, y1 = box
    shot = Image.open(path).convert("RGB")
    if crop:
        shot = shot.crop(crop)
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


def main():
    s = load("summary")
    e = s["engines"]
    n = sum(len(v) for v in s["checks"].values())
    ok = sum(sum(v.values()) for v in s["checks"].values())
    im = background()
    card(im, IMG / "query_times.png", (960, 60, 1570, 410), "Per-query medians, 50M rows", ICE, crop=(1275, 0, 2539, 779))
    card(im, IMG / "overhead.png", (960, 460, 1570, 627), "Whole run and peak memory", ORANGE)
    card(im, IMG / "udf.png", (960, 680, 1570, 885), "Spark UDF cost", RED)
    d = ImageDraw.Draw(im)
    d.text((80, 70), "MASTERFABRIC", font=font(36, 800), fill=WHITE)
    d.text((76, 120), "Spark Rust Bench", font=font(80, 800), fill=WHITE)
    x, y = 80, 238
    for word, col in (("PySpark", ORANGE), (" · ", GREY), ("Scala", YELLOW), (" · ", GREY), ("Polars", ICE), (" · ", GREY), ("DataFusion", PURPLE)):
        f = font(38, 800)
        d.text((x, y), word, font=f, fill=col)
        x += d.textlength(word, font=f)
    d.text((80, 318), "One SQL file, one Parquet dataset, equal container limits (6 CPU, 8 GiB).", font=font(26, 500), fill=GREY)
    d.text((80, 352), f"{ok}/{n} query results identical across engines. Measured on one machine.", font=font(26, 500), fill=GREY)
    p, sc, po, df = e["pyspark_50m"], e["scala_50m"], e["polars_50m"], e["datafusion_50m"]
    q = lambda r, k: r["queries"][k]["median_s"]
    facts = [
        (f"50M rows, 5 queries: Polars {po['sql_total_median_s']:.2f} s · DataFusion {df['sql_total_median_s']:.2f} s",
         f"Spark {p['sql_total_median_s']:.2f} s (PySpark) / {sc['sql_total_median_s']:.2f} s (Scala) · single-node local mode", ICE),
        (f"Peak memory: {po['peak_mem_mib'] / 1024:.1f} GiB vs {sc['peak_mem_mib'] / 1024:.1f} GiB",
         f"Polars vs Scala Spark at 50M rows · DataFusion {df['peak_mem_mib'] / 1024:.1f}, PySpark {p['peak_mem_mib'] / 1024:.1f} GiB", PURPLE),
        (f"PySpark SQL ≈ Scala SQL: {p['sql_total_median_s']:.2f} s vs {sc['sql_total_median_s']:.2f} s",
         "same Catalyst plan; Python only costs time when rows cross into Python", YELLOW),
        (f"Python UDF {q(p, 'udf_tier_py'):.1f} s · Arrow {q(p, 'udf_tier_arrow'):.2f} s · built-in {q(p, 'udf_builtin'):.2f} s",
         f"same column on 50M rows · Scala UDF {q(sc, 'udf_tier_scala'):.2f} s", RED),
    ]
    y = 430
    for title, sub, col in facts:
        d.rounded_rectangle((80, y + 6, 88, y + 62), radius=4, fill=col)
        d.text((106, y), title, font=font(29, 750), fill=WHITE)
        d.text((106, y + 38), sub, font=font(20, 450), fill=GREY)
        y += 92
    d.text((80, 820), "github.com/masterfabric/masterfabric-spark-rust-bench", font=font(30, 700), fill=WHITE)
    im.save(DOCS / "hero.png", optimize=True)
    print("docs/hero.png written")


if __name__ == "__main__":
    main()
