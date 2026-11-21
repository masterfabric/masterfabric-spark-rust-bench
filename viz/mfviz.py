"""Shared look for every generated image: palette, fonts and small drawing helpers.

Fonts: Inter and JetBrains Mono (SIL Open Font License 1.1), downloaded once from the
google/fonts repository into ~/.cache/mf-fonts. They are not committed to this repository.
"""

from __future__ import annotations

import urllib.request
from pathlib import Path

from PIL import ImageFont

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
IMG = DOCS / "images"
RESULTS = ROOT / "results"
SHOTS = DOCS / "screenshots"

BG = (13, 17, 28)
BG2 = (28, 18, 48)
PANEL = (24, 30, 46)
LINE = (52, 62, 86)
WHITE = (240, 244, 250)
GREY = (154, 164, 178)
ICE = (79, 195, 247)
ORANGE = (255, 122, 69)
GREEN = (61, 220, 151)
PURPLE = (167, 139, 250)
YELLOW = (250, 204, 21)
RED = (248, 81, 73)

HEX = {k: "#%02x%02x%02x" % v for k, v in dict(BG=BG, PANEL=PANEL, LINE=LINE, WHITE=WHITE, GREY=GREY, ICE=ICE,
                                                ORANGE=ORANGE, GREEN=GREEN, PURPLE=PURPLE, YELLOW=YELLOW,
                                                RED=RED).items()}

_FONTS = {
    "Inter.ttf": "https://github.com/google/fonts/raw/main/ofl/inter/Inter%5Bopsz,wght%5D.ttf",
    "JetBrainsMono.ttf": "https://github.com/google/fonts/raw/main/ofl/jetbrainsmono/JetBrainsMono%5Bwght%5D.ttf",
}
_CACHE = Path.home() / ".cache" / "mf-fonts"


def _font_path(name: str) -> Path:
    p = _CACHE / name
    if not p.exists():
        _CACHE.mkdir(parents=True, exist_ok=True)
        urllib.request.urlretrieve(_FONTS[name], p)
    return p


def font(size: int, weight: int = 400, mono: bool = False) -> ImageFont.FreeTypeFont:
    f = ImageFont.truetype(str(_font_path("JetBrainsMono.ttf" if mono else "Inter.ttf")), size)
    try:
        axes = f.get_variation_axes()
        f.set_variation_by_axes([min(max(size, 14), 32), weight] if len(axes) == 2 else [weight])
    except Exception:  # noqa: BLE001 - static font fallback
        pass
    return f


def mpl_setup():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib import font_manager

    for name in _FONTS:
        font_manager.fontManager.addfont(str(_font_path(name)))
    plt.rcParams.update({
        "font.family": "Inter", "font.size": 11, "axes.facecolor": HEX["PANEL"], "figure.facecolor": HEX["BG"],
        "axes.edgecolor": HEX["LINE"], "axes.labelcolor": HEX["WHITE"], "text.color": HEX["WHITE"],
        "xtick.color": HEX["GREY"], "ytick.color": HEX["GREY"], "grid.color": HEX["LINE"], "axes.grid": True,
        "grid.alpha": 0.6, "axes.titlesize": 14, "legend.frameon": False,
        "savefig.dpi": 160, "savefig.bbox": "tight", "savefig.facecolor": HEX["BG"],
    })
    return plt


# ---- diagram primitives (shared by make_diagrams.py and make_hero.py) ----
from PIL import Image, ImageDraw  # noqa: E402

def canvas(w, h):
    im = Image.new("RGB", (w, h), BG)
    px = im.load()
    for y in range(h):
        for x in range(0, w, 1):
            t = (x / w * 0.6 + y / h * 0.4)
            px[x, y] = tuple(int(BG[i] * (1 - t) + BG2[i] * t) for i in range(3))
    return im, ImageDraw.Draw(im)


def box(d, xy, title, lines=(), accent=ICE, title_size=24, line_size=18):
    x0, y0, x1, y1 = xy
    d.rounded_rectangle(xy, radius=18, fill=PANEL, outline=accent, width=3)
    d.rounded_rectangle((x0, y0, x0 + 10, y1), radius=4, fill=accent)
    d.text((x0 + 26, y0 + 16), title, font=font(title_size, 750), fill=WHITE)
    y = y0 + 16 + title_size + 12
    for ln in lines:
        d.text((x0 + 26, y), ln, font=font(line_size, 450), fill=GREY)
        y += int(line_size * 1.45)


def arrow(d, a, b, color=GREY, label=None, width=3, at=0.5):
    d.line([a, b], fill=color, width=width)
    import math
    ang = math.atan2(b[1] - a[1], b[0] - a[0])
    for s in (2.6, -2.6):
        d.line([b, (b[0] - 16 * math.cos(ang + s / 6), b[1] - 16 * math.sin(ang + s / 6))], fill=color, width=width)
    if label:
        mx, my = a[0] + (b[0] - a[0]) * at, a[1] + (b[1] - a[1]) * at
        f = font(16, 600)
        w = d.textlength(label, font=f)
        d.rounded_rectangle((mx - w / 2 - 8, my - 14, mx + w / 2 + 8, my + 14), radius=10, fill=BG)
        d.text((mx - w / 2, my - 10), label, font=f, fill=color)


