"""One visual system for every benchmark chart the README and docs embed.

Plain SVG, no dependencies, so a committed image has one readable file as its
provenance. Colours are CSS classes resolved by a <style> block inside the SVG,
with a `prefers-color-scheme: dark` override: GitHub renders an <img> SVG with
the viewer's scheme, so one file reads correctly on both themes.

Colour does one job per role and never two:
  - LATENCY BANDS use the reserved status colours, always beside a text label:
    GREEN p99 <= 1 s (certified), YELLOW <= 5 s, RED above (ADR 0048,
    2026-09-27). Nothing else on a chart is green, amber or red.
  - BROKERS use categorical slots in a fixed order (blue, orange, aqua,
    violet), validated for colour-vision deficiency in both modes; yellow and
    red are skipped so no broker impersonates a band.
Text is always ink, never the series colour; identity comes from the mark
beside it.
"""

from __future__ import annotations

FONT = 'system-ui,-apple-system,"Segoe UI",Roboto,sans-serif'

# role -> (light, dark). The dark column is its own validated step, not an inversion.
TOKENS = {
    "surface": ("#fcfcfb", "#1a1a19"),
    "border": ("#e1e0d9", "#2c2c2a"),
    "ink": ("#0b0b0b", "#ffffff"),
    "ink2": ("#52514e", "#c3c2b7"),
    "muted": ("#898781", "#898781"),
    "grid": ("#eceae4", "#262624"),
    "axis": ("#c3c2b7", "#383835"),
    # status — the bands
    "good": ("#0ca30c", "#0ca30c"),
    "warn": ("#fab219", "#fab219"),
    "crit": ("#d03b3b", "#d03b3b"),
    "ghost": ("#898781", "#6f6e69"),
    # categorical — brokers, fixed order
    "s1": ("#2a78d6", "#3987e5"),
    "s2": ("#eb6834", "#d95926"),
    "s3": ("#1baf7a", "#199e70"),
    "s4": ("#4a3aa7", "#9085e9"),
}

BROKER_SLOT = {"mqttd": "s1", "mosquitto": "s2", "emqx": "s3", "hivemq": "s4"}
BROKER_NAME = {"mqttd": "mqttd", "mosquitto": "Mosquitto", "emqx": "EMQX", "hivemq": "HiveMQ CE"}

BAND_SLOT = {"green": "good", "yellow": "warn", "red": "crit"}
BAND_LABEL = {"green": "GREEN", "yellow": "YELLOW", "red": "RED"}


def slot(broker: str) -> str:
    return BROKER_SLOT.get(broker.split("-")[0].lower(), "ghost")


def name(broker: str) -> str:
    key = broker.split("-")[0].lower()
    return BROKER_NAME.get(key, broker)


def band_of(ms: float) -> str:
    """The band a p99 upper bound falls in — the same lines as summarize-curve.py."""
    return "green" if ms <= 1000 else "yellow" if ms <= 5000 else "red"


def esc(text: object) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _css() -> str:
    def rules(i: int) -> str:
        out = []
        for role, pair in TOKENS.items():
            c = pair[i]
            out.append(f".f-{role}{{fill:{c}}}.k-{role}{{stroke:{c}}}")
        return "".join(out)

    return (
        "<style>"
        f"text{{font-family:{FONT};font-kerning:normal}}"
        ".num{font-variant-numeric:tabular-nums}"
        + rules(0)
        + "@media (prefers-color-scheme:dark){" + rules(1) + "}"
        "</style>"
    )


def open_svg(w: int, h: int, title: str, subtitle: str, desc: str = "") -> list[str]:
    """Card surface, title block and the theme style — the top of every chart."""
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" '
        f'role="img" aria-labelledby="t d">',
        f"<title id=\"t\">{esc(title)}</title><desc id=\"d\">{esc(desc or subtitle)}</desc>",
        _css(),
        f'<rect x="0.5" y="0.5" width="{w - 1}" height="{h - 1}" rx="14" class="f-surface k-border"/>',
        f'<text x="32" y="42" font-size="20" font-weight="650" class="f-ink">{esc(title)}</text>',
        f'<text x="32" y="64" font-size="13" class="f-ink2">{esc(subtitle)}</text>',
    ]


def text(x: float, y: float, s: object, *, size: int = 12, role: str = "ink2", anchor: str = "start",
         weight: int = 400, num: bool = False, halo: bool = False) -> str:
    """Ink text. `halo` rings it in the surface colour, for a label that sits on a line."""
    cls = f"f-{role}" + (" num" if num else "") + (" k-surface" if halo else "")
    extra = ' stroke-width="4" paint-order="stroke" stroke-linejoin="round"' if halo else ""
    wt = f' font-weight="{weight}"' if weight != 400 else ""
    return (f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}"{wt} text-anchor="{anchor}" '
            f'class="{cls}"{extra}>{esc(s)}</text>')


def hline(x0: float, x1: float, y: float, role: str = "grid", width: float = 1, dash: str = "") -> str:
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return f'<line x1="{x0:.1f}" y1="{y:.1f}" x2="{x1:.1f}" y2="{y:.1f}" class="k-{role}" stroke-width="{width}"{d}/>'


def vline(x: float, y0: float, y1: float, role: str = "grid", width: float = 1, dash: str = "") -> str:
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return f'<line x1="{x:.1f}" y1="{y0:.1f}" x2="{x:.1f}" y2="{y1:.1f}" class="k-{role}" stroke-width="{width}"{d}/>'


def column(x: float, y_top: float, y_base: float, w: float, role: str, *, round_top: bool = True,
           opacity: float = 1.0) -> str:
    """A column segment: 4px rounded data-end, square where it meets what is below."""
    h = max(y_base - y_top, 0.0)
    if h <= 0:
        return ""
    r = min(4.0, h, w / 2) if round_top else 0.0
    op = f' fill-opacity="{opacity}"' if opacity < 1 else ""
    if r == 0:
        return f'<rect x="{x:.1f}" y="{y_top:.1f}" width="{w:.1f}" height="{h:.1f}" class="f-{role}"{op}/>'
    return (f'<path d="M{x:.1f},{y_base:.1f} V{y_top + r:.1f} Q{x:.1f},{y_top:.1f} {x + r:.1f},{y_top:.1f} '
            f'H{x + w - r:.1f} Q{x + w:.1f},{y_top:.1f} {x + w:.1f},{y_top + r:.1f} V{y_base:.1f} Z" '
            f'class="f-{role}"{op}/>')


def swatch(x: float, y: float, role: str, label: str, *, kind: str = "box") -> list[str]:
    """A legend key: the mark beside ink text — identity never rides on text colour."""
    if kind == "line":
        mark = f'<line x1="{x:.1f}" y1="{y - 4:.1f}" x2="{x + 18:.1f}" y2="{y - 4:.1f}" class="k-{role}" stroke-width="2.5" stroke-linecap="round"/>'
    elif kind == "dash":
        mark = (f'<line x1="{x:.1f}" y1="{y - 4:.1f}" x2="{x + 18:.1f}" y2="{y - 4:.1f}" class="k-{role}" '
                'stroke-width="2" stroke-dasharray="4 3"/>')
    elif kind == "ghost":
        mark = (f'<rect x="{x + 1:.1f}" y="{y - 11:.1f}" width="12" height="12" rx="3" fill="none" '
                f'class="k-{role}" stroke-width="1.5" stroke-dasharray="3 2"/>')
    else:
        mark = f'<rect x="{x:.1f}" y="{y - 11:.1f}" width="12" height="12" rx="3" class="f-{role}"/>'
    return [mark, text(x + (26 if kind in ("line", "dash") else 19), y, label, size=12, role="ink2")]


def legend_width(label: str, kind: str = "box") -> float:
    """A generous estimate of a legend entry's width at 12px system sans."""
    return (26 if kind in ("line", "dash") else 19) + 6.4 * len(label) + 22


def compact(v: float) -> str:
    if v >= 1_000_000:
        s = f"{v / 1e6:.2f}".rstrip("0").rstrip(".")
        return f"{s}M"
    if v >= 1000:
        return f"{v / 1000:.0f}k"
    return f"{v:.0f}"
