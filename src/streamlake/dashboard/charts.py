"""Inline SVG chart primitives.

No chart library and no CDN, so the dashboard stays a single HTML file that opens from disk.
Everything here emits SVG strings that take their colour from CSS custom properties, which makes
dark mode a stylesheet swap rather than a second render.

Hues are assigned by fixed slot and never cycled, and every series is direct-labelled, so
identity never depends on colour alone.
"""

from __future__ import annotations

from dataclasses import dataclass
from html import escape

# Categorical slots, in fixed order. Light and dark steps of the same hues.
SERIES_LIGHT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
SERIES_DARK = ["#3987e5", "#d95926", "#199e70", "#c98500"]

STATUS = {
    "good": "#0ca30c",
    "warning": "#fab219",
    "serious": "#ec835a",
    "critical": "#d03b3b",
}


def _fmt(value: float, decimals: int = 0) -> str:
    return f"{value:,.{decimals}f}"


def compact(value: float) -> str:
    """1_234_567 -> '1.23M'. Axis labels have no room for full numbers."""
    for limit, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(value) >= limit:
            return f"{value / limit:.2f}".rstrip("0").rstrip(".") + suffix
    return _fmt(value)


@dataclass
class Series:
    name: str
    points: list[float]
    slot: int = 0


def hbar(
    labels: list[str],
    values: list[float],
    *,
    width: int = 640,
    row_height: int = 26,
    value_format: str = "{:,.0f}",
    label_width: int = 150,
    unit: str = "",
) -> str:
    # Horizontal rather than vertical: the category labels are place names, and vertical bars
    # would need them rotated.
    if not values:
        return '<p class="empty">no data</p>'

    height = row_height * len(values) + 12
    scale = max(values) or 1
    bar_width = width - label_width - 90
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" class="chart" '
        f'preserveAspectRatio="xMinYMin meet">'
    ]

    for index, (label, value) in enumerate(zip(labels, values, strict=False)):
        y = index * row_height
        length = max(2, (value / scale) * bar_width)
        parts.append(
            f'<g class="bar-row"><title>{escape(str(label))}: '
            f"{value_format.format(value)}{escape(unit)}</title>"
            f'<text x="{label_width - 8}" y="{y + 15}" text-anchor="end" class="cat-label">'
            f"{escape(str(label))}</text>"
            f'<rect x="{label_width}" y="{y + 4}" width="{length:.1f}" height="14" rx="4" '
            f'class="bar" />'
            f'<text x="{label_width + length + 8:.1f}" y="{y + 15}" class="val-label">'
            f"{value_format.format(value)}{escape(unit)}</text></g>"
        )
    parts.append("</svg>")
    return "".join(parts)


def multiline(
    x_labels: list[str],
    series: list[Series],
    *,
    width: int = 720,
    height: int = 280,
    y_label: str = "",
) -> str:
    """Multi-series line chart. One y axis only — two measures of different magnitude get two
    charts, because a second y scale lets the author pick where the lines cross."""
    if not series or not x_labels:
        return '<p class="empty">no data</p>'

    pad_left, pad_right, pad_top, pad_bottom = 54, 78, 28, 34
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    y_max = max((max(s.points) for s in series if s.points), default=1) or 1
    # Round the axis up to something a human would pick.
    step = 10 ** (len(str(int(y_max))) - 1)
    y_top = ((int(y_max / step) + 1) * step) if step else y_max

    def x_at(i: int) -> float:
        return pad_left + (i / max(len(x_labels) - 1, 1)) * plot_w

    def y_at(v: float) -> float:
        return pad_top + plot_h - (v / y_top) * plot_h

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" class="chart" '
        f'preserveAspectRatio="xMinYMin meet">'
    ]

    for tick in range(5):
        value = y_top * tick / 4
        y = y_at(value)
        parts.append(
            f'<line x1="{pad_left}" y1="{y:.1f}" x2="{pad_left + plot_w}" '
            f'y2="{y:.1f}" class="grid" />'
        )
        parts.append(
            f'<text x="{pad_left - 8}" y="{y + 4:.1f}" text-anchor="end" class="axis-label">'
            f"{compact(value)}</text>"
        )

    for index, label in enumerate(x_labels):
        if index % max(1, len(x_labels) // 8) == 0:
            parts.append(
                f'<text x="{x_at(index):.1f}" y="{height - 12}" text-anchor="middle" '
                f'class="axis-label">{escape(str(label))}</text>'
            )

    for s in series:
        slot = s.slot % len(SERIES_LIGHT)
        points = " ".join(f"{x_at(i):.1f},{y_at(v):.1f}" for i, v in enumerate(s.points))
        parts.append(f'<polyline points="{points}" class="line series-{slot}" />')
        for i, v in enumerate(s.points):
            parts.append(
                f'<circle cx="{x_at(i):.1f}" cy="{y_at(v):.1f}" r="7" class="dot series-{slot}">'
                f"<title>{escape(s.name)} · {escape(str(x_labels[i]))}: {_fmt(v)}</title></circle>"
            )
        # Label at the end of the line, so there is no legend to look up.
        parts.append(
            f'<text x="{x_at(len(s.points) - 1) + 10:.1f}" y="{y_at(s.points[-1]) + 4:.1f}" '
            f'class="series-label series-{slot}">{escape(s.name)}</text>'
        )

    if y_label:
        parts.append(
            f'<text x="{pad_left - 46}" y="{pad_top - 12}" class="axis-title">'
            f"{escape(y_label)}</text>"
        )
    parts.append("</svg>")
    return "".join(parts)


def stacked_bar(
    segments: list[tuple[str, float]],
    *,
    width: int = 640,
    height: int = 46,
) -> str:
    total = sum(v for _, v in segments) or 1
    x = 0.0
    gap = 2.0
    parts = [f'<svg viewBox="0 0 {width} {height}" role="img" class="chart">']
    for index, (name, value) in enumerate(segments):
        seg_w = max(0.0, (value / total) * width - gap)
        share = 100 * value / total
        slot = index % len(SERIES_LIGHT)
        parts.append(
            f"<g><title>{escape(name)}: {_fmt(value)} ({share:.1f}%)</title>"
            f'<rect x="{x:.1f}" y="0" width="{seg_w:.1f}" height="20" rx="4" '
            f'class="bar series-fill-{slot}" /></g>'
        )
        if share > 6:
            parts.append(
                f'<text x="{x + seg_w / 2:.1f}" y="38" text-anchor="middle" class="val-label">'
                f"{escape(name)} {share:.0f}%</text>"
            )
        x += seg_w + gap
    parts.append("</svg>")
    return "".join(parts)


def status_dot(status: str) -> str:
    # Callers always render this next to the status label, never in place of it.
    colour = STATUS.get(status, STATUS["warning"])
    return f'<span class="dot-status" style="background:{colour}"></span>'
