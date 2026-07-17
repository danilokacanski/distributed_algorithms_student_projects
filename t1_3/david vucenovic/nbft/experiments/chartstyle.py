"""Shared chart styling: validated categorical palette + recessive chrome.

Palette and chrome follow a validated reference design system (categorical
slots assigned in fixed order; aqua/yellow are sub-3:1 on the light surface,
so every chart carries direct line-end labels and distinct markers as the
required relief).
"""

from __future__ import annotations


import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt

SERIES = ["#2a78d6", "#1baf7a", "#eda100"]  # blue, aqua, yellow (fixed order)
MARKERS = ["o", "s", "^"]

INK = "#0b0b0b"
SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
SURFACE = "#fcfcfb"


def new_axes(title: str, subtitle: str = "", figsize=(7.6, 4.6)):
    fig, ax = plt.subplots(figsize=figsize, dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(BASELINE)
        ax.spines[side].set_linewidth(0.8)
    ax.grid(axis="y", color=GRID, linewidth=0.7)
    ax.set_axisbelow(True)
    ax.tick_params(colors=MUTED, labelsize=9)
    fig.suptitle(title, x=0.06, y=0.97, ha="left", fontsize=12, fontweight="bold", color=INK)
    if subtitle:
        fig.text(0.06, 0.905, subtitle, ha="left", fontsize=9, color=SECONDARY)
    return fig, ax


def line(ax, x, y, slot: int, label: str, markevery=None):
    ax.plot(
        x,
        y,
        color=SERIES[slot % len(SERIES)],
        marker=MARKERS[slot % len(MARKERS)],
        linewidth=2,
        markersize=6,
        markeredgecolor=SURFACE,
        markeredgewidth=1.0,
        markevery=markevery,
        label=label,
    )


def end_label(ax, x, y, text: str, dx: float = 0.0, dy: float = 0.0):
    """Direct label near the end of a line, in ink (not the series color)."""
    ax.annotate(
        text,
        (x, y),
        xytext=(6 + dx, dy),
        textcoords="offset points",
        fontsize=9,
        color=SECONDARY,
        va="center",
    )


def legend(ax):
    ax.legend(
        frameon=False,
        fontsize=9,
        labelcolor=SECONDARY,
        handlelength=1.6,
        borderaxespad=0.2,
    )


def axis_labels(ax, xlabel: str, ylabel: str):
    ax.set_xlabel(xlabel, fontsize=10, color=SECONDARY)
    ax.set_ylabel(ylabel, fontsize=10, color=SECONDARY)


def vline(ax, x: float, text: str):
    """Muted dashed reference line (e.g. the n/3 boundary)."""
    ax.axvline(x, color=MUTED, linewidth=1.0, linestyle=(0, (4, 3)), alpha=0.8)
    ax.annotate(
        text,
        (x, 1.0),
        xycoords=("data", "axes fraction"),
        xytext=(3, -2),
        textcoords="offset points",
        fontsize=8,
        color=MUTED,
        va="top",
    )


def save(fig, path: str):
    fig.tight_layout(rect=(0, 0, 1, 0.90))
    fig.savefig(path, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print(f"saved {path}")
