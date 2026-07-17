#!/usr/bin/env python3
"""Communication complexity: NBFT vs PBFT (reproduces Figure 6 of the paper).

NBFT traffic for one consensus round (Formula 4):

    C_NBFT = 2(n - 1) + 2(m - 1) * R + R^2,   R = floor((n - 1) / m)

PBFT traffic for the same round (preprepare + prepare + commit):

    C_PBFT = (n - 1) + (n - 1)^2 + n(n - 1)

The chart plots the ratio C_NBFT / C_PBFT for m = 4, 7, 10 on networks up
to 1000 nodes - the same view as the paper's Figure 6.

Usage:
    python -m experiments.complexity
"""

from __future__ import annotations


import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from experiments import chartstyle as cs

CHART_DIR = Path(__file__).resolve().parent / "charts"

GROUP_SIZES = (4, 7, 10)


def nbft_traffic(n: int, m: int) -> int:
    r = (n - 1) // m
    return 2 * (n - 1) + 2 * (m - 1) * r + r * r


def pbft_traffic(n: int) -> int:
    return (n - 1) + (n - 1) ** 2 + n * (n - 1)


def main() -> int:
    fig, ax = cs.new_axes(
        "Communication traffic ratio: NBFT / PBFT",
        "one consensus round, Formula 4 vs the PBFT message pattern",
    )
    for slot, m in enumerate(GROUP_SIZES):
        xs = list(range(4 * m + 1, 1001, 3))
        ys = [nbft_traffic(n, m) / pbft_traffic(n) for n in xs]
        cs.line(ax, xs, ys, slot, f"m = {m}", markevery=30)
        cs.end_label(ax, xs[-1], ys[-1], f"m = {m}")
    cs.axis_labels(ax, "network size n", "traffic ratio (lower is better)")
    ax.set_xlim(0, 1080)
    ax.set_ylim(0, None)
    cs.legend(ax)
    CHART_DIR.mkdir(exist_ok=True)
    cs.save(fig, str(CHART_DIR / "complexity.png"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
