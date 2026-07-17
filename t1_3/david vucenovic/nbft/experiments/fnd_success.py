#!/usr/bin/env python3
"""FND-model consensus experiment (reproduces Figures 4 and 5 of the paper).

For every number of Byzantine nodes the FND (Faulty Number Determined) model
places them uniformly at random over the ring and asks whether the threshold
vote-counting model can still reach (R - w) * m votes:

    * an honest representative whose group kept >= 2E+1 honest members
      carries the full m votes,
    * a blocked or starved group contributes its distinct honest signers,
    * a run also needs (n-1)/2 + 1 honest nodes for the reply quorum.

200 trials per point (like the paper) give the consensus success rate. The
curves show the drop happening visibly to the right of n/3.

Usage:
    python -m experiments.fnd_success [--trials 200]
"""

from __future__ import annotations


import argparse
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from experiments import chartstyle as cs
from nbft.params import ConsensusParams

CHART_DIR = Path(__file__).resolve().parent / "charts"

# (n - 1) divisible by m so no node stays ungrouped, as in the paper.
NETWORKS = {4: (101, 201, 301), 7: (106, 204, 302)}


def fnd_trial(params: ConsensusParams, byz_count: int, rng: random.Random) -> bool:
    n, m = params.n, params.m
    nodes = list(range(n))
    rng.shuffle(nodes)
    byzantine = set(nodes[:byz_count])

    ring = list(range(n))
    rng.shuffle(ring)  # random hash placement
    grouped = ring[1 : 1 + params.R * m]  # ring[0] plays the primary

    votes = 0
    for g in range(params.R):
        members = grouped[g * m : (g + 1) * m]
        honest = sum(1 for node in members if node not in byzantine)
        representative = members[rng.randrange(m)]  # hash-elected, uniform
        if representative not in byzantine and honest >= params.sig_quorum:
            votes += m  # full aggregate: F >= m - E
        else:
            votes += honest  # Model 1 fallback: individual broadcasts
    threshold_ok = votes >= params.vote_threshold
    reply_ok = (n - byz_count) >= params.reply_quorum
    return threshold_ok and reply_ok


def success_curve(params: ConsensusParams, trials: int, rng: random.Random):
    step = max(1, params.n // 100)
    xs = list(range(0, int(params.n * 0.55) + 1, step))
    ys = [
        sum(fnd_trial(params, i, rng) for _ in range(trials)) / trials
        for i in xs
    ]
    return xs, ys


def plot_for_group_size(m: int, trials: int, seed: int) -> Path:
    fig, ax = cs.new_axes(
        f"FND simulation: consensus success rate (m = {m})",
        f"{trials} random placements per point; dashed lines mark n/3",
    )
    rng = random.Random(seed)
    for slot, n in enumerate(NETWORKS[m]):
        params = ConsensusParams(n=n, m=m)
        xs, ys = success_curve(params, trials, rng)
        cs.line(ax, xs, ys, slot, f"n = {n}", markevery=max(1, len(xs) // 18))
        drop = next((x for x, y in zip(xs, ys) if y < 1.0), xs[-1])
        cs.end_label(ax, drop, 0.82, f"n = {n}", dx=4)
        cs.vline(ax, n / 3, "n/3")
    cs.axis_labels(ax, "number of Byzantine nodes", "consensus success rate")
    ax.set_ylim(-0.03, 1.06)
    cs.legend(ax)
    CHART_DIR.mkdir(exist_ok=True)
    path = CHART_DIR / f"fnd_m{m}.png"
    cs.save(fig, str(path))
    return path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=200, help="trials per point (paper: 200)")
    parser.add_argument("--seed", type=int, default=2022)
    args = parser.parse_args(argv)
    for m in NETWORKS:
        plot_for_group_size(m, args.trials, args.seed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
