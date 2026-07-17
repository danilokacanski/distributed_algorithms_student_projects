#!/usr/bin/env python3
"""Success rate measured on the full asyncio simulator (n = 17, m = 4).

Unlike the FND experiment (a statistical model), every point here runs the
complete protocol - all seven phases, both defense models, timeouts and
view changes - with an increasing number of crashed (fail-stop) nodes on
random positions. The shaded band is the theoretical fault-tolerance
interval [R, T] = [4, 7]; the dashed line marks n/3.

Usage:
    python -m experiments.simulated_success [--trials 12] [--max-byz 10]
"""

from __future__ import annotations


import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rich.console import Console

from experiments import chartstyle as cs
from nbft.config import SimulationConfig
from nbft.params import ConsensusParams
from nbft.simulator import simulate

SILENT = Console(quiet=True)

CHART_DIR = Path(__file__).resolve().parent / "charts"

# Tight but fair timeouts: a crashed primary needs the client-alert ->
# watchdog -> view-change path, which takes a few phase timeouts to walk.
FAST = dict(
    base_delay_ms=1.0,
    jitter_ms=2.0,
    phase_timeout_ms=120.0,
    client_timeout_ms=450.0,
    client_retries=4,
    trace_level="quiet",
)


def point(byz: int, trials: int) -> float:
    wins = 0
    for trial in range(trials):
        cfg = SimulationConfig(
            n=17,
            m=4,
            seed=1000 * (trial + 1) + byz,
            byz_count=byz,
            byz_behavior="crash" if byz else "none",
            byz_target="random",
            **FAST,
        )
        wins += simulate(cfg, console=SILENT).success
    return wins / trials


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=12, help="full protocol runs per point")
    parser.add_argument("--max-byz", type=int, default=10)
    args = parser.parse_args(argv)

    params = ConsensusParams(n=17, m=4)
    xs = list(range(args.max_byz + 1))
    ys = []
    for byz in xs:
        rate = point(byz, args.trials)
        ys.append(rate)
        print(f"byzantine={byz:2d}  success={rate:5.2f}")

    fig, ax = cs.new_axes(
        "Full-protocol simulation: success rate (n = 17, m = 4)",
        f"{args.trials} complete asyncio runs per point, crashed nodes on random positions",
    )
    r, t = params.tolerance_interval
    ax.axvspan(r, t, color=cs.SERIES[0], alpha=0.08)
    ax.annotate(
        f"tolerance interval [R, T] = [{r}, {t}]",
        ((r + t) / 2, 0.06),
        ha="center",
        fontsize=8,
        color=cs.SECONDARY,
    )
    cs.line(ax, xs, ys, 0, "measured success rate")
    cs.vline(ax, params.n / 3, "n/3")
    cs.axis_labels(ax, "number of crashed nodes", "consensus success rate")
    ax.set_ylim(-0.03, 1.06)
    ax.set_xticks(xs)
    cs.legend(ax)
    CHART_DIR.mkdir(exist_ok=True)
    cs.save(fig, str(CHART_DIR / "sim_success.png"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
