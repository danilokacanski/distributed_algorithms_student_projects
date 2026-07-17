#!/usr/bin/env python3
"""NBFT consensus simulator - command line entry point.

Examples:
    python main.py --list
    python main.py --scenario happy_path
    python main.py --scenario low_signatures --verbose
    python main.py -n 21 -m 4 --byzantine 3 --behavior equivocate --seed 7
"""

from __future__ import annotations


import argparse
import sys
from pathlib import Path

from nbft.byzantine import BEHAVIORS
from nbft.config import SimulationConfig
from nbft.simulator import simulate

SCENARIO_DIR = Path(__file__).parent / "scenarios"


def available_scenarios() -> dict[str, Path]:
    return {path.stem: path for path in sorted(SCENARIO_DIR.glob("*.toml"))}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nbft-sim",
        description="Simulator of the NBFT consensus algorithm (Yang et al., IEEE Access 2022).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-s", "--scenario", help="scenario preset name (see --list) or a path to a .toml file")
    parser.add_argument("--list", action="store_true", help="list available scenario presets and exit")

    group = parser.add_argument_group("overrides")
    group.add_argument("-n", type=int, dest="n", help="total number of nodes")
    group.add_argument("-m", type=int, dest="m", help="group size (4, 7, 10, ...)")
    group.add_argument("--rounds", type=int, help="number of client requests to decide")
    group.add_argument("--seed", type=int, help="random seed (reproducible runs)")
    group.add_argument("--byzantine", type=int, dest="byz_count", help="number of byzantine nodes")
    group.add_argument("--behavior", choices=BEHAVIORS, dest="byz_behavior", help="byzantine behavior")
    group.add_argument(
        "--target",
        choices=("random", "primary", "representative", "member"),
        dest="byz_target",
        help="which roles the byzantine nodes occupy",
    )
    group.add_argument("--delay", type=float, dest="base_delay_ms", help="base network delay [ms]")
    group.add_argument("--jitter", type=float, dest="jitter_ms", help="random extra delay [ms]")
    group.add_argument("--loss", type=float, dest="loss_rate", help="message loss rate [0..1]")
    group.add_argument("--phase-timeout", type=float, dest="phase_timeout_ms", help="phase timeout [ms]")
    group.add_argument("--client-timeout", type=float, dest="client_timeout_ms", help="client timeout [ms]")

    output = parser.add_argument_group("output")
    output.add_argument("--trace", choices=("quiet", "normal", "verbose"), dest="trace_level")
    output.add_argument("-v", "--verbose", action="store_true", help="shortcut for --trace verbose")
    output.add_argument("-q", "--quiet", action="store_true", help="shortcut for --trace quiet")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    scenarios = available_scenarios()

    if args.list:
        print("Available scenarios:")
        for name, path in scenarios.items():
            cfg = SimulationConfig.from_toml(path)
            print(f"  {name:<22} {cfg.description}")
        return 0

    if args.scenario:
        path = scenarios.get(args.scenario, Path(args.scenario))
        if not path.exists():
            parser.error(f"scenario '{args.scenario}' not found; use --list")
        cfg = SimulationConfig.from_toml(path)
    else:
        cfg = SimulationConfig(name="ad-hoc", description="ad-hoc run from CLI flags")

    trace_level = args.trace_level
    if args.verbose:
        trace_level = "verbose"
    elif args.quiet:
        trace_level = "quiet"

    cfg = cfg.with_overrides(
        n=args.n,
        m=args.m,
        rounds=args.rounds,
        seed=args.seed,
        byz_count=args.byz_count,
        byz_behavior=args.byz_behavior,
        byz_target=args.byz_target,
        base_delay_ms=args.base_delay_ms,
        jitter_ms=args.jitter_ms,
        loss_rate=args.loss_rate,
        phase_timeout_ms=args.phase_timeout_ms,
        client_timeout_ms=args.client_timeout_ms,
        trace_level=trace_level,
    )

    result = simulate(cfg)
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
