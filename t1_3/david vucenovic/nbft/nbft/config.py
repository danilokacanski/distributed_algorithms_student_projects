"""Simulation configuration: TOML scenario presets + CLI overrides."""

from __future__ import annotations


try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # Python 3.9 / 3.10
    import tomli as tomllib
from dataclasses import dataclass, field, fields, replace
from pathlib import Path


@dataclass(frozen=True)
class SimulationConfig:
    # scenario identity
    name: str = "ad-hoc"
    description: str = ""

    # consensus parameters
    n: int = 17
    m: int = 4

    # run control
    seed: int = 42
    rounds: int = 1

    # network conditions
    base_delay_ms: float = 5.0
    jitter_ms: float = 10.0
    loss_rate: float = 0.0

    # timeouts
    phase_timeout_ms: float = 400.0
    client_timeout_ms: float = 1500.0
    client_retries: int = 3

    # byzantine population
    byz_count: int = 0
    byz_behavior: str = "none"  # crash | silent_leader | equivocate | low_sig | delay
    byz_target: str = "random"  # random | primary | representative | member
    byz_extra_delay_ms: float = 1200.0  # used by the "delay" behavior

    # output
    trace_level: str = "normal"  # quiet | normal | verbose

    @classmethod
    def from_toml(cls, path: str | Path) -> "SimulationConfig":
        data = tomllib.loads(Path(path).read_text())
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown keys in {path}: {sorted(unknown)}")
        return cls(**data)

    def with_overrides(self, **overrides) -> "SimulationConfig":
        clean = {k: v for k, v in overrides.items() if v is not None}
        return replace(self, **clean) if clean else self
