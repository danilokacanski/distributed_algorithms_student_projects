"""Simulation orchestration: build the network, run rounds, report."""

from __future__ import annotations


import asyncio
import random
import time
from dataclasses import dataclass, field

from .byzantine import ByzantineNode
from .client import Client, ClientOutcome
from .config import SimulationConfig
from .hashring import GENESIS_HASH, HashRing, Membership
from .node import HonestNode, Node
from .params import ConsensusParams
from .trace import Tracer


@dataclass
class SimulationResult:
    config: SimulationConfig
    params: ConsensusParams
    outcomes: list[ClientOutcome] = field(default_factory=list)
    byzantine: dict[str, str] = field(default_factory=dict)
    committed_per_round: list[int] = field(default_factory=list)
    honest_count: int = 0
    chains_consistent: bool = True
    final_view: int = 0
    sent: dict = field(default_factory=dict)
    dropped: dict = field(default_factory=dict)
    duration_ms: float = 0.0

    @property
    def success(self) -> bool:
        return bool(self.outcomes) and all(o.success for o in self.outcomes) and self.chains_consistent


def pick_byzantine(cfg: SimulationConfig, membership: Membership, rng: random.Random) -> dict[str, str]:
    """Choose which nodes are Byzantine according to the scenario target."""
    if cfg.byz_count <= 0 or cfg.byz_behavior == "none":
        return {}
    reps = list(membership.representatives)
    members = [nid for group in membership.groups for nid in group if nid not in reps]
    pool: list[str] = []
    if cfg.byz_target == "primary":
        pool = [membership.primary] + rng.sample(members, len(members))
    elif cfg.byz_target == "representative":
        pool = rng.sample(reps, len(reps)) + rng.sample(members, len(members))
    elif cfg.byz_target == "member":
        pool = rng.sample(members, len(members))
    elif cfg.byz_target == "random":
        everyone = list(membership.all_nodes())
        pool = rng.sample(everyone, len(everyone))
    else:
        raise ValueError(f"unknown byzantine target: {cfg.byz_target}")
    chosen = pool[: cfg.byz_count]
    return {nid: cfg.byz_behavior for nid in chosen}


async def run_simulation(cfg: SimulationConfig, console=None) -> SimulationResult:
    from .network import Network

    params = ConsensusParams(cfg.n, cfg.m)
    tracer = Tracer(cfg.trace_level, console)
    rng = random.Random(cfg.seed)

    node_ids = [f"10.0.0.{i}" for i in range(1, cfg.n + 1)]
    ring = HashRing(node_ids)
    membership0 = ring.membership(0, GENESIS_HASH, None, params.m, params.R)
    byzantine = pick_byzantine(cfg, membership0, rng)

    tracer.banner(cfg.name, cfg.description, params, cfg.seed)
    tracer.layout(membership0, byzantine)
    if byzantine:
        tracer.event(
            "BYZANTINE",
            f"{len(byzantine)}/{cfg.n} byzantine nodes, behavior '{cfg.byz_behavior}': "
            + ", ".join(sorted(byzantine)),
        )

    network = Network(rng, cfg.base_delay_ms, cfg.jitter_ms, cfg.loss_rate, tracer)
    nodes: dict[str, Node] = {}
    for nid in node_ids:
        if nid in byzantine:
            nodes[nid] = ByzantineNode(byzantine[nid], nid, params, cfg, ring, network, tracer)
        else:
            nodes[nid] = HonestNode(nid, params, cfg, ring, network, tracer)
    client = Client(cfg, params, network, tracer)

    loop = asyncio.get_running_loop()
    node_tasks = [loop.create_task(node.run()) for node in nodes.values()]
    client.start()
    tracer.start_clock()
    started = time.monotonic()

    result = SimulationResult(config=cfg, params=params, byzantine=byzantine)
    honest = [node for nid, node in nodes.items() if nid not in byzantine]
    result.honest_count = len(honest)

    for round_no in range(1, cfg.rounds + 1):
        payload = f"tx-{round_no:03d}"
        primary_hint = honest[0].membership.primary
        outcome = await client.request(payload, primary_hint, tuple(node_ids))
        result.outcomes.append(outcome)
        # Let stragglers finish preprepare2/reply before the next round.
        await asyncio.sleep((cfg.base_delay_ms + cfg.jitter_ms) * 3 / 1000.0)
        committed = sum(1 for node in honest if node.committed(outcome.digest))
        result.committed_per_round.append(committed)
        tracer.event(
            "INFO",
            f"round {round_no}: {committed}/{len(honest)} honest nodes committed d={outcome.digest}",
        )

    # Shut everything down.
    for node in nodes.values():
        node.stop()
    client.stop()
    for task in node_tasks:
        task.cancel()
    await asyncio.gather(*node_tasks, return_exceptions=True)

    # Honest chains must never conflict (they may lag due to message loss).
    chains = sorted((tuple(b.digest for b in node.chain) for node in honest), key=len)
    result.chains_consistent = all(
        chains[i] == chains[i + 1][: len(chains[i])] for i in range(len(chains) - 1)
    )
    result.final_view = max(node.view for node in honest)
    result.sent = dict(network.sent)
    result.dropped = dict(network.dropped)
    result.duration_ms = (time.monotonic() - started) * 1000

    tracer.traffic_report(result.sent, result.dropped, params, rounds=cfg.rounds)
    rounds_ok = sum(1 for o in result.outcomes if o.success)
    lines = [
        f"rounds decided: {rounds_ok}/{cfg.rounds}",
        f"honest chains consistent: {'yes' if result.chains_consistent else 'NO'}",
        f"final view: {result.final_view}",
        f"duration: {result.duration_ms:.0f}ms",
    ]
    for i, outcome in enumerate(result.outcomes, start=1):
        committed = result.committed_per_round[i - 1]
        lines.append(
            f"  round {i}: d={outcome.digest} replies={outcome.replies} "
            f"attempts={outcome.attempts} committed={committed}/{len(honest)}"
        )
    tracer.outcome(result.success, "\n".join(lines))
    return result


def simulate(cfg: SimulationConfig, console=None) -> SimulationResult:
    """Synchronous entry point."""
    return asyncio.run(run_simulation(cfg, console))
