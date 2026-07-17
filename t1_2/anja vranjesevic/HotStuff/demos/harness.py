"""Shared demo scaffolding: build n replicas wired to one simulated network,
run them for a while, then check that all correct replicas agree.

Keeps the individual demo files down to a scenario + a few asserts.
"""
from __future__ import annotations

import asyncio
import random
from collections import defaultdict
from typing import Any, Callable, Optional

from hotstuff.client import Client
from hotstuff.crypto import gen_keys
from hotstuff.log import event
from hotstuff.network import Network
from hotstuff.pacemaker import Pacemaker
from hotstuff.replica import Replica

N, F = 4, 1


class Cluster:
    def __init__(self, n: int = N, f: int = F, *, fixed_leader: Optional[int] = None,
                 delay: float = 0.01, jitter: float = 0.0, drop_prob: float = 0.0,
                 seed: int = 0, base_timeout: float = 0.4, timeouts: bool = False,
                 make_state=None, replica_cls=Replica,
                 byzantine: Optional[dict[int, Any]] = None):
        self.n, self.f = n, f
        self.rng = random.Random(seed)
        self.net = Network(n, delay=delay, jitter=jitter, drop_prob=drop_prob, rng=self.rng)
        self.pm = Pacemaker(n, fixed_leader=fixed_leader, base_timeout=base_timeout,
                            timeouts=timeouts)
        self.sks, self.vks = gen_keys(n)
        self.make_state = make_state
        self.states = [make_state() if make_state else None for _ in range(n)]
        self.replicas: list[Replica] = []
        byzantine = byzantine or {}
        for i in range(n):
            cls = byzantine.get(i, (replica_cls, {}))[0] if i in byzantine else replica_cls
            kwargs = byzantine.get(i, (replica_cls, {}))[1] if i in byzantine else {}
            self.replicas.append(cls(i, n, f, self.net, self.sks[i], self.vks, self.pm,
                                     state_machine=self.states[i], **kwargs))
        self.client = Client(self.replicas, f)
        for r in self.replicas:
            r._respond = self.client.on_execute
        self._tasks: dict[int, asyncio.Task] = {}

    def _spawn(self, r: Replica) -> None:
        self._tasks[r.id] = asyncio.create_task(r.run())

    async def run(self, seconds: float, script: Optional[Callable] = None,
                  stop_when_done: bool = False) -> None:
        """Run the cluster for `seconds`. If `stop_when_done`, end as soon as the
        scenario script returns (used by scenarios that drive to convergence),
        capped at `seconds`; otherwise the script is a background feeder and we
        always run the full duration."""
        for r in self.replicas:
            self._spawn(r)
        extra = asyncio.create_task(script(self)) if script is not None else None
        if extra is not None and stop_when_done:
            try:
                await asyncio.wait_for(asyncio.shield(extra), timeout=seconds)
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(seconds)
        for r in self.replicas:
            r.stop()
        tasks = list(self._tasks.values()) + ([extra] if extra else [])
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # ---- fault injection (M2/M4) ----
    def crash(self, node_id: int) -> None:
        """Kill a replica and cut it off the network (crash / total partition)."""
        self.replicas[node_id].stop()
        t = self._tasks.pop(node_id, None)
        if t is not None:
            t.cancel()
        self.net.partition(node_id)
        event("demo", "CRASH", f"R{node_id} killed and partitioned", replica=node_id)

    def restart(self, node_id: int) -> None:
        """Bring a crashed replica back as a *fresh* process (no persisted state —
        persistence is a stated non-goal). It rejoins and catches up via sync."""
        self.net.heal(node_id)
        self.net.inboxes[node_id] = asyncio.Queue()   # drop any stale pre-crash mail
        state = self.make_state() if self.make_state else None
        self.states[node_id] = state
        r = Replica(node_id, self.n, self.f, self.net, self.sks[node_id], self.vks,
                    self.pm, state_machine=state, on_execute=self.client.on_execute)
        self.replicas[node_id] = r
        self.client.replicas[node_id] = r
        self._spawn(r)
        event("demo", "RESTART", f"R{node_id} rejoined (fresh state, catching up)", replica=node_id)

    async def drive_until(self, pred: Callable[["Cluster"], bool], *, max_rounds: int = 80,
                          gap: float = 0.1, tag: str = "ping") -> bool:
        """Keep the cluster busy (submitting harmless no-op commands to keep views
        rotating and DECIDEs flowing) until `pred` holds. Lets a scenario wait for
        a specific event — convergence, or a particular command committing — rather
        than racing a fixed wall-clock deadline."""
        for i in range(max_rounds):
            if pred(self):
                return True
            self.client.submit({"op": tag, "seq": 10_000 + i})
            await asyncio.sleep(gap)
        return pred(self)

    async def drive_until_converged(self, excluded: set[int] = frozenset(), **kw) -> bool:
        return await self.drive_until(lambda c: c.all_converged(excluded), **kw)

    def all_converged(self, excluded: set[int] = frozenset()) -> bool:
        logs = [[n.hash for n in r.committed] for r in self.correct(excluded)]
        return bool(logs) and bool(logs[0]) and all(log == logs[0] for log in logs)

    # ---- convergence / safety checks over correct replicas ----
    def correct(self, excluded: set[int] = frozenset()):
        return [r for r in self.replicas if r.id not in excluded]

    def committed_cmds(self, r: Replica) -> list:
        return [n.cmd for n in r.committed]

    def check_convergence(self, excluded: set[int] = frozenset()) -> bool:
        """All correct replicas executed identical, prefix-consistent logs."""
        logs = {r.id: [n.hash for n in r.committed] for r in self.correct(excluded)}
        lengths = [len(v) for v in logs.values()]
        m = min(lengths) if lengths else 0
        ref = None
        ok = True
        for rid, hashes in logs.items():
            prefix = hashes[:m]
            if ref is None:
                ref = prefix
            elif prefix != ref:
                ok = False
                event("check", "DIVERGE", f"R{rid} log prefix differs")
        event("check", "converge" if ok else "FAIL",
              f"correct replicas={sorted(logs)} committed>={m}")
        return ok

    def check_safety_invariants(self, excluded: set[int] = frozenset()) -> bool:
        """The two safety properties, checkable after any run (M4):

        (a) Theorem 2 — no two correct replicas ever commit conflicting branches:
            every pair of committed logs is prefix-consistent.
        (b) Lemma 1 — within a single (type, view) there is at most one QC:
            all QCs any correct replica observed for a given phase+view name the
            same node.
        """
        reps = self.correct(excluded)
        ok = True

        logs = {r.id: [n.hash for n in r.committed] for r in reps}
        ids = sorted(logs)
        for i in ids:
            for j in ids:
                if i < j:
                    a, b = logs[i], logs[j]
                    k = min(len(a), len(b))
                    if a[:k] != b[:k]:
                        ok = False
                        event("check", "THEOREM2-FAIL", f"R{i} vs R{j} committed conflicting branches")

        by_tv: dict[tuple, set] = defaultdict(set)
        for r in reps:
            for (t, v, h) in r.qcs:
                by_tv[(t, v)].add(h)
        for (t, v), hashes in by_tv.items():
            if len(hashes) > 1:
                ok = False
                event("check", "LEMMA1-FAIL", f"{t.value} view={v}: {len(hashes)} conflicting QCs")

        event("check", "SAFE" if ok else "SAFETY-VIOLATION",
              f"Theorem 2 + Lemma 1 over correct replicas {ids}")
        return ok
