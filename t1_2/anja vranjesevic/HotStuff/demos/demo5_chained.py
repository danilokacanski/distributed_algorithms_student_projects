"""Demo 5 — Chained HotStuff (Algorithm 3), the "better project" bonus.

Two parts:

(1) The SAME double-spend scenario as demo 3, now on Chained HotStuff — identical
    ledger outcome (exactly one OK, all replicas agree), proving the chained
    variant is a drop-in replacement that reuses every other component.

(2) A mini throughput comparison: Basic vs Chained, same seeded command stream,
    same wall-clock budget, rotating leaders. Chained pipelines the phases (one
    committed command per view instead of one per four), so it commits more.

    python -m demos.demo5_chained
"""
import asyncio

from bank.ledger import OK, Ledger, Transfer
from hotstuff.chained_replica import ChainedReplica
from hotstuff.log import event, mute, reset_clock
from hotstuff.replica import Replica
from demos.harness import Cluster

DS_START = {"A": 100, "B": 0, "C": 0, "D": 0}
T1 = Transfer("A", "B", 80, nonce=1)
T2 = Transfer("A", "C", 80, nonce=2)


# ------------------------------------------------------------------ (1) double-spend on Chained
def double_spend_chained(seed: int) -> Cluster:
    cluster = Cluster(fixed_leader=0, delay=0.006, seed=seed, replica_cls=ChainedReplica,
                      make_state=lambda: Ledger(DS_START))
    order = [T1, T2]
    cluster.rng.shuffle(order)

    def both_committed_everywhere(c: Cluster) -> bool:
        if not c.all_converged():
            return False
        cmds = {n.cmd for n in c.replicas[0].committed}
        return T1 in cmds and T2 in cmds

    async def script(c: Cluster) -> None:
        for cmd in order:
            c.replicas[0].submit(cmd)
        # a few fillers give T1/T2 the 3 descendants they need to commit
        for i in range(6):
            c.replicas[0].submit(Transfer("D", "A", 1, nonce=1000 + i))
            await asyncio.sleep(0.03)
        # then stop feeding and let the pipeline drain until every replica agrees
        for _ in range(100):
            if both_committed_everywhere(c):
                break
            await asyncio.sleep(0.02)

    asyncio.run(cluster.run(3.0, script, stop_when_done=True))
    return cluster


# ------------------------------------------------------------------ (2) throughput comparison
def throughput(replica_cls, seconds: float, seed: int = 0) -> int:
    cluster = Cluster(fixed_leader=None, timeouts=True, base_timeout=0.3, delay=0.004,
                      seed=seed, replica_cls=replica_cls,
                      make_state=lambda: Ledger({"A": 10 ** 9, "B": 0, "C": 0, "D": 0}))

    async def script(c: Cluster) -> None:
        i = 0
        # keep a healthy backlog so leaders are never starved
        while True:
            if sum(len(r._pending) for r in c.replicas) < 40:
                for _ in range(40):
                    c.client.submit(Transfer("A", "B", 1, nonce=i)); i += 1
            await asyncio.sleep(0.02)

    asyncio.run(cluster.run(seconds, script))
    return min(len(r.committed) for r in cluster.replicas)   # committed on every replica


def main() -> None:
    reset_clock()

    # ---- part 1 ----
    event("demo", "SCENARIO", "1) double-spend on Chained HotStuff")
    cluster = double_spend_chained(seed=1)
    ref = cluster.states[0]
    for cmd, outcome in ref.history:
        if cmd in (T1, T2):
            event("demo", "apply", f"{cmd} -> {outcome}")
    assert cluster.all_converged(), "chained replicas diverged"
    assert all(s.balances == ref.balances for s in cluster.states)
    assert [o for c, o in ref.history if c in (T1, T2)].count(OK) == 1
    assert ref.check_invariants()
    event("demo", "result", f"chained: exactly one transfer won; balances {ref.balances}")

    # over several seeds: winner may differ, but always exactly one OK and agreement
    mute(True)
    agree = True
    for s in range(8):
        cc = double_spend_chained(seed=s)
        r0 = cc.states[0]
        agree &= cc.all_converged() and [o for c, o in r0.history if c in (T1, T2)].count(OK) == 1
    mute(False)
    assert agree, "chained double-spend not consistent across seeds"
    event("demo", "sweep", "8 seeded chained runs: exactly one OK, full agreement every time")

    # ---- part 2 ----
    event("demo", "----", "")
    event("demo", "SCENARIO", "2) throughput: Basic vs Chained, rotating leaders, 2.0s each")
    mute(True)
    basic = throughput(Replica, seconds=2.0, seed=0)
    chained = throughput(ChainedReplica, seconds=2.0, seed=0)
    mute(False)
    speedup = chained / basic if basic else float("inf")
    event("demo", "THROUGHPUT", f"Basic   committed {basic:4d} commands in 2.0s")
    event("demo", "THROUGHPUT", f"Chained committed {chained:4d} commands in 2.0s  ({speedup:.1f}x)")
    print()
    print("    protocol   | committed (2.0s, rotating leaders)")
    print("    -----------+-----------------------------------")
    print(f"    Basic      | {basic}")
    print(f"    Chained    | {chained}")
    print()
    assert chained > basic, "chained should out-commit basic via pipelining"
    event("demo", "PASS", "chained matches basic's safety and beats its throughput OK")


if __name__ == "__main__":
    main()
