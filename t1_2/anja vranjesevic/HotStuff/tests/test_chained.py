"""M5 — Chained HotStuff: converges + stays safe, and produces the same ledger
state as Basic HotStuff on the same command stream."""
import asyncio

from bank.ledger import Ledger, Transfer
from hotstuff.chained_replica import ChainedReplica
from hotstuff.log import mute
from hotstuff.replica import Replica, cmd_key
from demos.harness import Cluster

mute(True)
START = {"A": 1000, "B": 0, "C": 0, "D": 0}
# order-independent transfers (A is rich enough that all succeed) → final state is
# the same whatever order a protocol commits them in.
STREAM = ([Transfer("A", "B", 3, n) for n in range(6)]
          + [Transfer("A", "C", 5, n) for n in range(6, 12)]
          + [Transfer("A", "D", 2, n) for n in range(12, 18)])


def run_stream(replica_cls, seed=0):
    cluster = Cluster(fixed_leader=0, delay=0.005, seed=seed,
                      replica_cls=replica_cls, make_state=lambda: Ledger(START))
    want = {cmd_key(t) for t in STREAM}

    def done(c):
        if not c.all_converged():
            return False
        got = {cmd_key(n.cmd) for n in c.replicas[0].committed}
        return want <= got

    async def script(c):
        for t in STREAM:
            c.client.submit(t)
        # pings act as fillers to flush the chained pipeline's tail
        assert await c.drive_until(done, max_rounds=200, gap=0.04), "stream did not fully commit"

    asyncio.run(cluster.run(10.0, script, stop_when_done=True))
    return cluster


def test_chained_converges_and_is_safe():
    c = run_stream(ChainedReplica)
    assert c.all_converged()
    assert c.check_safety_invariants()


def test_basic_and_chained_reach_same_ledger():
    basic = run_stream(Replica)
    chained = run_stream(ChainedReplica)

    expected = {"A": 1000 - (6 * 3 + 6 * 5 + 6 * 2), "B": 18, "C": 30, "D": 12}
    assert basic.states[0].balances == expected
    assert chained.states[0].balances == expected
    # both protocols, all replicas, identical final ledger
    assert all(s.balances == expected for s in basic.states)
    assert all(s.balances == expected for s in chained.states)
    assert basic.check_safety_invariants() and chained.check_safety_invariants()
