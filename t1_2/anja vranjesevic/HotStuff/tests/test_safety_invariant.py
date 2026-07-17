"""M4 — safety holds under Byzantine leaders: Theorem 2 (no conflicting commits)
and Lemma 1 (no two conflicting QCs per view)."""
import asyncio

from bank.ledger import Ledger, Transfer
from byzantine import CensoringReplica, EquivocatingReplica
from hotstuff.log import mute
from demos.harness import Cluster

mute(True)
START = {"A": 100, "B": 50, "C": 50, "D": 50}


def test_equivocation_gathers_no_quorum_and_stays_safe():
    byz = 1
    cluster = Cluster(fixed_leader=None, timeouts=True, base_timeout=0.12, delay=0.006,
                      seed=5, make_state=lambda: Ledger(START),
                      byzantine={byz: (EquivocatingReplica, {})})

    async def script(c):
        for i in range(10):
            c.client.submit(Transfer("A", "B", 1, nonce=i)); await asyncio.sleep(0.05)
        assert await c.drive_until_converged(excluded={byz})

    asyncio.run(cluster.run(6.0, script, stop_when_done=True))
    assert cluster.check_convergence(excluded={byz})           # honest replicas agree
    assert cluster.check_safety_invariants(excluded={byz})     # Theorem 2 + Lemma 1
    assert len(cluster.correct({byz})[0].committed) > 0        # and they made progress


def test_censorship_defeated_by_rotation():
    byz = 0
    cluster = Cluster(fixed_leader=None, timeouts=True, base_timeout=0.12, delay=0.006,
                      seed=2, make_state=lambda: Ledger(START),
                      byzantine={byz: (CensoringReplica, {"censor_sender": "C"})})
    censored = Transfer("C", "D", 25, nonce=99)

    def c_committed(c):
        return any(n.cmd == censored for n in c.correct({byz})[0].committed)

    async def script(c):
        for i in range(3):
            c.client.submit(Transfer("A", "B", 1, nonce=i)); await asyncio.sleep(0.05)
        c.client.submit(censored)
        for i in range(3, 8):
            c.client.submit(Transfer("A", "B", 1, nonce=i)); await asyncio.sleep(0.05)
        # rotation must eventually let an honest leader propose C's transfer
        assert await c.drive_until(c_committed), "censorship not defeated"
        assert await c.drive_until_converged(excluded={byz})

    asyncio.run(cluster.run(6.0, script, stop_when_done=True))
    assert c_committed(cluster), "censored transfer never committed"
    assert cluster.check_convergence(excluded={byz})
    assert cluster.check_safety_invariants(excluded={byz})
