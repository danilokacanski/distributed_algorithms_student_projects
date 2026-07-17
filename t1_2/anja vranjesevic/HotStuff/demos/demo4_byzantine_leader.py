"""Demo 4 — Byzantine leaders, two attacks, both defeated by the honest rules.

(1) EQUIVOCATION: a Byzantine leader proposes two conflicting blocks to disjoint
    halves of the cluster. Neither half reaches quorum (3 of 4), so no QC forms,
    the view times out, and an honest leader takes over. Living proof of Lemma 1.

(2) CENSORSHIP: a Byzantine leader refuses to propose Bank C's transfers. Leader
    rotation defeats it — C's transfer commits within a few views, the moment an
    honest leader takes a turn.

After each run the safety-invariant checker asserts Theorem 2 (no conflicting
commits) and Lemma 1 (no two conflicting QCs per view).

    python -m demos.demo4_byzantine_leader
"""
import asyncio

from bank.ledger import OK, Ledger, Transfer
from hotstuff.log import event, reset_clock
from byzantine import CensoringReplica, EquivocatingReplica
from demos.harness import Cluster

START = {"A": 100, "B": 50, "C": 50, "D": 50}
BYZ = 1  # the Byzantine replica id


def demo_equivocation() -> None:
    event("demo", "SCENARIO", "1) equivocating leader R1 (leads views 1,5,9,...)")
    cluster = Cluster(fixed_leader=None, timeouts=True, base_timeout=0.15, delay=0.008,
                      seed=5, make_state=lambda: Ledger(START),
                      byzantine={BYZ: (EquivocatingReplica, {})})

    async def script(c: Cluster) -> None:
        for i in range(12):
            c.client.submit(Transfer("A", "B", 1, nonce=i))
            await asyncio.sleep(0.06)
        assert await c.drive_until_converged(excluded={BYZ}), "honest replicas failed to converge"

    asyncio.run(cluster.run(6.0, script, stop_when_done=True))

    honest = cluster.correct({BYZ})
    committed = len(honest[0].committed)
    event("demo", "result", f"honest replicas committed {committed} despite R1 equivocating each turn")
    assert cluster.check_convergence(excluded={BYZ}), "honest replicas diverged"
    assert cluster.check_safety_invariants(excluded={BYZ}), "SAFETY VIOLATED under equivocation"
    assert committed > 0
    event("demo", "PASS", "equivocation gathered no quorum; honest majority made progress OK")


def demo_censorship() -> None:
    event("demo", "SCENARIO", "2) censoring leader R0 drops Bank C's transfers (R0 leads 0,4,8,...)")
    cluster = Cluster(fixed_leader=None, timeouts=True, base_timeout=0.15, delay=0.008,
                      seed=2, make_state=lambda: Ledger(START),
                      byzantine={0: (CensoringReplica, {"censor_sender": "C"})})
    c_transfer = Transfer("C", "D", 25, nonce=99)

    def c_committed(cl: Cluster) -> bool:
        return any(n.cmd == c_transfer for n in cl.correct({0})[0].committed)

    async def script(cl: Cluster) -> None:
        # background traffic so views keep rotating
        for i in range(4):
            cl.client.submit(Transfer("A", "B", 1, nonce=i))
            await asyncio.sleep(0.05)
        cl.client.submit(c_transfer)                       # the censored transfer
        cl._submit_view = max(r.view for r in cl.replicas)
        for i in range(4, 10):
            cl.client.submit(Transfer("A", "B", 1, nonce=i))
            await asyncio.sleep(0.05)
        assert await cl.drive_until(c_committed), "censorship not defeated"
        assert await cl.drive_until_converged(excluded={0}), "honest replicas failed to converge"

    asyncio.run(cluster.run(6.0, script, stop_when_done=True))

    # find where C's censored transfer finally landed in the honest log
    honest = cluster.correct({0})[0]
    committed = [n for n in honest.committed if n.cmd == c_transfer]
    assert committed, "censored transfer never committed — censorship NOT defeated"
    commit_view = committed[0].view_number
    delay = commit_view - cluster._submit_view
    outcome = dict(cluster.states[honest.id].history)[c_transfer]
    event("demo", "result",
          f"C's transfer committed at view {commit_view} (~{max(delay,1)} views after submit) -> {outcome}")
    assert cluster.check_convergence(excluded={0})
    assert cluster.check_safety_invariants(excluded={0}), "SAFETY VIOLATED under censorship"
    assert outcome == OK
    event("demo", "PASS", "rotation defeated censorship; C's transfer committed OK")


def main() -> None:
    reset_clock()
    demo_equivocation()
    event("demo", "----", "")
    demo_censorship()


if __name__ == "__main__":
    main()
