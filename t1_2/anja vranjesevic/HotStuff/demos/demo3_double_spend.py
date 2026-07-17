"""Demo 3 — double-spend: consensus orders, the state machine judges.

Bank A holds 100. Two conflicting settlements are submitted at (nearly) the same
time to different replicas:

    T1: TRANSFER A -> B  80
    T2: TRANSFER A -> C  80

Only one can succeed. HotStuff picks *an* order; both transfers are committed and
logged, but the first to apply succeeds (A: 100 -> 20) and the second is REJECTED
for insufficient funds. Every replica judges the same ordered log, so they never
disagree about which one won — even though the winner varies run to run with the
(seeded) network timing.

    python -m demos.demo3_double_spend
"""
import asyncio

from bank.ledger import OK, Ledger, Transfer
from hotstuff.log import event, mute, reset_clock
from demos.harness import Cluster

START = {"A": 100, "B": 0, "C": 0, "D": 0}
T1 = Transfer("A", "B", 80, nonce=1)
T2 = Transfer("A", "C", 80, nonce=2)


def run_once(seed: int) -> Cluster:
    """One double-spend run. Submission order is shuffled by the seed, so the
    winner differs across seeds — but all replicas always agree within a run."""
    cluster = Cluster(fixed_leader=0, delay=0.005, seed=seed,
                      make_state=lambda: Ledger(START))
    order = [T1, T2]
    cluster.rng.shuffle(order)                      # seeded: who arrives first

    async def script(c: Cluster) -> None:
        for cmd in order:
            c.replicas[0].submit(cmd)               # submit to the leader
        while len(c.states[0].history) < 2:         # wait until both are committed
            await asyncio.sleep(0.02)

    asyncio.run(cluster.run(1.5, script, stop_when_done=True))
    return cluster


def winner(ledger: Ledger) -> str:
    return "B" if ledger.balances["B"] == 80 else ("C" if ledger.balances["C"] == 80 else "none")


def main() -> None:
    reset_clock()
    cluster = run_once(seed=1)

    ref = cluster.states[0]
    event("demo", "ledger", f"final balances (R0): {ref.balances}")
    for cmd, outcome in ref.history:
        event("demo", "apply", f"{cmd} -> {outcome}")

    # every replica reached the identical ledger
    assert cluster.check_convergence()
    assert all(s.balances == ref.balances for s in cluster.states), "replicas disagree on balances"
    oks = [o for _, o in ref.history if o == OK]
    assert len(oks) == 1, f"expected exactly one successful transfer, got {len(oks)}"
    assert ref.balances["A"] == 20 and ref.check_invariants()
    event("demo", "result", f"exactly one transfer succeeded; winner = bank {winner(ref)}")

    # ---- invariant sweep: 50 seeded runs, winners may differ, agreement always ----
    mute(True)
    wins = {"B": 0, "C": 0}
    for s in range(50):
        c = run_once(seed=s)
        r0 = c.states[0]
        assert c.check_convergence(), f"seed {s}: replicas diverged"
        assert all(st.balances == r0.balances for st in c.states), f"seed {s}: balances differ"
        assert len([o for _, o in r0.history if o == OK]) == 1, f"seed {s}: not exactly one OK"
        assert r0.check_invariants(), f"seed {s}: money not conserved"
        wins[winner(r0)] += 1
    mute(False)

    event("demo", "sweep", f"50 seeded runs: winner B={wins['B']} C={wins['C']}, "
                           f"exactly one OK and full agreement every time")
    event("demo", "PASS", "double-spend resolved by consensus ordering OK")


if __name__ == "__main__":
    main()
