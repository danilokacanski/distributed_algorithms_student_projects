"""M3 — ledger apply() is deterministic; double-spend resolves to exactly one OK
and identical state on all replicas across seeds."""
from bank.ledger import INSUFFICIENT, NONCE_REUSED, OK, Ledger, Transfer
from demos.demo3_double_spend import OK as _OK, run_once, winner


def test_apply_basic_and_insufficient():
    lg = Ledger({"A": 100, "B": 0})
    assert lg.apply(Transfer("A", "B", 80, 1)) == OK
    assert lg.balances == {"A": 20, "B": 80}
    # second 80 no longer affordable
    assert lg.apply(Transfer("A", "B", 80, 2)) == INSUFFICIENT
    assert lg.balances == {"A": 20, "B": 80}          # unchanged on reject
    assert lg.check_invariants()


def test_nonce_replay_rejected():
    lg = Ledger({"A": 100, "B": 0})
    assert lg.apply(Transfer("A", "B", 10, 1)) == OK
    assert lg.apply(Transfer("A", "B", 10, 1)) == NONCE_REUSED   # same nonce
    assert lg.balances == {"A": 90, "B": 10}


def test_apply_is_order_dependent_but_deterministic():
    # Same two commands, two orders → different results, each deterministic.
    a = Ledger({"A": 100, "B": 0, "C": 0})
    a.apply(Transfer("A", "B", 80, 1)); a.apply(Transfer("A", "C", 80, 2))
    b = Ledger({"A": 100, "B": 0, "C": 0})
    b.apply(Transfer("A", "C", 80, 2)); b.apply(Transfer("A", "B", 80, 1))
    assert a.balances == {"A": 20, "B": 80, "C": 0}
    assert b.balances == {"A": 20, "B": 0, "C": 80}


def test_double_spend_consensus_agreement_sweep():
    wins = {"B": 0, "C": 0, "none": 0}
    for seed in range(12):
        c = run_once(seed)
        r0 = c.states[0]
        assert c.check_convergence(), f"seed {seed}: diverged"
        assert all(s.balances == r0.balances for s in c.states), f"seed {seed}: balances differ"
        assert [o for _, o in r0.history].count(_OK) == 1, f"seed {seed}: not exactly one OK"
        assert r0.balances["A"] == 20 and r0.check_invariants()
        wins[winner(r0)] += 1
    assert wins["none"] == 0
    assert wins["B"] > 0 and wins["C"] > 0, f"winner never varied: {wins}"
