"""M1 — the safety predicate safeNode in isolation (Algorithm 1, lines 25–27).

    b0 ── b1(v1) ── b2(v3)     b1 is where we LOCK
              └──── b3(v3)     b3 conflicts with b1's descendant b2
"""
from hotstuff.crypto import gen_keys
from hotstuff.network import Network
from hotstuff.pacemaker import Pacemaker
from hotstuff.replica import Replica
from hotstuff.tree import GENESIS, GENESIS_QC
from hotstuff.types import QC, MsgType, Node

N, F = 4, 1


def _replica():
    sks, vks = gen_keys(N)
    return Replica(0, N, F, Network(N), sks[0], vks, Pacemaker(N))


def _qc(view, node_hash):
    return QC(MsgType.PRE_COMMIT, view, node_hash)


def test_safe_when_locked_on_genesis():
    r = _replica()
    b1 = Node(GENESIS.hash, {"c": 1}, 1)
    r.tree.add(b1)
    # locked on genesis → everything extends genesis → always safe
    assert r.safe_node(b1, _qc(0, GENESIS.hash))


def test_safety_rule_extends_locked_node():
    r = _replica()
    b1 = Node(GENESIS.hash, {"c": 1}, 1)
    b2 = Node(b1.hash, {"c": 2}, 3)
    r.tree.add(b1); r.tree.add(b2)
    r.locked_qc = _qc(2, b1.hash)          # locked on b1 at view 2
    # b2 extends b1 → safe via the safety rule even though qc.view (1) < lock (2)
    assert r.safe_node(b2, _qc(1, b1.hash))


def test_unsafe_conflicting_and_not_higher():
    r = _replica()
    b1 = Node(GENESIS.hash, {"c": 1}, 1)
    b3 = Node(b1.hash, {"c": 3}, 3)        # sibling branch off b1
    b2 = Node(b1.hash, {"c": 2}, 3)
    r.tree.add(b1); r.tree.add(b2); r.tree.add(b3)
    r.locked_qc = _qc(5, b2.hash)          # locked on b2
    # b3 conflicts with b2 and its justify view (4) is not above the lock (5) → UNSAFE
    assert not r.safe_node(b3, _qc(4, b1.hash))


def test_liveness_rule_unlocks_on_higher_qc():
    r = _replica()
    b1 = Node(GENESIS.hash, {"c": 1}, 1)
    b2 = Node(b1.hash, {"c": 2}, 3)
    b3 = Node(b1.hash, {"c": 3}, 3)
    r.tree.add(b1); r.tree.add(b2); r.tree.add(b3)
    r.locked_qc = _qc(5, b2.hash)
    # b3 conflicts with b2, but a QC from a HIGHER view (6) lets us switch (liveness)
    assert r.safe_node(b3, _qc(6, b1.hash))
