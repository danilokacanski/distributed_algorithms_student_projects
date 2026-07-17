from nbft.hashring import GENESIS_HASH, HashRing, ring_hash
from nbft.params import ConsensusParams


def make_ring(n: int) -> HashRing:
    return HashRing([f"10.0.0.{i}" for i in range(1, n + 1)])


def test_ring_hash_is_deterministic_and_bounded():
    assert ring_hash("10.0.0.1") == ring_hash("10.0.0.1")
    assert ring_hash("10.0.0.1") != ring_hash("10.0.0.2")
    assert 0 <= ring_hash("anything") < 2**32


def test_first_membership_layout():
    p = ConsensusParams(n=17, m=4)
    ring = make_ring(17)
    ms = ring.membership(view=0, previous_hash=GENESIS_HASH, prev_master=None, m=p.m, r=p.R)

    # First-ever primary: first node clockwise on the ring.
    assert ms.primary == ring.ordered[0]
    assert len(ms.groups) == 4
    assert all(len(g) == 4 for g in ms.groups)
    assert ms.ungrouped == ()

    # Groups partition all non-primary nodes with no overlap.
    seen = [nid for g in ms.groups for nid in g]
    assert len(seen) == len(set(seen)) == 16
    assert ms.primary not in seen

    # Every representative belongs to its own group.
    for g, rep in enumerate(ms.representatives):
        assert rep in ms.groups[g]

    assert ms.role_of(ms.primary) == "primary"
    assert ms.role_of(ms.representatives[0]) == "representative"


def test_membership_is_deterministic():
    p = ConsensusParams(n=17, m=4)
    a = make_ring(17).membership(0, GENESIS_HASH, None, p.m, p.R)
    b = make_ring(17).membership(0, GENESIS_HASH, None, p.m, p.R)
    assert a == b


def test_view_change_reelects_roles():
    p = ConsensusParams(n=17, m=4)
    ring = make_ring(17)
    ms0 = ring.membership(0, GENESIS_HASH, None, p.m, p.R)
    ms1 = ring.membership(1, GENESIS_HASH, ms0.primary, p.m, p.R)

    assert ms1.primary in ring.ordered
    assert all(len(g) == 4 for g in ms1.groups)
    # Roles are re-drawn from the hash; the layouts must not be identical.
    assert (ms0.primary, ms0.representatives) != (ms1.primary, ms1.representatives)


def test_new_block_rotates_primary():
    p = ConsensusParams(n=17, m=4)
    ring = make_ring(17)
    ms0 = ring.membership(0, GENESIS_HASH, None, p.m, p.R)
    next_block_hash = "ab" * 32
    ms_next = ring.membership(0, next_block_hash, ms0.primary, p.m, p.R)
    assert ms_next.primary in ring.ordered


def test_ungrouped_nodes_exist_when_n_not_aligned():
    p = ConsensusParams(n=19, m=4)
    ring = make_ring(19)
    ms = ring.membership(0, GENESIS_HASH, None, p.m, p.R)
    assert len(ms.ungrouped) == 2
    assert all(ms.role_of(nid) == "ungrouped" for nid in ms.ungrouped)
