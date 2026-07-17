from nbft.hashring import GENESIS_HASH, HashRing
from nbft.messages import sign
from nbft.params import ConsensusParams
from nbft.voting import VoteLedger

P = ConsensusParams(n=17, m=4)
RING = HashRing([f"10.0.0.{i}" for i in range(1, 18)])
MS = RING.membership(0, GENESIS_HASH, None, P.m, P.R)
D = "deadbeefcafe0123"


def sigs_of(group: int, count: int, digest: str = D) -> tuple[str, ...]:
    return tuple(sign(nid, digest) for nid in MS.groups[group][:count])


def test_full_aggregate_counts_as_m_votes():
    ledger = VoteLedger(P, MS)
    f = ledger.add_group_aggregate(0, D, sigs_of(0, 3))
    assert f == 3 == P.full_vote_quorum
    assert ledger.group_votes(0, D) == P.m == 4


def test_weak_aggregate_counts_actual_signatures():
    ledger = VoteLedger(P, MS)
    f = ledger.add_group_aggregate(0, D, sigs_of(0, 2))
    assert f == 2
    assert ledger.group_votes(0, D) == 2


def test_individual_broadcasts_merge_without_double_counting():
    ledger = VoteLedger(P, MS)
    ledger.add_group_aggregate(0, D, sigs_of(0, 2))
    # The same member also broadcasts on its own (Model 1) - still 2 votes.
    ledger.add_group_individual(0, D, (sign(MS.groups[0][0], D),))
    assert ledger.group_votes(0, D) == 2
    # A third distinct member joins - 3 votes, still below the full m.
    ledger.add_group_individual(0, D, (sign(MS.groups[0][2], D),))
    assert ledger.group_votes(0, D) == 3


def test_foreign_and_invalid_signatures_are_rejected():
    ledger = VoteLedger(P, MS)
    outsider = MS.groups[1][0]  # not a member of group 0
    ledger.add_group_aggregate(0, D, (sign(outsider, D), sign(MS.groups[0][0], "wrong-digest")))
    assert ledger.group_votes(0, D) == 0


def test_ungrouped_replica_is_one_vote():
    p19 = ConsensusParams(n=19, m=4)
    ring = HashRing([f"10.0.0.{i}" for i in range(1, 20)])
    ms = ring.membership(0, GENESIS_HASH, None, p19.m, p19.R)
    assert ms.ungrouped
    ledger = VoteLedger(p19, ms)
    ledger.add_ungrouped(ms.ungrouped[0], D, (sign(ms.ungrouped[0], D),))
    assert ledger.votes_for(D) == 1


def test_network_threshold():
    ledger = VoteLedger(P, MS)
    for g in range(3):
        ledger.add_group_aggregate(g, D, sigs_of(g, 3))
    # 3 full groups = 12 votes = (R - w) * m -> threshold met.
    assert ledger.votes_for(D) == 12 == P.vote_threshold
    assert ledger.threshold_met(D)
    # Proof carries every distinct signer seen so far.
    assert len(ledger.proof_signatures(D)) == 9
