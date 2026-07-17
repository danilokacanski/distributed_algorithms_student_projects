import pytest

from nbft.params import ConsensusParams


def test_paper_example_n17_m4():
    """The running example from the paper and the presentation: n=17, m=4."""
    p = ConsensusParams(n=17, m=4)
    assert p.f1 == 1
    assert p.R == 4
    assert p.E == 1
    assert p.w == 1
    assert p.T == 1 * 4 + (4 - 1) * 1 == 7
    assert p.tolerance_interval == (4, 7)
    assert p.sig_quorum == 3
    assert p.full_vote_quorum == 3
    assert p.vote_threshold == 12
    assert p.reply_quorum == 9
    assert p.grouped_count == 16
    assert p.ungrouped_count == 0
    # 1/3 of 17 is ~5.7, so the upper bound really exceeds a third.
    assert p.T > 17 / 3


def test_fnd_experiment_size_n101_m4():
    p = ConsensusParams(n=101, m=4)
    assert p.R == 25
    assert p.E == 1
    assert p.w == 8
    assert p.T == 8 * 4 + 17 * 1 == 49
    assert p.T > 101 / 3


def test_group_size_must_be_3f_plus_1():
    for bad_m in (3, 5, 6, 8, 9):
        with pytest.raises(ValueError):
            ConsensusParams(n=100, m=bad_m)


def test_at_least_four_groups_required():
    with pytest.raises(ValueError):
        ConsensusParams(n=13, m=4)  # R = 3
    ConsensusParams(n=17, m=4)  # R = 4 is fine


def test_ungrouped_nodes():
    p = ConsensusParams(n=19, m=4)
    assert p.R == 4
    assert p.ungrouped_count == 2
