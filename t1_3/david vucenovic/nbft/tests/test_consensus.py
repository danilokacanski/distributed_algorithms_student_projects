"""End-to-end simulations: the full protocol under different fault loads.

All runs use tiny delays/timeouts so the whole suite stays fast, and
`trace_level="quiet"` so pytest output stays clean.
"""

from nbft.config import SimulationConfig
from nbft.messages import MsgType
from nbft.simulator import simulate

FAST = dict(
    base_delay_ms=1.0,
    jitter_ms=2.0,
    loss_rate=0.0,
    phase_timeout_ms=150.0,
    client_timeout_ms=900.0,
    trace_level="quiet",
)


def test_happy_path_two_rounds():
    cfg = SimulationConfig(n=17, m=4, seed=1, rounds=2, **FAST)
    result = simulate(cfg)
    assert result.success
    assert result.committed_per_round == [17, 17]
    assert result.final_view == 0
    # Empirical traffic matches Formula 4: C = 2(n-1) + 2(m-1)R + R^2 = 72.
    consensus = sum(
        c for t, c in result.sent.items() if t not in (MsgType.REQUEST, MsgType.REPLY)
    )
    assert consensus == 2 * 72


def test_crashed_members_are_tolerated():
    cfg = SimulationConfig(
        n=17, m=4, seed=2, byz_count=2, byz_behavior="crash", byz_target="member", **FAST
    )
    result = simulate(cfg)
    assert result.success
    assert result.committed_per_round[0] == 15  # every honest node commits


def test_crashed_representatives_are_blocked_and_bypassed():
    cfg = SimulationConfig(
        n=17, m=4, seed=3, byz_count=2, byz_behavior="crash", byz_target="representative", **FAST
    )
    result = simulate(cfg)
    assert result.success


def test_low_sig_representative_triggers_model1():
    cfg = SimulationConfig(
        n=17, m=4, seed=4, byz_count=1, byz_behavior="low_sig", byz_target="representative", **FAST
    )
    result = simulate(cfg)
    assert result.success


def test_equivocating_members_cannot_break_consensus():
    cfg = SimulationConfig(
        n=17, m=4, seed=5, byz_count=4, byz_behavior="equivocate", byz_target="member", **FAST
    )
    result = simulate(cfg)
    assert result.success
    assert result.chains_consistent


def test_silent_leader_forces_view_change():
    cfg = SimulationConfig(
        n=17, m=4, seed=6, byz_count=1, byz_behavior="silent_leader", byz_target="primary", **FAST
    )
    result = simulate(cfg)
    assert result.success
    assert result.final_view >= 1  # the network had to move to a new view
    assert result.outcomes[0].attempts >= 2  # the client had to alert the nodes


def test_lossy_network_still_decides():
    cfg = SimulationConfig(n=17, m=4, seed=7, **{**FAST, "loss_rate": 0.05})
    result = simulate(cfg)
    assert result.success


def test_too_many_crashes_break_consensus():
    # 10 crashed nodes leave only 7 honest ones - below the reply quorum of 9,
    # far beyond the tolerance interval upper bound T = 7.
    cfg = SimulationConfig(
        n=17,
        m=4,
        seed=8,
        byz_count=10,
        byz_behavior="crash",
        byz_target="random",
        **{**FAST, "client_timeout_ms": 400.0, "client_retries": 2},
    )
    result = simulate(cfg)
    assert not result.success
