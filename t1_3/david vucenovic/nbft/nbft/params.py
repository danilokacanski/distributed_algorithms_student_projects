"""Consensus parameters of the NBFT algorithm.

All quantities follow Section III of the paper:

    n  - total number of nodes in the network
    m  - group size, m = 3*f1 + 1
    R  - number of groups, R = floor((n - 1) / m), R >= 4
    E  - max Byzantine nodes tolerated inside one group, E = floor((m - 1) / 3)
    w  - max groups with abnormal consensus, w = floor((R - 1) / 3)
    T  - upper bound of the fault-tolerance interval, T = w*m + (R - w)*E

The fault-tolerance of NBFT is therefore the interval [R, T] rather than a
single value as in PBFT.
"""

from __future__ import annotations


from dataclasses import dataclass


@dataclass(frozen=True)
class ConsensusParams:
    """Derived NBFT parameters for a network of `n` nodes grouped by `m`."""

    n: int
    m: int

    def __post_init__(self) -> None:
        if self.m < 4 or (self.m - 1) % 3 != 0:
            raise ValueError(
                f"group size m={self.m} is invalid: m must satisfy m = 3*f1 + 1 (4, 7, 10, ...)"
            )
        if self.R < 4:
            raise ValueError(
                f"n={self.n}, m={self.m} gives R={self.R} groups; the paper requires R >= 4 "
                f"(need n >= {4 * self.m + 1})"
            )

    @property
    def f1(self) -> int:
        """Byzantine budget used to size a group (m = 3*f1 + 1)."""
        return (self.m - 1) // 3

    @property
    def R(self) -> int:
        """Number of consensus groups."""
        return (self.n - 1) // self.m

    @property
    def E(self) -> int:
        """Max Byzantine nodes tolerated inside a single group."""
        return (self.m - 1) // 3

    @property
    def w(self) -> int:
        """Max groups whose consensus may fail without breaking the network."""
        return (self.R - 1) // 3

    @property
    def T(self) -> int:
        """Upper bound of the fault-tolerance interval (Formula 1)."""
        return self.w * self.m + (self.R - self.w) * self.E

    @property
    def tolerance_interval(self) -> tuple[int, int]:
        """The NBFT fault-tolerance interval [R, T]."""
        return (self.R, self.T)

    @property
    def sig_quorum(self) -> int:
        """Signatures a representative must aggregate in in-prepare2 (2E + 1)."""
        return 2 * self.E + 1

    @property
    def full_vote_quorum(self) -> int:
        """Valid signatures needed for a group to count as m votes (F >= m - E)."""
        return self.m - self.E

    @property
    def vote_threshold(self) -> int:
        """Network-wide vote threshold in out-prepare: H >= (R - w) * m."""
        return (self.R - self.w) * self.m

    @property
    def reply_quorum(self) -> int:
        """Replies the client needs to accept the result: (n - 1) / 2 + 1."""
        return (self.n - 1) // 2 + 1

    @property
    def grouped_count(self) -> int:
        """Nodes that end up inside groups (the primary stays outside)."""
        return self.R * self.m

    @property
    def ungrouped_count(self) -> int:
        """Nodes left without a group (ignored by the paper's analysis)."""
        return self.n - 1 - self.grouped_count

    def describe(self) -> str:
        return (
            f"n={self.n} m={self.m} | R={self.R} groups, E={self.E}/group, "
            f"w={self.w} faulty groups | tolerance [{self.R}, {self.T}] | "
            f"vote threshold {self.vote_threshold}, reply quorum {self.reply_quorum}"
        )
