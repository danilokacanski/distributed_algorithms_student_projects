"""Model 2 - the threshold vote-counting model (Section III-D).

Counting rules in the out-prepare stage:

    * a representative's aggregate with F >= m - E valid signatures
      counts as the full m votes of its group,
    * otherwise the group contributes as many votes as distinct valid
      signers were actually seen for that digest (aggregates and the
      individual broadcasts of members who blocked their representative
      are merged, so nobody is ever counted twice),
    * an ungrouped replica contributes 1 vote.

The network reaches consensus on a digest once

    H >= (R - w) * m.
"""

from __future__ import annotations


from collections import defaultdict

from .hashring import Membership
from .messages import sig_signer, sig_valid
from .params import ConsensusParams


class VoteLedger:
    def __init__(self, params: ConsensusParams, membership: Membership):
        self.params = params
        self.membership = membership
        # (group, digest) -> best aggregate size seen from the representative
        self._aggregate_best: dict[tuple[int, str], int] = defaultdict(int)
        # (group, digest) -> distinct member ids whose valid signature was seen
        self._group_signers: dict[tuple[int, str], set[str]] = defaultdict(set)
        # digest -> distinct ungrouped node ids
        self._ungrouped: dict[str, set[str]] = defaultdict(set)

    def _valid_group_signers(self, group: int, digest: str, signatures: tuple[str, ...]) -> set[str]:
        members = set(self.membership.groups[group])
        return {
            sig_signer(sig)
            for sig in signatures
            if sig_valid(sig, digest) and sig_signer(sig) in members
        }

    def add_group_aggregate(self, group: int, digest: str, signatures: tuple[str, ...]) -> int:
        """Aggregate forwarded by the representative of `group`. Returns F."""
        signers = self._valid_group_signers(group, digest, signatures)
        self._group_signers[(group, digest)] |= signers
        f = len(signers)
        self._aggregate_best[(group, digest)] = max(self._aggregate_best[(group, digest)], f)
        return f

    def add_group_individual(self, group: int, digest: str, signatures: tuple[str, ...]) -> None:
        """Broadcast of a member that blocked its representative (Model 1)."""
        self._group_signers[(group, digest)] |= self._valid_group_signers(group, digest, signatures)

    def add_ungrouped(self, node_id: str, digest: str, signatures: tuple[str, ...]) -> None:
        if any(sig_valid(sig, digest) and sig_signer(sig) == node_id for sig in signatures):
            self._ungrouped[digest].add(node_id)

    def group_votes(self, group: int, digest: str) -> int:
        if self._aggregate_best[(group, digest)] >= self.params.full_vote_quorum:
            return self.params.m
        return len(self._group_signers[(group, digest)])

    def votes_for(self, digest: str) -> int:
        total = sum(self.group_votes(g, digest) for g in range(len(self.membership.groups)))
        return total + len(self._ungrouped[digest])

    def threshold_met(self, digest: str) -> bool:
        return self.votes_for(digest) >= self.params.vote_threshold

    def proof_signatures(self, digest: str) -> tuple[str, ...]:
        """All collected valid signatures consistent with `digest`."""
        from .messages import sign

        signers: set[str] = set()
        for (_, d), ids in self._group_signers.items():
            if d == digest:
                signers |= ids
        signers |= self._ungrouped[digest]
        return tuple(sorted(sign(nid, digest) for nid in signers))
