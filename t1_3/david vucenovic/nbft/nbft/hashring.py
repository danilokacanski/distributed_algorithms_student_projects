"""Consistent-hash ring used for grouping and role election (Section III-B).

Every node is mapped onto a ring of size 2^32 through hash(node_ip).
Roles are never assigned manually - they all fall out of hash values:

    hash(node_ip)                              -> position on the ring / group
    hash(master_ip + previous_hash + view)     -> primary of the round
    hash(master_ip + view + group_number)      -> representative of a group

Because the inputs include the previous block hash and the view number,
nobody can predict in advance which node will hold which role, which is
exactly what limits the coordination power of Byzantine nodes.
"""

from __future__ import annotations


import hashlib
from dataclasses import dataclass

RING_SPACE = 2**32

GENESIS_HASH = hashlib.sha256(b"nbft-genesis").hexdigest()


def ring_hash(key: str) -> int:
    """Map an arbitrary string to a position on the ring [0, 2^32)."""
    digest = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(digest[:8], "big") % RING_SPACE


@dataclass(frozen=True)
class Membership:
    """Roles of every node for one (view, previous_hash) pair."""

    view: int
    primary: str
    groups: tuple[tuple[str, ...], ...]
    representatives: tuple[str, ...]
    ungrouped: tuple[str, ...]

    def group_of(self, node_id: str) -> int | None:
        for g, members in enumerate(self.groups):
            if node_id in members:
                return g
        return None

    def role_of(self, node_id: str) -> str:
        if node_id == self.primary:
            return "primary"
        if node_id in self.representatives:
            return "representative"
        if self.group_of(node_id) is not None:
            return "member"
        return "ungrouped"

    def all_nodes(self) -> tuple[str, ...]:
        nodes = [self.primary]
        for members in self.groups:
            nodes.extend(members)
        nodes.extend(self.ungrouped)
        return tuple(nodes)


class HashRing:
    def __init__(self, node_ids: list[str]):
        if len(set(node_ids)) != len(node_ids):
            raise ValueError("duplicate node ids on the ring")
        self.positions = {nid: ring_hash(nid) for nid in node_ids}
        # Ties on position are broken by the id itself so the order is total.
        self.ordered = sorted(node_ids, key=lambda nid: (self.positions[nid], nid))

    def _clockwise_from(self, point: int, candidates: list[str]) -> str:
        """First candidate at or after `point`, wrapping around the ring."""
        pool = sorted(candidates, key=lambda nid: (self.positions[nid], nid))
        for nid in pool:
            if self.positions[nid] >= point:
                return nid
        return pool[0]

    def elect_primary(self, view: int, previous_hash: str, prev_master: str | None) -> str:
        """hash(master_ip + previous_hash + view_number) -> nearest clockwise node.

        For the very first grouping (view 0, no previous primary) the paper
        takes the first node in the clockwise direction of the ring.
        """
        if prev_master is None and view == 0:
            return self.ordered[0]
        point = ring_hash(f"{prev_master or ''}|{previous_hash}|{view}")
        return self._clockwise_from(point, self.ordered)

    def membership(self, view: int, previous_hash: str, prev_master: str | None, m: int, r: int) -> Membership:
        """Compute primary, groups, and representatives for one round."""
        primary = self.elect_primary(view, previous_hash, prev_master)

        # Walk the ring clockwise starting right after the primary, skipping
        # the primary itself; the paper shifts the start by floor(view / n).
        start = self.ordered.index(primary)
        others = [self.ordered[(start + 1 + i) % len(self.ordered)] for i in range(len(self.ordered) - 1)]
        offset = (view // len(self.ordered)) % len(others)
        others = others[offset:] + others[:offset]

        groups = tuple(tuple(others[g * m : (g + 1) * m]) for g in range(r))
        ungrouped = tuple(others[r * m :])

        # Representative of each group: hash(master_ip + view + group_number)
        # projected onto the sub-ring formed by the group.
        representatives = tuple(
            self._clockwise_from(ring_hash(f"{primary}|{view}|{g}"), list(members))
            for g, members in enumerate(groups)
        )
        return Membership(
            view=view,
            primary=primary,
            groups=groups,
            representatives=representatives,
            ungrouped=ungrouped,
        )
