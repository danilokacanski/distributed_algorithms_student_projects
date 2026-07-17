"""The block tree — paper §4.2, plus `extends`/`conflicts` used by safeNode.

Every replica keeps its own copy. Nodes are keyed by hash; ancestry is followed
by walking `parent_hash` links up to the genesis block b0.
"""
from __future__ import annotations

from typing import Optional

from .types import QC, MsgType, Node

# Genesis block b0 — paper §6 bootstrap. Hard-coded and identical on every
# replica; it is its own root (parent_hash = None, view 0).
GENESIS = Node(parent_hash=None, cmd=None, view_number=0)

# b0 carries a hard-coded "self QC" so the very first PREPARE has a justify to
# point at. It has no signers — it is trusted by construction, never verified.
GENESIS_QC = QC(type=MsgType.PREPARE, view_number=0, node_hash=GENESIS.hash, sigs=())


class Tree:
    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {GENESIS.hash: GENESIS}

    def add(self, node: Node) -> None:
        self.nodes[node.hash] = node

    def get(self, node_hash: Optional[str]) -> Optional[Node]:
        return self.nodes.get(node_hash) if node_hash else None

    def has(self, node_hash: str) -> bool:
        return node_hash in self.nodes

    def extends(self, node: Node, ancestor: Node) -> bool:
        """True iff `ancestor` lies on the parent-chain from `node` up to b0
        (a node extends itself). Missing links stop the walk → False."""
        cur: Optional[Node] = node
        while cur is not None:
            if cur.hash == ancestor.hash:
                return True
            cur = self.get(cur.parent_hash)
        return False

    def conflicts(self, a: Node, b: Node) -> bool:
        """Two nodes conflict iff neither extends the other — i.e. they sit on
        different branches. Committing conflicting nodes is the safety violation
        HotStuff forbids (Theorem 2)."""
        return not self.extends(a, b) and not self.extends(b, a)
