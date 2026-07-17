"""M0 — extends / conflicts on a small hand-built tree.

        b0 ── b1 ── b2        (main branch)
                └── b3        (fork off b1)
"""
from hotstuff.tree import GENESIS, Tree
from hotstuff.types import Node


def _build():
    t = Tree()
    b1 = Node(GENESIS.hash, {"c": 1}, 1)
    b2 = Node(b1.hash, {"c": 2}, 2)
    b3 = Node(b1.hash, {"c": 3}, 3)  # sibling of b2
    for b in (b1, b2, b3):
        t.add(b)
    return t, b1, b2, b3


def test_extends_self_and_ancestors():
    t, b1, b2, b3 = _build()
    assert t.extends(b2, b2)       # a node extends itself
    assert t.extends(b2, b1)       # parent
    assert t.extends(b2, GENESIS)  # transitive to genesis
    assert t.extends(b3, b1)


def test_does_not_extend_across_fork():
    t, b1, b2, b3 = _build()
    assert not t.extends(b2, b3)
    assert not t.extends(b3, b2)
    assert not t.extends(b1, b2)   # ancestor does not extend its descendant


def test_conflicts_only_across_branches():
    t, b1, b2, b3 = _build()
    assert t.conflicts(b2, b3)          # different branches
    assert not t.conflicts(b2, b1)      # same branch (b2 extends b1)
    assert not t.conflicts(b2, GENESIS)
    assert not t.conflicts(b2, b2)


def test_broken_chain_stops_walk():
    # A node whose parent is absent from the tree extends only itself.
    t = Tree()
    orphan = Node("missing-parent-hash", {"c": 9}, 1)
    t.add(orphan)
    assert t.extends(orphan, orphan)
    assert not t.extends(orphan, GENESIS)
