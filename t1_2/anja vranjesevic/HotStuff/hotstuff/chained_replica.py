"""Chained HotStuff — Algorithm 3 (paper §5). The "better project" variant.

Basic HotStuff spends four rounds to commit one command. Chained HotStuff keeps
the *same* messages but pipelines them: there is a single GENERIC phase per view,
and one view's votes are the next view's proposal justification. Each proposal's
QC simultaneously acts as the PREPARE of its own node, the PRE-COMMIT of its
parent, the COMMIT of its grandparent. A node commits once it heads a
**three-chain** of consecutive views:

    One-chain  (b*  ->  b'')            update generic_qc     (PRE-COMMIT)
    Two-chain  (b*  ->  b'' -> b')      lock  b'              (COMMIT)
    Three-chain(b* -> b'' -> b' -> b)   commit b              (DECIDE)

Everything else — types, crypto, tree, network, pacemaker, the bank ledger — is
reused unchanged; only this replica logic differs. Interface matches `Replica`
so the same Cluster harness, client and safety checker drive it.
"""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import Any, Callable, Optional

from .crypto import build_qc, sign_vote, verify_qc, verify_vote
from .eventlog import qc_dict
from .log import event
from .network import Network
from .pacemaker import Pacemaker
from .replica import cmd_key
from .tree import GENESIS, GENESIS_QC, Tree
from .types import QC, Msg, MsgType, Node


class ChainedReplica:
    def __init__(self, node_id: int, n: int, f: int, net: Network,
                 signing_key, verify_keys, pacemaker: Pacemaker,
                 state_machine: Any = None,
                 on_execute: Optional[Callable[[int, Any, Any], None]] = None):
        self.id = node_id
        self.n, self.f = n, f
        self.quorum = n - f
        self.net = net
        self.sk = signing_key
        self.vks = verify_keys
        self.pm = pacemaker
        self.sm = state_machine
        self._respond = on_execute

        self.view = 0
        self.tree = Tree()
        self.generic_qc: QC = GENESIS_QC     # highest QC seen (prepareQC analog)
        self.locked_node: Node = GENESIS     # node we are locked on (safety)
        self.committed: list[Node] = []
        self._executed: set[str] = set()

        self._pending: deque = deque()
        self._pending_keys: set[str] = set()
        self._committed_cmds: set[str] = set()
        self._branch_cmds: dict[str, set[str]] = {GENESIS.hash: set()}   # cmds present per branch
        self._votes: dict[tuple, dict[int, Msg]] = defaultdict(dict)
        self._new_view: dict[int, dict[int, QC]] = defaultdict(dict)
        self._formed: set[tuple] = set()
        self._proposed: set[int] = set()
        self._voted: set[int] = set()
        self._left: set[int] = set()
        self._failures = 0
        self._timer: Optional[asyncio.TimerHandle] = None
        self.qcs: list[tuple] = []
        self.running = True

    # ------------------------------------------------------------------ lifecycle
    async def run(self) -> None:
        event(f"C{self.id}", "start", f"leader(1)={self.pm.leader(1)}")
        self._advance_from(0)
        while self.running:
            self.dispatch(await self.net.recv(self.id))

    def stop(self) -> None:
        self.running = False
        if self._timer is not None:
            self._timer.cancel(); self._timer = None

    def submit(self, cmd: Any) -> None:
        k = cmd_key(cmd)
        if k in self._pending_keys or k in self._committed_cmds:
            return
        self._pending.append(cmd); self._pending_keys.add(k)
        self._try_propose(self.view, self.generic_qc)

    # ------------------------------------------------------------------ dispatch
    def dispatch(self, m: Msg) -> None:
        if m.type == MsgType.NEW_VIEW:
            self.on_new_view(m)
        elif m.type == MsgType.GENERIC:
            self.on_generic_vote(m) if m.partial_sig is not None else self.on_generic(m)

    def _record_qc(self, qc: QC) -> None:
        if qc is not None and len(qc.sigs) > 0:
            self.qcs.append((qc.type, qc.view_number, qc.node_hash))

    def _verify_justify(self, qc: Optional[QC]) -> bool:
        genesis = qc is not None and qc.view_number == 0 and len(qc.sigs) == 0
        return genesis or (qc is not None and verify_qc(qc, self.vks, self.quorum))

    # ================================================================ NEW-VIEW (bootstrap / timeout)
    def on_new_view(self, m: Msg) -> None:
        start = m.view_number + 1
        if not self.pm.is_leader(start, self.id) or not self._verify_justify(m.justify):
            return
        self._new_view[start][m.sender] = m.justify or GENESIS_QC
        if len(self._new_view[start]) >= self.quorum:
            high = max(self._new_view[start].values(), key=lambda q: q.view_number)
            self._try_propose(start, high)

    # ================================================================ PROPOSE (leader)
    def _try_propose(self, view: int, justify: QC) -> None:
        """createLeaf on the highest QC and broadcast one GENERIC proposal."""
        if view in self._proposed or not self.pm.is_leader(view, self.id):
            return
        if not self._pending:
            return
        cmd = self._next_command(justify.node_hash)
        if cmd is None:
            return
        leaf = Node(parent_hash=justify.node_hash, cmd=cmd, view_number=view, justify=justify)
        self.tree.add(leaf)
        self._register_branch(leaf)
        self._proposed.add(view)
        event(f"C{self.id}", "PROPOSE", f"view={view} node={leaf.short()} cmd={cmd} justify.view={justify.view_number}",
              view=view, phase="generic", node=leaf.short(),
              parent=(leaf.parent_hash[:6] if leaf.parent_hash else None), cmd=str(cmd), qc=qc_dict(justify))
        self.net.broadcast(Msg(MsgType.GENERIC, view, self.id, node=leaf, justify=justify))

    def _register_branch(self, node: Node) -> None:
        """Record which commands already appear on the branch ending at `node`,
        so a leader never re-proposes a command still in flight on that branch
        (pipelining means a command isn't committed until 3 views later)."""
        base = self._branch_cmds.get(node.parent_hash)
        if base is None:                       # parent unknown yet — walk to reconstruct
            base = set()
            cur = self.tree.get(node.parent_hash)
            while cur is not None and cur.hash != GENESIS.hash:
                if cur.cmd is not None:
                    base.add(cmd_key(cur.cmd))
                cur = self.tree.get(cur.parent_hash)
        self._branch_cmds[node.hash] = base | ({cmd_key(node.cmd)} if node.cmd is not None else set())

    def _next_command(self, parent_hash: str) -> Optional[Any]:
        """Oldest pending command not already committed and not already on the
        branch we are about to extend."""
        on_branch = self._branch_cmds.get(parent_hash, set())
        skipped = []
        chosen = None
        while self._pending:
            cmd = self._pending.popleft()
            k = cmd_key(cmd)
            if k in self._committed_cmds:
                self._pending_keys.discard(k); continue
            if k in on_branch:
                skipped.append(cmd); continue      # already in flight on this branch
            chosen = cmd; break
        for cmd in reversed(skipped):
            self._pending.appendleft(cmd)
        return chosen

    # ================================================================ GENERIC (replica)
    def on_generic(self, m: Msg) -> None:
        """Replica side of the single phase: maybe vote, then run the chained
        update rule (one/two/three-chain) — Algorithm 3."""
        if m.sender != self.pm.leader(m.view_number) or not self._verify_justify(m.justify):
            return
        b_star = m.node
        self.tree.add(b_star)
        self._register_branch(b_star)
        if m.view_number > self.view:
            self._enter(m.view_number)

        # vote iff safeNode, sending the vote to the NEXT leader (pipelining)
        if m.view_number not in self._voted and self.safe_node(b_star, m.justify):
            self._voted.add(m.view_number)
            nxt = self.pm.leader(m.view_number + 1)
            event(f"C{self.id}", "vote GENERIC", f"view={m.view_number} node={b_star.short()} -> leader {nxt}",
                  view=m.view_number, phase="generic", node=b_star.short(), frm=self.id, to=nxt)
            self.net.send(nxt, Msg(MsgType.GENERIC, m.view_number, self.id, node=b_star,
                                   partial_sig=(self.id, sign_vote(self.sk, MsgType.GENERIC,
                                                                   m.view_number, b_star.hash))))
        self.update(b_star)

    def on_generic_vote(self, m: Msg) -> None:
        """Leader of view (m.view+1) collects GENERIC votes for m.view → QC →
        proposes the next node justified by that QC."""
        rid, sig = m.partial_sig
        node = m.node
        start = m.view_number + 1
        if node is None or not self.pm.is_leader(start, self.id):
            return
        if not verify_vote(self.vks[rid], MsgType.GENERIC, m.view_number, node.hash, sig):
            return
        key = (m.view_number, node.hash)
        if key in self._formed:
            return
        self._votes[key][rid] = m
        if len(self._votes[key]) >= self.quorum:
            self._formed.add(key)
            qc = build_qc(list(self._votes[key].values()), self.quorum)
            self._record_qc(qc)
            event(f"C{self.id}", "QC GENERIC", f"view={m.view_number} node={node.short()} -> propose view {start}",
                  view=m.view_number, phase="generic", node=node.short(), qc=qc_dict(qc))
            self._try_propose(start, qc)

    # ================================================================ chained update rule
    def update(self, b_star: Node) -> None:
        """One-/two-/three-chain updates — the heart of Algorithm 3 (§5)."""
        b2 = self.tree.get(b_star.justify.node_hash)        # b''  (b_star -> b'')
        if b2 is None:
            return
        # PRE-COMMIT (one-chain): adopt the newest QC as generic_qc
        if b_star.justify.view_number > self.generic_qc.view_number:
            self.generic_qc = b_star.justify
        b1 = self.tree.get(b2.justify.node_hash) if b2.justify else None   # b'
        b0 = self.tree.get(b1.justify.node_hash) if (b1 and b1.justify) else None   # b

        # COMMIT (two-chain): lock on b'
        if b1 is not None and b1.view_number > self.locked_node.view_number:
            self.locked_node = b1
            event(f"C{self.id}", "LOCK", f"node={b1.short()} view={b1.view_number}",
                  view=b1.view_number, phase="commit", node=b1.short(), locked_qc_view=b1.view_number)

        # DECIDE (three-chain of consecutive views): commit b
        if (b0 is not None and b1 is not None
                and b1.view_number == b0.view_number + 1
                and b2.view_number == b1.view_number + 1):
            self.commit(b0)

    def safe_node(self, node: Node, qc: QC) -> bool:
        """safeNode — same rule as Basic (Algorithm 1, 25–27), qc certifies the
        node's parent."""
        return self.tree.extends(node, self.locked_node) or qc.view_number > self.locked_node.view_number

    def commit(self, node: Node) -> None:
        chain = []
        cur: Optional[Node] = node
        while cur is not None and cur.hash != GENESIS.hash and cur.hash not in self._executed:
            chain.append(cur); cur = self.tree.get(cur.parent_hash)
        for b in reversed(chain):
            self._executed.add(b.hash); self.committed.append(b)
            result = self.sm.apply(b.cmd) if (self.sm is not None and b.cmd is not None) else None
            if b.cmd is not None:
                self._committed_cmds.add(cmd_key(b.cmd))
            _balances = getattr(self.sm, "balances", None)
            event(f"C{self.id}", "DECIDE", f"#{len(self.committed)} node={b.short()} cmd={b.cmd} -> {result}",
                  view=b.view_number, phase="decide", node=b.short(),
                  parent=(b.parent_hash[:6] if b.parent_hash else None),
                  cmd=str(b.cmd), outcome=(str(result) if result is not None else None),
                  balances=(dict(_balances) if _balances is not None else None))
            if self._respond is not None and b.cmd is not None:
                self._respond(self.id, b.cmd, result)

    # ================================================================ view change / timeouts
    def _enter(self, view: int) -> None:
        self.view = view
        self._arm_timer()

    def _advance_from(self, prev: int) -> None:
        if prev in self._left:
            return
        self._left.add(prev)
        self.view = prev + 1
        self.net.send(self.pm.leader(self.view),
                      Msg(MsgType.NEW_VIEW, prev, self.id, justify=self.generic_qc))
        self._arm_timer()

    def _arm_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel(); self._timer = None
        if not getattr(self.pm, "timeouts", False):
            return
        delay = self.pm.timeout_for(self.view, self._failures)
        self._timer = asyncio.get_event_loop().call_later(delay, self._on_timeout, self.view)

    def _on_timeout(self, view: int) -> None:
        if not self.running or view != self.view or view in self._left:
            return
        self._failures += 1
        event(f"C{self.id}", "TIMEOUT", f"view={view} -> nextView (leader {self.pm.leader(view + 1)})")
        self._advance_from(view)
