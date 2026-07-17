"""Basic HotStuff — Algorithm 2 (paper §4.1). One replica, event-driven.

Each replica is an independent asyncio task reading its inbox and dispatching on
message type. A single `MsgType` tag is shared between a leader's *proposal*
(broadcast, `partial_sig is None`) and a replica's *vote* (point-to-point to the
leader, `partial_sig` set); the handler branches on that. The four phases

    PREPARE → PRE-COMMIT → COMMIT → DECIDE

run strictly in order within a view (Basic HotStuff is not pipelined — that is
Chained's job, M5). The safety-critical step is the lock in `on_commit`
(Algorithm 2, line 25); it is commented loudly there.
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
from .tree import GENESIS, GENESIS_QC, Tree
from .types import (QC, Msg, MsgType, Node, canonical_bytes, matching_qc)


def _is_genesis_qc(qc: Optional[QC]) -> bool:
    return qc is not None and qc.view_number == 0 and len(qc.sigs) == 0


def cmd_key(cmd: Any) -> str:
    """Stable identity of a command, used to de-duplicate the pending buffer and
    avoid proposing an already-committed command twice."""
    return canonical_bytes(cmd).hex()


class Replica:
    def __init__(self, node_id: int, n: int, f: int, net: Network,
                 signing_key, verify_keys, pacemaker: Pacemaker,
                 state_machine: Any = None,
                 on_execute: Optional[Callable[[int, Any, Any], None]] = None):
        self.id = node_id
        self.n = n
        self.f = f
        self.quorum = n - f          # (n − f) = 2f + 1 votes form a QC
        self.net = net
        self.sk = signing_key
        self.vks = verify_keys
        self.pm = pacemaker
        self.sm = state_machine      # replicated state machine (bank ledger); may be None
        self._respond = on_execute   # reply-to-client callback

        # --- protocol state (paper names) ---
        self.view = 0
        self.tree = Tree()
        self.locked_qc: QC = GENESIS_QC      # lockedQC — safety
        self.prepare_qc: QC = GENESIS_QC     # prepareQC — carried in NEW-VIEW / liveness
        self.committed: list[Node] = []      # executed nodes, in order (the log)
        self._executed: set[str] = set()

        # --- bookkeeping ---
        self._pending: deque = deque()               # client commands awaiting proposal
        self._pending_keys: set[str] = set()
        self._committed_cmds: set[str] = set()
        self._votes: dict[tuple, dict[int, Msg]] = defaultdict(dict)   # QC tallies (leader)
        self._new_view: dict[int, dict[int, QC]] = defaultdict(dict)   # NEW-VIEW tallies
        self._formed: set[tuple] = set()             # QCs already formed (fire-once)
        self._proposed: set[int] = set()             # views we proposed in (leader)
        self._high_qc: dict[int, QC] = {}            # highQC picked per view (leader)
        self._decided: set[int] = set()              # views already decided (advance-once)
        self._left: set[int] = set()                 # views we have moved on from (once)
        self._failures = 0                           # consecutive timed-out views (back-off)
        self._timer: Optional[asyncio.TimerHandle] = None
        self._deferred: list[Msg] = []               # decides awaiting catch-up sync
        self.qcs: list[tuple] = []                    # (type, view, node_hash) of every QC seen — Lemma 1 audit
        self.running = True

    def _record_qc(self, qc: QC) -> None:
        if qc is not None and len(qc.sigs) > 0:       # ignore the sig-less genesis QC
            self.qcs.append((qc.type, qc.view_number, qc.node_hash))

    # ------------------------------------------------------------------ lifecycle
    async def run(self) -> None:
        event(f"R{self.id}", "start", f"leader(1)={self.pm.leader(1)}")
        self._advance_from(0)               # bootstrap: send NEW-VIEW that starts view 1
        while self.running:
            msg = await self.net.recv(self.id)
            self.dispatch(msg)

    def stop(self) -> None:
        self.running = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def submit(self, cmd: Any) -> None:
        """A client hands this replica a command. Only the leader will actually
        propose it; buffering it everywhere means whoever is leader can."""
        k = cmd_key(cmd)
        if k in self._pending_keys or k in self._committed_cmds:
            return
        self._pending.append(cmd)
        self._pending_keys.add(k)
        self._try_propose(self.view)

    # ------------------------------------------------------------------ helpers
    def _sign(self, type: MsgType, view: int, node: Node) -> tuple:
        return (self.id, sign_vote(self.sk, type, view, node.hash))

    def _send_vote(self, type: MsgType, view: int, node: Node) -> None:
        """voteMsg(type, node, ⊥) → leader(view) — Algorithm 1, lines 7–10."""
        self.net.send(self.pm.leader(view),
                      Msg(type, view, self.id, node=node,
                          partial_sig=self._sign(type, view, node)))

    def _verify_justify(self, qc: Optional[QC]) -> bool:
        return _is_genesis_qc(qc) or (qc is not None and verify_qc(qc, self.vks, self.quorum))

    # ------------------------------------------------------------------ dispatch
    def dispatch(self, m: Msg) -> None:
        is_vote = m.partial_sig is not None
        # A correct leader message from a view ahead of us means we fell behind
        # (e.g. just restarted); jump forward so we participate in the live view.
        if m.type in (MsgType.PREPARE, MsgType.PRE_COMMIT, MsgType.COMMIT, MsgType.DECIDE) \
                and not is_vote and m.view_number > self.view:
            self._sync_view(m.view_number)
        if m.type == MsgType.NEW_VIEW:
            self.on_new_view(m)
        elif m.type == MsgType.PREPARE:
            self._on_prepare_vote(m) if is_vote else self.on_prepare(m)
        elif m.type == MsgType.PRE_COMMIT:
            self._on_generic_vote(m) if is_vote else self.on_pre_commit(m)
        elif m.type == MsgType.COMMIT:
            self._on_generic_vote(m) if is_vote else self.on_commit(m)
        elif m.type == MsgType.DECIDE:
            self.on_decide(m)
        elif m.type == MsgType.SYNC_REQ:
            self.on_sync_req(m)
        elif m.type == MsgType.SYNC_RESP:
            self.on_sync_resp(m)

    # ---- vote tally (leader side, shared by all phases) ----
    def _tally(self, m: Msg) -> Optional[list[Msg]]:
        """Record one vote; return the winning vote-set the moment a fresh quorum
        of *valid, distinct* signatures is reached (fires exactly once)."""
        rid, sig = m.partial_sig
        node = m.node
        if node is None or not verify_vote(self.vks[rid], m.type, m.view_number, node.hash, sig):
            return None
        key = (m.type, m.view_number, node.hash)
        if key in self._formed:
            return None
        self._votes[key][rid] = m
        if len(self._votes[key]) >= self.quorum:
            self._formed.add(key)
            return list(self._votes[key].values())
        return None

    # ================================================================ NEW-VIEW
    def on_new_view(self, m: Msg) -> None:
        """Leader for view (m.view+1) collecting NEW-VIEW — Algorithm 2, lines 2–4."""
        start = m.view_number + 1
        if not self.pm.is_leader(start, self.id):
            return
        if not self._verify_justify(m.justify):
            return
        self._new_view[start][m.sender] = m.justify or GENESIS_QC
        if len(self._new_view[start]) >= self.quorum and start not in self._high_qc:
            # highQC ← arg max over the collected justify QCs (line 4)
            high = max(self._new_view[start].values(), key=lambda q: q.view_number)
            self._high_qc[start] = high
            event(f"R{self.id}", "NEW-VIEW", f"view={start} highQC.view={high.view_number} (leader)",
                  view=start, phase="new-view", qc=qc_dict(high))
            self._try_propose(start)

    # ================================================================ PREPARE
    def _try_propose(self, view: int) -> None:
        """Leader: once it holds highQC for the view and a pending command,
        createLeaf and broadcast PREPARE — Algorithm 2, lines 5–6."""
        if view not in self._high_qc or view in self._proposed:
            return
        if not self.pm.is_leader(view, self.id):
            return
        cmd = self._next_command()
        if cmd is None:
            return
        high = self._high_qc[view]
        leaf = self.create_leaf(high.node_hash, cmd, view)   # line 5
        self.tree.add(leaf)
        self._proposed.add(view)
        event(f"R{self.id}", "PROPOSE", f"view={view} node={leaf.short()} cmd={cmd} highQC.view={high.view_number}",
              view=view, phase="prepare", node=leaf.short(),
              parent=(leaf.parent_hash[:6] if leaf.parent_hash else None), cmd=str(cmd))
        self._send_proposal(view, leaf, high)                # line 6

    def _send_proposal(self, view: int, leaf: Node, justify: QC) -> None:
        """Broadcast the PREPARE proposal. Isolated so a Byzantine leader can
        override it to equivocate (send conflicting proposals to disjoint subsets)."""
        self.net.broadcast(Msg(MsgType.PREPARE, view, self.id, node=leaf, justify=justify))

    def _next_command(self) -> Optional[Any]:
        while self._pending:
            cmd = self._pending[0]
            if cmd_key(cmd) in self._committed_cmds:
                self._pending.popleft(); self._pending_keys.discard(cmd_key(cmd))
                continue
            return self._pending.popleft()
        return None

    def create_leaf(self, parent_hash: str, cmd: Any, view: int) -> Node:
        """createLeaf(parent, cmd) — Algorithm 1, lines 11–14 (+ our view tag)."""
        return Node(parent_hash=parent_hash, cmd=cmd, view_number=view)

    def on_prepare(self, m: Msg) -> None:
        """Replica side of PREPARE — Algorithm 2, lines 7–10."""
        if m.sender != self.pm.leader(m.view_number) or not self._verify_justify(m.justify):
            return
        node, qc = m.node, m.justify
        self.tree.add(node)
        self.view = max(self.view, m.view_number)
        parent = self.tree.get(qc.node_hash)
        if parent is None:
            # We are missing the branch this proposal builds on — fetch it from
            # the leader and skip voting this view; we catch up via later decides.
            self._request_sync(qc.node_hash, m.sender)
            return
        # vote iff node extends justify.node  AND  safeNode(node, justify)   (line 9)
        if not self.tree.extends(node, parent):
            event(f"R{self.id}", "reject", f"view={m.view_number} node={node.short()} !extends",
                  view=m.view_number, node=node.short())
            return
        if not self.safe_node(node, qc):
            event(f"R{self.id}", "reject", f"view={m.view_number} node={node.short()} !safeNode",
                  view=m.view_number, node=node.short())
            return
        event(f"R{self.id}", "vote PREPARE", f"view={m.view_number} node={node.short()}",
              view=m.view_number, phase="prepare", node=node.short(),
              frm=self.id, to=self.pm.leader(m.view_number))
        self._send_vote(MsgType.PREPARE, m.view_number, node)   # line 10

    def _on_prepare_vote(self, m: Msg) -> None:
        """Leader collects PREPARE votes → prepareQC → PRE-COMMIT — lines 11–14."""
        votes = self._tally(m)
        if votes is None:
            return
        self.prepare_qc = build_qc(votes, self.quorum)
        self._record_qc(self.prepare_qc)
        event(f"R{self.id}", "QC PREPARE", f"view={m.view_number} node={m.node.short()} -> PRE-COMMIT",
              view=m.view_number, phase="pre-commit", node=m.node.short(), qc=qc_dict(self.prepare_qc))
        self.net.broadcast(Msg(MsgType.PRE_COMMIT, m.view_number, self.id, justify=self.prepare_qc))

    # ================================================================ PRE-COMMIT
    def on_pre_commit(self, m: Msg) -> None:
        """Replica side of PRE-COMMIT — Algorithm 2, lines 15–18."""
        if m.sender != self.pm.leader(m.view_number) or not matching_qc(m.justify, MsgType.PREPARE, m.view_number):
            return
        if not self._verify_justify(m.justify):
            return
        self.prepare_qc = m.justify                    # line 17
        node = self.tree.get(m.justify.node_hash)
        if node is None:
            return
        event(f"R{self.id}", "vote P-COMMIT", f"view={m.view_number} node={node.short()}",
              view=m.view_number, phase="pre-commit", node=node.short(),
              frm=self.id, to=self.pm.leader(m.view_number))
        self._send_vote(MsgType.PRE_COMMIT, m.view_number, node)   # line 18

    def _on_generic_vote(self, m: Msg) -> None:
        """Leader collects PRE-COMMIT / COMMIT votes and drives the next phase
        (lines 19–22 for PRE-COMMIT votes, 27–30 for COMMIT votes)."""
        votes = self._tally(m)
        if votes is None:
            return
        qc = build_qc(votes, self.quorum)
        self._record_qc(qc)
        if m.type == MsgType.PRE_COMMIT:
            event(f"R{self.id}", "QC P-COMMIT", f"view={m.view_number} node={m.node.short()} -> COMMIT",
                  view=m.view_number, phase="commit", node=m.node.short(), qc=qc_dict(qc))
            self.net.broadcast(Msg(MsgType.COMMIT, m.view_number, self.id, justify=qc))
        else:  # COMMIT votes → commitQC → DECIDE
            event(f"R{self.id}", "QC COMMIT", f"view={m.view_number} node={m.node.short()} -> DECIDE",
                  view=m.view_number, phase="decide", node=m.node.short(), qc=qc_dict(qc))
            self.net.broadcast(Msg(MsgType.DECIDE, m.view_number, self.id, justify=qc))

    # ================================================================ COMMIT
    def on_commit(self, m: Msg) -> None:
        """Replica side of COMMIT — Algorithm 2, lines 23–26."""
        if m.sender != self.pm.leader(m.view_number) or not matching_qc(m.justify, MsgType.PRE_COMMIT, m.view_number):
            return
        if not self._verify_justify(m.justify):
            return
        node = self.tree.get(m.justify.node_hash)
        if node is None:
            return
        # ***** THE LOCK — Algorithm 2, line 25. This is the safety-critical step. *****
        # Once locked on this node's QC, we will refuse to vote for any conflicting
        # node in a lower view (see safe_node). This is what makes two conflicting
        # commits impossible (Lemma 1 / Theorem 2).
        self.locked_qc = m.justify
        event(f"R{self.id}", "LOCK", f"view={m.view_number} node={node.short()} locked_qc.view={self.locked_qc.view_number}",
              view=m.view_number, phase="commit", node=node.short(), locked_qc_view=self.locked_qc.view_number)
        self._send_vote(MsgType.COMMIT, m.view_number, node)   # line 26

    # ================================================================ DECIDE
    def on_decide(self, m: Msg) -> None:
        """Replica side of DECIDE — Algorithm 2, lines 31–33, then next view."""
        if m.sender != self.pm.leader(m.view_number) or not matching_qc(m.justify, MsgType.COMMIT, m.view_number):
            return
        if not self._verify_justify(m.justify):
            return
        node = self.tree.get(m.justify.node_hash)
        # Only execute once the full branch back to genesis (or an already
        # executed node) is present — otherwise we'd leave a hole in the log.
        missing = m.justify.node_hash if node is None else self._first_missing_ancestor(node)
        if missing is not None:
            self._request_sync(missing, m.sender)
            self._deferred.append(m)                  # replay once the branch arrives
            return
        self.execute(node)                            # line 33
        if m.view_number not in self._decided:
            self._decided.add(m.view_number)
            self._failures = 0                        # progress → reset back-off
            self._advance_from(m.view_number)         # advance to next view

    def execute(self, node: Node) -> None:
        """Execute node and any un-executed ancestors, oldest first, then reply
        to the client. 'Consensus orders; the state machine judges.'"""
        chain = []
        cur: Optional[Node] = node
        while cur is not None and cur.hash != GENESIS.hash and cur.hash not in self._executed:
            chain.append(cur)
            cur = self.tree.get(cur.parent_hash)
        for b in reversed(chain):
            self._executed.add(b.hash)
            self.committed.append(b)
            result = self.sm.apply(b.cmd) if (self.sm is not None and b.cmd is not None) else None
            k = cmd_key(b.cmd) if b.cmd is not None else None
            if k is not None:
                self._committed_cmds.add(k)
            _balances = getattr(self.sm, "balances", None)
            event(f"R{self.id}", "DECIDE", f"#{len(self.committed)} node={b.short()} cmd={b.cmd} -> {result}",
                  view=b.view_number, phase="decide", node=b.short(),
                  parent=(b.parent_hash[:6] if b.parent_hash else None),
                  cmd=str(b.cmd), outcome=(str(result) if result is not None else None),
                  balances=(dict(_balances) if _balances is not None else None))
            if self._respond is not None and b.cmd is not None:
                self._respond(self.id, b.cmd, result)

    # ================================================================ safety rule
    def safe_node(self, node: Node, qc: QC) -> bool:
        """safeNode(node, qc) — Algorithm 1, lines 25–27.

        Vote is safe if the node extends our locked node (safety rule) OR the
        justifying QC is from a higher view than our lock (liveness rule — lets a
        correct new leader unstick us once it has seen a newer QC).
        """
        locked_node = self.tree.get(self.locked_qc.node_hash)
        safety = locked_node is not None and self.tree.extends(node, locked_node)   # line 26
        liveness = qc.view_number > self.locked_qc.view_number                      # line 27
        return safety or liveness

    # ================================================================ view change
    def _advance_from(self, prev_view: int) -> None:
        """nextView: move to prev_view+1 and send NEW-VIEW⟨⊥, prepareQC⟩ to its
        leader — Algorithm 2, line 35. Called after DECIDE (happy path) and on
        timeout (recovery). There is no separate view-change subprotocol: the new
        leader's normal PREPARE (collect NEW-VIEWs, take highQC) *is* the view
        change (paper §1 — linear view change)."""
        if prev_view in self._left:
            return
        self._left.add(prev_view)
        self.view = prev_view + 1
        self.net.send(self.pm.leader(self.view),
                      Msg(MsgType.NEW_VIEW, prev_view, self.id, justify=self.prepare_qc))
        self._arm_timer()

    def _arm_timer(self) -> None:
        """Start (or restart) the per-view timeout — paper §4.4. Disabled unless
        the pacemaker enables it (M1 runs with a perfect network, no timeouts)."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not getattr(self.pm, "timeouts", False):
            return
        delay = self.pm.timeout_for(self.view, self._failures)
        self._timer = asyncio.get_event_loop().call_later(delay, self._on_timeout, self.view)

    def _on_timeout(self, view: int) -> None:
        """Algorithm 2, lines 34–35: gave up waiting in `view`; rotate to the next
        leader. Safety is untouched — we only ever move forward and re-send our
        prepareQC, never un-lock."""
        if not self.running or view != self.view or view in self._left:
            return
        self._failures += 1
        event(f"R{self.id}", "TIMEOUT", f"view={view} -> nextView (leader {self.pm.leader(view + 1)})",
              view=view)
        self._advance_from(view)

    def _sync_view(self, view: int) -> None:
        """Jump forward to a live view we discovered we were behind on, without
        emitting a NEW-VIEW (we mark the skipped views as already left)."""
        for v in range(self.view, view):
            self._left.add(v)
        self.view = view
        self._arm_timer()

    # ================================================================ catch-up (§4.2)
    def _first_missing_ancestor(self, node: Node) -> Optional[str]:
        """Walk parents from `node`; return the first hash we don't have (so we
        can fetch it), or None if the chain is complete back to genesis/executed."""
        cur: Optional[Node] = node
        while cur is not None and cur.hash != GENESIS.hash and cur.hash not in self._executed:
            if cur.parent_hash and not self.tree.has(cur.parent_hash):
                return cur.parent_hash
            cur = self.tree.get(cur.parent_hash)
        return None

    def _request_sync(self, node_hash: str, peer: int) -> None:
        self.net.send(peer, Msg(MsgType.SYNC_REQ, self.view, self.id, payload=node_hash))

    def on_sync_req(self, m: Msg) -> None:
        """A peer is missing a branch ending at m.payload; reply with the nodes
        from that hash back to genesis that we hold."""
        chain: list[Node] = []
        cur = self.tree.get(m.payload)
        while cur is not None and cur.hash != GENESIS.hash:
            chain.append(cur)
            cur = self.tree.get(cur.parent_hash)
        if chain:
            self.net.send(m.sender, Msg(MsgType.SYNC_RESP, self.view, self.id, payload=chain))

    def on_sync_resp(self, m: Msg) -> None:
        for node in m.payload:
            self.tree.add(node)
        # Retry decides that were blocked waiting for these ancestors.
        deferred, self._deferred = self._deferred, []
        for msg in deferred:
            self.on_decide(msg)
