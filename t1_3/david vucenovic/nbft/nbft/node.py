"""Consensus node: state, phase handlers, defense models, view change.

`HonestNode` follows the NBFT protocol exactly:

    request      client -> primary
    preprepare1  primary broadcasts the request to every node
    in-prepare1  members of each group exchange signed prepares
    in-prepare2  the representative aggregates >= 2E+1 signatures
                 and returns the aggregate to its group
    out-prepare  representatives (or members that blocked them - Model 1)
                 broadcast to the other representatives; votes are counted
                 with the threshold vote-counting model (Model 2)
    commit       representatives that reached the vote threshold forward
                 the aggregated proof to the primary
    preprepare2  the primary broadcasts the proof as the second broadcast
    reply        every node that verified the proof answers the client

Model 1 (node decision broadcast) is implemented in `_model1_block`: a
member blocks its representative and broadcasts on its own when the
representative equivocates, stays silent past the timeout, or aggregates
fewer than 2E+1 signatures.

The view change is a simplified PBFT-style one: when the client is not
served in time it alerts all nodes; a node whose alert watchdog expires
votes view+1, and a quorum of votes moves every node to the new view,
where the hash ring elects a fresh primary, groups and representatives.
"""

from __future__ import annotations


import asyncio
import hashlib
from dataclasses import dataclass, field

from .config import SimulationConfig
from .hashring import GENESIS_HASH, HashRing, Membership
from .messages import Message, MsgType, payload_digest, sig_signer, sig_valid, sign
from .params import ConsensusParams
from .trace import Tracer
from .voting import VoteLedger


@dataclass(frozen=True)
class Block:
    seq: int
    digest: str
    payload: str
    previous_hash: str

    @property
    def block_hash(self) -> str:
        return hashlib.sha256(f"{self.seq}|{self.digest}|{self.previous_hash}".encode()).hexdigest()


@dataclass
class RoundState:
    """Per-(view, seq) volatile consensus state of one node."""

    digest: str | None = None  # digest this node prepared from preprepare1
    payload: str | None = None
    prepare_sigs: dict[str, set[str]] = field(default_factory=dict)  # digest -> sig tokens
    rep_aggregate_sent: bool = False  # representative only
    rep_partial_sent: bool = False  # representative timed out, sent a partial aggregate
    rep_ok: bool = False  # member accepted the representative's aggregate
    rep_blocked: bool = False  # Model 1 fired
    rep_timer: asyncio.Task | None = None
    ledger: VoteLedger | None = None  # representatives only
    commit_sent: bool = False
    commit_proofs: dict[str, tuple[str, ...]] = field(default_factory=dict)  # primary: digest -> sigs
    preprepare2_sent: bool = False
    executed: bool = False


class Node:
    """Common plumbing: inbox loop, timers, membership, the local chain."""

    def __init__(
        self,
        node_id: str,
        params: ConsensusParams,
        cfg: SimulationConfig,
        ring: HashRing,
        network,
        tracer: Tracer,
    ):
        self.id = node_id
        self.params = params
        self.cfg = cfg
        self.ring = ring
        self.net = network
        self.trace = tracer
        self.inbox = network.register(node_id)
        self.running = True

        self.view = 0
        self.chain: list[Block] = []
        self.last_committed_primary: str | None = None
        self.membership: Membership = self._compute_membership(0)

        self.states: dict[tuple[int, int], RoundState] = {}
        self.pending: dict[str, str] = {}  # digest -> payload known from the client
        self.watchdogs: dict[str, asyncio.Task] = {}  # digest -> commit watchdog
        self.viewchange_votes: dict[int, set[str]] = {}
        self.voted_view: int = 0  # highest view this node voted for
        self._timers: set[asyncio.Task] = set()

    # -- helpers -----------------------------------------------------------

    def head_hash(self) -> str:
        return self.chain[-1].block_hash if self.chain else GENESIS_HASH

    def next_seq(self) -> int:
        return len(self.chain) + 1

    def _compute_membership(self, view: int) -> Membership:
        return self.ring.membership(view, self.head_hash(), self.last_committed_primary, self.params.m, self.params.R)

    def _state(self, view: int, seq: int) -> RoundState:
        return self.states.setdefault((view, seq), RoundState())

    def my_group(self) -> int | None:
        return self.membership.group_of(self.id)

    def is_primary(self) -> bool:
        return self.membership.primary == self.id

    def is_representative(self) -> bool:
        g = self.my_group()
        return g is not None and self.membership.representatives[g] == self.id

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.get_running_loop().create_task(coro)
        self._timers.add(task)
        task.add_done_callback(self._timers.discard)
        return task

    def _send(self, msg: Message, recipient: str) -> None:
        self.net.send(msg, recipient)

    def _broadcast(self, msg: Message, recipients) -> None:
        for recipient in recipients:
            if recipient != self.id:
                self._send(msg, recipient)

    def stop(self) -> None:
        self.running = False
        for task in list(self._timers):
            task.cancel()

    def committed(self, digest: str) -> bool:
        return any(block.digest == digest for block in self.chain)

    # -- main loop ---------------------------------------------------------

    async def run(self) -> None:
        while self.running:
            msg = await self.inbox.get()
            if not self.running:
                return
            self.handle(msg)

    def handle(self, msg: Message) -> None:
        handler = {
            MsgType.REQUEST: self.on_request,
            MsgType.PREPREPARE1: self.on_preprepare1,
            MsgType.IN_PREPARE1: self.on_in_prepare1,
            MsgType.IN_PREPARE2: self.on_in_prepare2,
            MsgType.OUT_PREPARE: self.on_out_prepare,
            MsgType.COMMIT: self.on_commit,
            MsgType.PREPREPARE2: self.on_preprepare2,
            MsgType.VIEW_CHANGE: self.on_view_change,
        }.get(msg.type)
        if handler is not None:
            handler(msg)

    # handlers are provided by HonestNode
    def on_request(self, msg: Message) -> None: ...
    def on_preprepare1(self, msg: Message) -> None: ...
    def on_in_prepare1(self, msg: Message) -> None: ...
    def on_in_prepare2(self, msg: Message) -> None: ...
    def on_out_prepare(self, msg: Message) -> None: ...
    def on_commit(self, msg: Message) -> None: ...
    def on_preprepare2(self, msg: Message) -> None: ...
    def on_view_change(self, msg: Message) -> None: ...


class HonestNode(Node):
    # -- request / preprepare1 ----------------------------------------------

    def on_request(self, msg: Message) -> None:
        digest = payload_digest(msg.payload)
        self.pending[digest] = msg.payload
        if self.committed(digest):
            # Late alert for something already decided: just re-reply.
            self._send(
                Message(MsgType.REPLY, self.id, self.view, 0, digest=digest),
                msg.sender,
            )
            return
        if self.is_primary():
            self._propose(digest)
        elif msg.sender == "client":
            # The client is complaining - arm the commit watchdog that can
            # end in a view-change vote.
            self._arm_watchdog(digest)

    def _propose(self, digest: str) -> None:
        seq = self.next_seq()
        st = self._state(self.view, seq)
        if st.preprepare2_sent or st.digest is not None:
            return  # already proposed in this view
        payload = self.pending[digest]
        st.digest, st.payload = digest, payload
        self.trace.event(
            "PHASE", f"preprepare1: primary {self.id} proposes seq={seq} d={digest} (view {self.view})"
        )
        msg = Message(
            MsgType.PREPREPARE1,
            self.id,
            self.view,
            seq,
            digest=digest,
            payload=payload,
            signatures=(sign(self.id, digest),),
        )
        self._broadcast(msg, self.membership.all_nodes())

    def on_preprepare1(self, msg: Message) -> None:
        if msg.sender != self.membership.primary or msg.view != self.view:
            return
        if msg.seq != self.next_seq() or payload_digest(msg.payload) != msg.digest:
            return
        st = self._state(msg.view, msg.seq)
        if st.digest is not None:
            return  # accept only the first preprepare1 (equivocation defense)
        st.digest, st.payload = msg.digest, msg.payload
        self.pending[msg.digest] = msg.payload

        group = self.my_group()
        if group is None:
            # Ungrouped replicas take part in out-prepare directly (1 vote).
            out = Message(
                MsgType.OUT_PREPARE,
                self.id,
                msg.view,
                msg.seq,
                digest=msg.digest,
                signatures=(sign(self.id, msg.digest),),
            )
            self._broadcast(out, self.membership.representatives)
            return

        # in-prepare1: members send their signed prepare to the representative
        # (star topology borrowed from HotStuff - this is what makes the
        # in-group traffic linear, 2(m-1) per group, as in Formula 4).
        st.prepare_sigs.setdefault(msg.digest, set()).add(sign(self.id, msg.digest))
        if self.is_representative():
            self._maybe_aggregate(msg.view, msg.seq, st)
            self._arm_rep_partial_timer(msg.view, msg.seq)
        else:
            prep = Message(
                MsgType.IN_PREPARE1,
                self.id,
                msg.view,
                msg.seq,
                digest=msg.digest,
                signatures=(sign(self.id, msg.digest),),
                group=group,
            )
            self._send(prep, self.membership.representatives[group])
            self._arm_rep_timer(msg.view, msg.seq)

    # -- in-prepare1 / in-prepare2 -------------------------------------------

    def on_in_prepare1(self, msg: Message) -> None:
        group = self.my_group()
        if group is None or msg.view != self.view or msg.group != group:
            return
        if not self.is_representative() or msg.sender not in self.membership.groups[group]:
            return
        st = self._state(msg.view, msg.seq)
        bucket = st.prepare_sigs.setdefault(msg.digest, set())
        bucket.update(sig for sig in msg.signatures if sig_valid(sig, msg.digest))
        self._maybe_aggregate(msg.view, msg.seq, st)

    def _maybe_aggregate(self, view: int, seq: int, st: RoundState) -> None:
        if (
            not st.rep_aggregate_sent
            and st.digest is not None
            and len(st.prepare_sigs.get(st.digest, ())) >= self.params.sig_quorum
        ):
            self._send_aggregate(view, seq, st)

    def _send_aggregate(self, view: int, seq: int, st: RoundState) -> None:
        """Representative: return the aggregate to the group (in-prepare2)
        and carry the group's voice to the other representatives (out-prepare)."""
        st.rep_aggregate_sent = True
        group = self.my_group()
        sigs = tuple(sorted(st.prepare_sigs[st.digest]))
        self.trace.event(
            "PHASE",
            f"in-prepare2: representative {self.id} (group {group}) aggregated "
            f"{len(sigs)} signatures for d={st.digest}",
        )
        agg = Message(
            MsgType.IN_PREPARE2, self.id, view, seq, digest=st.digest, signatures=sigs, group=group
        )
        self._broadcast(agg, self.membership.groups[group])

        out = Message(
            MsgType.OUT_PREPARE, self.id, view, seq, digest=st.digest, signatures=sigs, group=group
        )
        self._broadcast(out, self.membership.representatives)
        # Count our own group's aggregate in our own ledger as well.
        self._ledger(st).add_group_aggregate(group, st.digest, sigs)
        self._check_vote_threshold(view, seq, st)

    def _arm_rep_partial_timer(self, view: int, seq: int) -> None:
        """Algorithm 1, first clause: a representative that cannot gather
        2E+1 prepares before the timeout forwards whatever aggregate it has
        to the other representatives instead of staying silent."""
        st = self._state(view, seq)

        async def timer() -> None:
            await asyncio.sleep(self.cfg.phase_timeout_ms / 1000.0)
            if st.rep_aggregate_sent or st.rep_partial_sent or st.executed or st.digest is None:
                return
            st.rep_partial_sent = True
            group = self.my_group()
            sigs = tuple(sorted(st.prepare_sigs.get(st.digest, ())))
            self.trace.event(
                "MODEL-1",
                f"representative {self.id} (group {group}) timed out with only "
                f"{len(sigs)} prepare(s) < 2E+1 = {self.params.sig_quorum} - "
                f"forwarding the partial aggregate",
            )
            out = Message(
                MsgType.OUT_PREPARE, self.id, view, seq, digest=st.digest, signatures=sigs, group=group
            )
            self._broadcast(out, self.membership.representatives)
            self._ledger(st).add_group_aggregate(group, st.digest, sigs)
            self._check_vote_threshold(view, seq, st)

        self._spawn(timer())

    def _arm_rep_timer(self, view: int, seq: int) -> None:
        """Model 1, condition (2): representative silent past the timeout."""
        st = self._state(view, seq)

        async def timer() -> None:
            await asyncio.sleep(self.cfg.phase_timeout_ms / 1000.0)
            if not st.rep_ok and not st.rep_blocked and not st.executed:
                self._model1_block(view, seq, st, "timeout waiting for the representative's aggregate")

        st.rep_timer = self._spawn(timer())

    def on_in_prepare2(self, msg: Message) -> None:
        group = self.my_group()
        if group is None or msg.view != self.view or msg.group != group:
            return
        if msg.sender != self.membership.representatives[group] or self.is_representative():
            return
        st = self._state(msg.view, msg.seq)
        if st.rep_ok or st.rep_blocked or st.digest is None:
            return

        # Model 1, condition (1): the representative's message differs from ours.
        if msg.digest != st.digest:
            self._model1_block(msg.view, msg.seq, st, f"representative equivocated (d={msg.digest} != ours {st.digest})")
            return
        # Model 1, condition (3): fewer than 2E+1 aggregated signatures.
        members = set(self.membership.groups[group])
        valid = {
            sig for sig in msg.signatures if sig_valid(sig, msg.digest) and sig_signer(sig) in members
        }
        if len(valid) < self.params.sig_quorum:
            self._model1_block(
                msg.view,
                msg.seq,
                st,
                f"aggregate carries {len(valid)} signatures < 2E+1 = {self.params.sig_quorum}",
            )
            return

        st.rep_ok = True
        if st.rep_timer is not None:
            st.rep_timer.cancel()

    def _model1_block(self, view: int, seq: int, st: RoundState, reason: str) -> None:
        """Node decision broadcast model: block the representative and speak
        for ourselves to the representatives of the other groups."""
        group = self.my_group()
        st.rep_blocked = True
        if st.rep_timer is not None:
            st.rep_timer.cancel()
        rep = self.membership.representatives[group]
        self.trace.event("MODEL-1", f"{self.id} blocked representative {rep} (group {group}): {reason}")
        others = tuple(
            r for g, r in enumerate(self.membership.representatives) if g != group
        )
        out = Message(
            MsgType.OUT_PREPARE,
            self.id,
            view,
            seq,
            digest=st.digest,
            signatures=(sign(self.id, st.digest),),
            group=group,
        )
        self._broadcast(out, others)

    # -- out-prepare / commit --------------------------------------------------

    def _ledger(self, st: RoundState) -> VoteLedger:
        if st.ledger is None:
            st.ledger = VoteLedger(self.params, self.membership)
        return st.ledger

    def on_out_prepare(self, msg: Message) -> None:
        if not self.is_representative() or msg.view != self.view:
            return
        st = self._state(msg.view, msg.seq)
        ledger = self._ledger(st)

        sender_group = self.membership.group_of(msg.sender)
        if sender_group is None:
            if msg.sender in self.membership.ungrouped:
                ledger.add_ungrouped(msg.sender, msg.digest, msg.signatures)
            else:
                return
        elif msg.sender == self.membership.representatives[sender_group]:
            f = ledger.add_group_aggregate(sender_group, msg.digest, msg.signatures)
            if f >= self.params.full_vote_quorum:
                self.trace.event(
                    "MODEL-2",
                    f"group {sender_group} aggregate has F={f} >= m-E="
                    f"{self.params.full_vote_quorum} valid signatures -> counts as m={self.params.m} votes",
                    key=f"m2-full-{sender_group}-{msg.digest}",
                )
            else:
                self.trace.event(
                    "MODEL-2",
                    f"group {sender_group} aggregate has only F={f} < m-E="
                    f"{self.params.full_vote_quorum} signatures -> counts as {ledger.group_votes(sender_group, msg.digest)} votes",
                    key=f"m2-weak-{sender_group}-{msg.digest}",
                )
        else:
            ledger.add_group_individual(sender_group, msg.digest, msg.signatures)

        self._check_vote_threshold(msg.view, msg.seq, st)

    def _check_vote_threshold(self, view: int, seq: int, st: RoundState) -> None:
        if st.commit_sent or st.digest is None:
            return
        ledger = self._ledger(st)
        votes = ledger.votes_for(st.digest)
        if votes >= self.params.vote_threshold:
            st.commit_sent = True
            proof = ledger.proof_signatures(st.digest)
            self.trace.event(
                "MODEL-2",
                f"commit: representative {self.id} reached the vote threshold "
                f"({votes} >= {self.params.vote_threshold}), forwarding proof to the primary "
                f"(other representatives do the same)",
                key=f"m2-commit-{st.digest}",
            )
            commit = Message(
                MsgType.COMMIT,
                self.id,
                view,
                seq,
                digest=st.digest,
                signatures=proof,
                votes=votes,
            )
            self._send(commit, self.membership.primary)

    # -- preprepare2 / reply ----------------------------------------------------

    def on_commit(self, msg: Message) -> None:
        if not self.is_primary() or msg.view != self.view:
            return
        st = self._state(msg.view, msg.seq)
        if st.preprepare2_sent:
            return
        if msg.votes < self.params.vote_threshold:
            return
        valid = tuple(sorted({sig for sig in msg.signatures if sig_valid(sig, msg.digest)}))
        if len(valid) < self.proof_quorum():
            return
        st.commit_proofs[msg.digest] = valid
        st.preprepare2_sent = True
        self.trace.event(
            "PHASE",
            f"preprepare2: primary {self.id} broadcasts the consensus proof "
            f"({msg.votes} votes, {len(valid)} signatures) - second broadcast",
        )
        pp2 = Message(
            MsgType.PREPREPARE2,
            self.id,
            msg.view,
            msg.seq,
            digest=msg.digest,
            signatures=valid,
            votes=msg.votes,
        )
        self._broadcast(pp2, self.membership.all_nodes())
        self._execute(msg.view, msg.seq, msg.digest)

    def proof_quorum(self) -> int:
        """Minimum distinct signatures a network-wide proof must carry:
        (R - w) correct groups each contributing at least m - E signers."""
        return (self.params.R - self.params.w) * self.params.full_vote_quorum

    def on_preprepare2(self, msg: Message) -> None:
        if msg.sender != self.membership.primary or msg.view != self.view:
            return
        if msg.votes < self.params.vote_threshold:
            return
        valid = {sig for sig in msg.signatures if sig_valid(sig, msg.digest)}
        if len({sig_signer(sig) for sig in valid}) < self.proof_quorum():
            return
        self._execute(msg.view, msg.seq, msg.digest)

    def _execute(self, view: int, seq: int, digest: str) -> None:
        st = self._state(view, seq)
        if st.executed or self.committed(digest):
            return
        st.executed = True
        payload = self.pending.get(digest, st.payload or "")
        block = Block(seq=seq, digest=digest, payload=payload, previous_hash=self.head_hash())
        self.chain.append(block)
        self.last_committed_primary = self.membership.primary
        if st.rep_timer is not None:
            st.rep_timer.cancel()
        watchdog = self.watchdogs.pop(digest, None)
        if watchdog is not None:
            watchdog.cancel()
        if self.id == self.membership.primary:
            self.trace.event("BLOCK", f"block #{seq} committed, d={digest}, chain head {block.block_hash[:12]}")
        reply = Message(MsgType.REPLY, self.id, view, seq, digest=digest)
        self._send(reply, "client")
        # Next round: roles are re-drawn because the chain head changed.
        self.membership = self._compute_membership(self.view)

    # -- view change --------------------------------------------------------------

    def _arm_watchdog(self, digest: str) -> None:
        if digest in self.watchdogs:
            return

        async def watchdog() -> None:
            # Keep voting for the next view while the request stays undecided:
            # the hash may well re-elect a faulty primary, so a single view
            # change is not always enough.
            while self.running and not self.committed(digest):
                await asyncio.sleep(2 * self.cfg.phase_timeout_ms / 1000.0)
                if self.committed(digest) or not self.running:
                    return
                self._vote_view_change(max(self.view, self.voted_view) + 1)

        self.watchdogs[digest] = self._spawn(watchdog())

    def _vote_view_change(self, new_view: int) -> None:
        if new_view <= self.voted_view or new_view <= self.view:
            return
        self.voted_view = new_view
        self.viewchange_votes.setdefault(new_view, set()).add(self.id)
        self.trace.event(
            "TIMEOUT",
            f"{self.id} votes for view change -> view {new_view} (peers follow)",
            key=f"vc-vote-{new_view}",
        )
        msg = Message(MsgType.VIEW_CHANGE, self.id, self.view, 0, new_view=new_view)
        self._broadcast(msg, self.membership.all_nodes())
        self._maybe_adopt_view(new_view)

    def on_view_change(self, msg: Message) -> None:
        if msg.new_view is None or msg.new_view <= self.view:
            return
        votes = self.viewchange_votes.setdefault(msg.new_view, set())
        votes.add(msg.sender)
        # Echo rule: join the view change once f+1 nodes are asking for it.
        echo_quorum = (self.params.n - 1) // 3 + 1
        if len(votes) >= echo_quorum and self.voted_view < msg.new_view:
            self._vote_view_change(msg.new_view)
        self._maybe_adopt_view(msg.new_view)

    def _maybe_adopt_view(self, new_view: int) -> None:
        quorum = 2 * ((self.params.n - 1) // 3) + 1
        if new_view <= self.view or len(self.viewchange_votes.get(new_view, ())) < quorum:
            return
        self.view = new_view
        self.membership = self._compute_membership(new_view)
        self.trace.event(
            "VIEW-CHANGE",
            f"network moves to view {new_view}; new primary {self.membership.primary}",
            key=f"vc-adopt-{new_view}",
        )
        if self.is_primary():
            for digest in self.pending:
                if not self.committed(digest):
                    self._propose(digest)
                    break
