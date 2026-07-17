"""Two Byzantine leaders, each a small subclass of the honest Replica. This is
the only inheritance in the codebase (per the readability rules): a Byzantine
node differs from an honest one in exactly one overridden method.

Both attacks are defeated by the *unmodified* honest rules — that is the point.
Equivocation runs headlong into Lemma 1 (no two conflicting QCs in a view);
censorship is defeated by leader rotation (§4.4 liveness).
"""
from __future__ import annotations

from typing import Any, Optional

from hotstuff.log import event
from hotstuff.replica import Replica, cmd_key
from hotstuff.tree import Tree
from hotstuff.types import Msg, MsgType, Node


class EquivocatingReplica(Replica):
    """As leader, proposes TWO conflicting blocks to disjoint halves of the
    cluster (X to {0,1}, Y to {2,3}). Neither half can reach quorum (3 of 4), so
    no QC forms, the view times out, and an honest leader takes over. This is the
    living proof of Lemma 1: a Byzantine leader cannot manufacture two conflicting
    QCs in one view — the honest replicas simply never give it a quorum."""

    def _send_proposal(self, view: int, leaf: Node, justify) -> None:
        # `leaf` (block X) is the honest proposal; fabricate a conflicting block Y
        # on the same parent (different cmd → different hash → conflicts with X).
        other = Node(parent_hash=justify.node_hash, cmd={"equivocation": view}, view_number=view)
        self.tree.add(other)
        group_x = [i for i in range(self.n) if i % 2 == 0]   # {0,2} vs {1,3}: 2 and 2
        group_y = [i for i in range(self.n) if i % 2 == 1]
        event(f"R{self.id}", "EQUIVOCATE",
              f"view={view} block X={leaf.short()}->{group_x}  block Y={other.short()}->{group_y}",
              view=view, phase="prepare", node=leaf.short())
        for r in group_x:
            self.net.send(r, Msg(MsgType.PREPARE, view, self.id, node=leaf, justify=justify))
        for r in group_y:
            self.net.send(r, Msg(MsgType.PREPARE, view, self.id, node=other, justify=justify))


class CensoringReplica(Replica):
    """As leader, silently refuses to propose any transfer from a target bank.
    The censored transfers stay buffered on the honest replicas and get proposed
    the moment leadership rotates to an honest node — so they commit within a few
    views. Set `censor_sender` via the constructor kwarg."""

    def __init__(self, *args: Any, censor_sender: str = "C", **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.censor_sender = censor_sender

    def _is_censored(self, cmd: Any) -> bool:
        return getattr(cmd, "sender", None) == self.censor_sender

    def _next_command(self) -> Optional[Any]:
        """Same as the honest picker, but skips (keeps buffered) censored commands."""
        skipped = []
        chosen = None
        while self._pending:
            cmd = self._pending.popleft()
            if cmd_key(cmd) in self._committed_cmds:
                self._pending_keys.discard(cmd_key(cmd))
                continue
            if self._is_censored(cmd):
                skipped.append(cmd)
                continue
            chosen = cmd
            break
        for cmd in reversed(skipped):        # leave censored ones for an honest leader
            self._pending.appendleft(cmd)
        if chosen is not None:
            event(f"R{self.id}", "CENSOR", f"leader dropped nothing but {self.censor_sender}'s transfers this view")
        return chosen
