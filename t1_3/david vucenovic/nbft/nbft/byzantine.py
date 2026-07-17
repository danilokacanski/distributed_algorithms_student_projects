"""Byzantine behaviors used by the fault scenarios.

Every behavior corrupts the node at the send/receive boundary, so the
honest protocol logic in `HonestNode` stays untouched:

    crash          the node stops processing anything (fail-stop)
    silent_leader  behaves honestly except when elected primary - then it
                   never proposes, forcing the timeout + view change path
    equivocate     sends conflicting digests to different peers
    low_sig        as a representative, forwards an aggregate with only E
                   signatures (< 2E+1), triggering Model 1 condition (3)
    delay          adds a delay larger than the phase timeout to every
                   outgoing message

Byzantine nodes never forge other nodes' signatures: unforgeability is an
assumption of the fault model (real deployments use real cryptography).
"""

from __future__ import annotations


from .messages import Message, MsgType, payload_digest, sign
from .node import HonestNode

BEHAVIORS = ("crash", "silent_leader", "equivocate", "low_sig", "delay")

_DIGEST_PHASES = (
    MsgType.PREPREPARE1,
    MsgType.IN_PREPARE1,
    MsgType.IN_PREPARE2,
    MsgType.OUT_PREPARE,
    MsgType.PREPREPARE2,
)


class ByzantineNode(HonestNode):
    def __init__(self, behavior: str, *args, **kwargs):
        if behavior not in BEHAVIORS:
            raise ValueError(f"unknown byzantine behavior: {behavior}")
        super().__init__(*args, **kwargs)
        self.behavior = behavior

    # -- receive side ---------------------------------------------------------

    def handle(self, msg: Message) -> None:
        if self.behavior == "crash":
            return  # fail-stop: the node is gone
        super().handle(msg)

    def on_request(self, msg: Message) -> None:
        if self.behavior == "silent_leader" and self.is_primary():
            self.trace.event(
                "BYZANTINE",
                f"primary {self.id} received the request and stays silent",
                key=f"silent-{msg.digest}",
            )
            return
        super().on_request(msg)

    # -- send side --------------------------------------------------------------

    def _send(self, msg: Message, recipient: str) -> None:
        msg = self._corrupt(msg, recipient)
        if msg is None:
            return
        extra = self.cfg.byz_extra_delay_ms if self.behavior == "delay" else 0.0
        self.net.send(msg, recipient, extra_delay_ms=extra)

    def _corrupt(self, msg: Message, recipient: str) -> Message | None:
        if self.behavior == "equivocate" and msg.type in _DIGEST_PHASES and msg.digest:
            # Tell one half of the peers the truth and feed the other half
            # a conflicting value signed only by ourselves.
            if hash(recipient) % 2 == 1:
                evil_payload = (msg.payload or msg.digest) + " [conflicting]"
                evil_digest = payload_digest(evil_payload)
                return Message(
                    msg.type,
                    self.id,
                    msg.view,
                    msg.seq,
                    digest=evil_digest,
                    payload=evil_payload if msg.payload else "",
                    signatures=(sign(self.id, evil_digest),),
                    group=msg.group,
                    votes=msg.votes,
                )
        if self.behavior == "low_sig" and msg.type in (MsgType.IN_PREPARE2, MsgType.OUT_PREPARE):
            if len(msg.signatures) >= self.params.sig_quorum:
                truncated = tuple(sorted(msg.signatures))[: self.params.E]
                self.trace.event(
                    "BYZANTINE",
                    f"representative {self.id} truncates the aggregate to "
                    f"{len(truncated)} signature(s) (< 2E+1 = {self.params.sig_quorum})",
                    key=f"lowsig-{self.id}-{msg.type.value}-{msg.seq}",
                )
                return Message(
                    msg.type,
                    self.id,
                    msg.view,
                    msg.seq,
                    digest=msg.digest,
                    signatures=truncated,
                    group=msg.group,
                )
        return msg
