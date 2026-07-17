"""Message types and simulated signatures.

One `Message` dataclass covers every protocol phase; fields that a phase
does not use stay at their defaults. Signatures are simulated as readable
tokens ("node~digest") - the simulator checks their count and consistency
(2E+1, m-E thresholds) exactly like the paper, without real cryptography.
"""

from __future__ import annotations


import hashlib
from dataclasses import dataclass
from enum import Enum


class MsgType(str, Enum):
    REQUEST = "request"
    PREPREPARE1 = "preprepare1"
    IN_PREPARE1 = "in-prepare1"
    IN_PREPARE2 = "in-prepare2"
    OUT_PREPARE = "out-prepare"
    COMMIT = "commit"
    PREPREPARE2 = "preprepare2"
    REPLY = "reply"
    VIEW_CHANGE = "view-change"


def payload_digest(payload: str) -> str:
    """Digest of a client request (shortened for readable logs)."""
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def sign(node_id: str, digest: str) -> str:
    """Simulated signature of `digest` by `node_id`."""
    return f"{node_id}~{digest}"


def sig_signer(sig: str) -> str:
    return sig.split("~", 1)[0]


def sig_valid(sig: str, digest: str) -> bool:
    parts = sig.split("~", 1)
    return len(parts) == 2 and parts[1] == digest


@dataclass(frozen=True)
class Message:
    type: MsgType
    sender: str
    view: int
    seq: int
    digest: str = ""
    payload: str = ""  # carried by REQUEST and PREPREPARE1
    signatures: tuple[str, ...] = ()  # aggregated signature tokens
    group: int | None = None  # group this message speaks for
    votes: int = 0  # vote total carried by COMMIT
    new_view: int | None = None  # carried by VIEW_CHANGE

    def short(self) -> str:
        extra = ""
        if self.signatures:
            extra += f" sigs={len(self.signatures)}"
        if self.type is MsgType.COMMIT:
            extra += f" votes={self.votes}"
        if self.new_view is not None:
            extra += f" new_view={self.new_view}"
        return f"{self.type.value} v={self.view} seq={self.seq} d={self.digest or '-'}{extra}"
