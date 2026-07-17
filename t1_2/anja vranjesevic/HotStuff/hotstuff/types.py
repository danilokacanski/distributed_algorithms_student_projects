"""Core data structures — HotStuff paper §4.2 (Node, QC, Msg).

Kept deliberately tiny and immutable. A `Node` is a block in the tree; a `QC`
(quorum certificate) is a proof that a quorum voted for the same ⟨type, view,
node⟩ triple; a `Msg` is what replicas exchange (Algorithm 1, `msg`/`voteMsg`).
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class MsgType(str, Enum):
    """Message / QC phase tag (Algorithm 2). Order = protocol pipeline order.

    SYNC_REQ / SYNC_RESP are outside the paper's core — a tiny catch-up channel
    (§4.2 notes a replica may fetch missing ancestors from peers)."""
    NEW_VIEW = "new-view"
    PREPARE = "prepare"
    PRE_COMMIT = "pre-commit"
    COMMIT = "commit"
    DECIDE = "decide"
    GENERIC = "generic"        # Chained HotStuff (Algorithm 3): the single phase per view
    SYNC_REQ = "sync-req"
    SYNC_RESP = "sync-resp"


def canonical_bytes(obj: Any) -> bytes:
    """Deterministic byte encoding used for hashing and signing.

    Dataclasses → sorted dict; everything else via JSON with sorted keys. This
    guarantees every replica hashes/signs byte-identical content.
    """
    def default(o: Any) -> Any:
        if dataclasses.is_dataclass(o) and not isinstance(o, type):
            return dataclasses.asdict(o)
        raise TypeError(f"not canonically serializable: {type(o)!r}")

    return json.dumps(obj, default=default, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True)
class Node:
    """A block in the tree — paper §4.2. `cmd` is an opaque application command
    (or None for the genesis / a dummy filler node).

    The hash is SHA-256 over the canonical JSON of (parent_hash, cmd, view).
    Including the view means the same command proposed in two views yields two
    distinct nodes, which keeps the tree unambiguous.

    `justify` (the QC this node carries) is only used by Chained HotStuff, where
    the chain is defined by node→justify links. It is deliberately EXCLUDED from
    the hash and from equality — a node's identity is its content, not the QC that
    happened to certify its parent. `compare=False` keeps the QC's raw signature
    bytes out of dataclass eq/hash too.
    """
    parent_hash: Optional[str]
    cmd: Any
    view_number: int
    justify: Any = field(default=None, compare=False)

    @property
    def hash(self) -> str:
        return hashlib.sha256(canonical_bytes(
            {"parent": self.parent_hash, "cmd": self.cmd, "view": self.view_number}
        )).hexdigest()

    def short(self) -> str:
        """First 6 hex chars — for readable, story-like logs."""
        return self.hash[:6]


@dataclass(frozen=True)
class QC:
    """Quorum certificate over ⟨type, view_number, node_hash⟩ — paper §4.2.

    Paper uses one threshold signature; we keep an explicit tuple of exactly
    `quorum` distinct (replica_id, ed25519_sig) pairs. Safety is identical; only
    the authenticator size differs (documented deviation — see README).
    """
    type: MsgType
    view_number: int
    node_hash: Optional[str]
    sigs: tuple = ()  # tuple of (replica_id: int, signature: bytes)

    def signers(self) -> frozenset:
        return frozenset(rid for rid, _ in self.sigs)


@dataclass
class Msg:
    """A protocol message — Algorithm 1 `msg()` / `voteMsg()`.

    Leader proposals carry `node` + `justify`; replica votes carry a
    `partial_sig` and (per the paper) leave `node`/`justify` implicit via the
    justify QC. `sender` is our transport addition so the bus can route/route-log.
    """
    type: MsgType
    view_number: int
    sender: int
    node: Optional[Node] = None
    justify: Optional[QC] = None
    partial_sig: Optional[tuple] = None  # (replica_id, signature: bytes)
    payload: Any = None                  # catch-up only: requested hash / list[Node]


def matching_msg(m: Msg, t: MsgType, v: int) -> bool:
    """Algorithm 1, line 21."""
    return m.type == t and m.view_number == v


def matching_qc(qc: Optional[QC], t: MsgType, v: int) -> bool:
    """Algorithm 1, line 23."""
    return qc is not None and qc.type == t and qc.view_number == v
