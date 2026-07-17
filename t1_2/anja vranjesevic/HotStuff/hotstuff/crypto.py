"""Ed25519 signing + quorum-certificate build/verify — paper §3 (crypto model).

Deviation from the paper (stated once, here and in README): the paper aggregates
one *threshold* signature per QC via `tcombine`. We instead keep an explicit
list of `quorum` distinct per-replica Ed25519 signatures. Safety is identical —
a QC still proves a quorum signed the same triple — only the authenticator is
O(n) bytes instead of O(1). Everything else maps to Algorithm 1 lines 15–19.
"""
from __future__ import annotations

from typing import Iterable

from nacl.exceptions import BadSignatureError
from nacl.signing import SigningKey, VerifyKey

from .types import QC, Msg, MsgType, canonical_bytes


def gen_keys(n: int) -> tuple[list[SigningKey], list[VerifyKey]]:
    """Deterministic keygen: replica i's key is seeded from i so a run (and its
    logs) is reproducible. Fine for a simulator; never do this for real keys."""
    signing = [SigningKey(bytes([i]) * 32) for i in range(n)]
    verify = [sk.verify_key for sk in signing]
    return signing, verify


def _vote_digest(type: MsgType, view: int, node_hash) -> bytes:
    """The bytes a replica signs when voting — the ⟨type, view, node⟩ triple."""
    return canonical_bytes([type.value, view, node_hash])


def sign_vote(sk: SigningKey, type: MsgType, view: int, node_hash) -> bytes:
    """`tsign_u(⟨type, view, node⟩)` — Algorithm 1, line 9. Detached signature."""
    return sk.sign(_vote_digest(type, view, node_hash)).signature


def verify_vote(vk: VerifyKey, type: MsgType, view: int, node_hash, sig: bytes) -> bool:
    try:
        vk.verify(_vote_digest(type, view, node_hash), sig)
        return True
    except BadSignatureError:
        return False


def build_qc(votes: Iterable[Msg], quorum: int) -> QC:
    """`QC(V)` — Algorithm 1, lines 15–19.

    `votes` must all match on ⟨type, view, node⟩ (the caller collects them that
    way). We take exactly `quorum` distinct signers, sorted for determinism, so
    every leader builds a byte-identical QC from the same votes.
    """
    votes = list(votes)
    type, view = votes[0].type, votes[0].view_number
    node_hash = votes[0].node.hash if votes[0].node is not None else None

    by_signer: dict[int, bytes] = {}
    for v in votes:
        assert v.type == type and v.view_number == view, "mixed votes in build_qc"
        rid, sig = v.partial_sig
        by_signer.setdefault(rid, sig)

    chosen = sorted(by_signer.items())[:quorum]
    return QC(type=type, view_number=view, node_hash=node_hash, sigs=tuple(chosen))


def verify_qc(qc: QC, verify_keys: list[VerifyKey], quorum: int) -> bool:
    """A QC is valid iff it carries ≥ quorum *distinct* signers whose signatures
    all verify over its own ⟨type, view, node⟩ triple."""
    seen: set[int] = set()
    for rid, sig in qc.sigs:
        if rid in seen or not (0 <= rid < len(verify_keys)):
            return False
        if not verify_vote(verify_keys[rid], qc.type, qc.view_number, qc.node_hash, sig):
            return False
        seen.add(rid)
    return len(seen) >= quorum
