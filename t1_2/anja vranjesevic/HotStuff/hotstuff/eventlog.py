"""Structured event recorder — the bridge from the running protocol to the GUI.

The console `event()` in `log.py` stays the human-readable narrative. When a
`Recorder` is active, every `event(...)` call is ALSO captured as a structured
dict matching the schema the web GUI (`gui/index.html`) consumes. That keeps a
single instrumentation path: protocol code calls `event(...)` exactly once and
both the console log and the GUI feed stay in sync.

Schema of one captured event (fields absent when not applicable):
    t              float   seconds since the log clock was reset
    replica        int?    node index (0..3), or None for bus/checker events
    kind           str     normalized: start|NEW-VIEW|PROPOSE|vote|QC|LOCK|
                           DECIDE|TIMEOUT|reject|DROP|CRASH|RESTART|EQUIVOCATE|
                           CENSOR|converge|SAFE|...
    view           int?    current view number
    phase          str?    new-view|prepare|pre-commit|commit|decide|generic
    node,parent    str?    6-hex short block hashes (tree edges)
    cmd            str?    human command string
    from,to        int?    message routing (bus animation)
    qc             dict?   {type, view, node, signers[]}
    locked_qc_view int?
    prepare_qc_view int?
    outcome        str?    ledger result for a DECIDE
    balances       dict?   ledger snapshot after a DECIDE / at start
    detail         str     the raw console detail string (log fallback)
"""
from __future__ import annotations

import re
from typing import Any, Optional

# "R2" (basic) or "C2" (chained) -> replica index 2; "net"/"check"/"demo" -> None
_RID = re.compile(r"[RC](\d+)$")


def _norm_kind(kind: str) -> str:
    """Collapse the phase-tagged console kinds ("vote PREPARE", "QC COMMIT") to
    the bare kind the GUI switches on. The phase lives in the `phase` field."""
    if kind.startswith("vote"):
        return "vote"
    if kind.startswith("QC"):
        return "QC"
    return kind


class Recorder:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def capture(self, t: float, who: str, kind: str, detail: str, fields: dict) -> None:
        # explicit replica= wins (e.g. a "demo"-authored CRASH names the victim),
        # otherwise derive it from the R#/C# author tag.
        rid = fields.pop("replica", None)
        if rid is None:
            m = _RID.match(who or "")
            rid = int(m.group(1)) if m else None

        ev: dict[str, Any] = {"t": round(t, 3), "replica": rid,
                              "kind": _norm_kind(kind), "detail": detail}
        for k, v in fields.items():
            if v is None:
                continue
            ev["from" if k == "frm" else k] = v
        self.events.append(ev)


_active: Optional[Recorder] = None


def start() -> Recorder:
    """Begin capturing. Returns the fresh recorder (also stored as the active one)."""
    global _active
    _active = Recorder()
    return _active


def stop() -> Optional[Recorder]:
    """Stop capturing and return the recorder that was active (or None)."""
    global _active
    r, _active = _active, None
    return r


def capture(t: float, who: str, kind: str, detail: str, fields: dict) -> None:
    """Called by `log.event`. No-op unless a recorder is active."""
    if _active is not None:
        _active.capture(t, who, kind, detail, fields)


def qc_dict(qc: Any) -> Optional[dict]:
    """Render a QC as the GUI's compact {type, view, node, signers} shape."""
    if qc is None:
        return None
    return {
        "type": qc.type.value,
        "view": qc.view_number,
        "node": qc.node_hash[:6] if qc.node_hash else None,
        "signers": sorted(qc.signers()),
    }
