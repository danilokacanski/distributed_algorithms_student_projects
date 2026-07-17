"""One-line structured event log. Every phase transition, vote, lock and decide
goes through `event(...)` so a demo run reads like a narrative, e.g.

    [  0.31s]   R2 | COMMIT      | view=7 node=ab12f3 locked_qc.view=6

The same stream is what the safety checker and any future GUI consume.
"""
from __future__ import annotations

import sys
import time

from . import eventlog

# Windows consoles default to cp1252, which chokes on non-ASCII (e.g. arrows) and
# raises mid-log — inside an async task that kills the replica. Force UTF-8 and
# never let an un-encodable byte crash a run.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except (AttributeError, ValueError):
    pass

_t0 = time.time()
_MUTED = False


def mute(on: bool = True) -> None:
    """Silence output (used by tests / invariant sweeps that run many rounds)."""
    global _MUTED
    _MUTED = on


def reset_clock() -> None:
    global _t0
    _t0 = time.time()


def event(who: str, kind: str, detail: str = "", **fields) -> None:
    """Emit one protocol event. `who`/`kind`/`detail` are the console narrative;
    optional structured `**fields` (view, phase, node, cmd, frm, to, qc, outcome,
    balances, ...) are forwarded to the GUI recorder when one is active. Muting
    only silences the console — capture still happens so a recorded run is
    complete even during otherwise-quiet stretches."""
    t = time.time() - _t0
    eventlog.capture(t, who, kind, detail, dict(fields))
    if _MUTED:
        return
    print(f"[{t:7.2f}s] {who:>4} | {kind:<11} | {detail}")
