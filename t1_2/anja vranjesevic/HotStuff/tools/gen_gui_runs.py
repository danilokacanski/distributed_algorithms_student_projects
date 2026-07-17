"""Record real protocol runs and emit gui/runs.js for the web GUI.

Each of the five scenarios is run with DELIBERATELY SLOWED timing (larger network
delay, generous gaps between commands) so the resulting event stream is easy to
watch step-by-step in the GUI. The captured events are lightly post-processed
(narration stripped, per-replica DECIDEs collapsed to one per block, timeline
stretched) and written as:

    window.HOTSTUFF_RUNS = { "double-spend": [...], "happy": [...], ... }

The GUI (gui/index.html) loads that file if present and shows genuine data;
without it, the GUI falls back to its own baked-in sample arrays.

    python -m tools.gen_gui_runs
"""
from __future__ import annotations

import asyncio
import json
import os

from bank.ledger import Ledger, Transfer
from byzantine import EquivocatingReplica
from hotstuff import eventlog, log
from hotstuff.chained_replica import ChainedReplica
from demos.harness import Cluster

# Initial balances — chosen to match the GUI's default so the ledger panel reads
# correctly even before the first DECIDE arrives.
BAL0 = {"A": 100, "B": 100, "C": 0, "D": 100}

# Kinds the GUI understands; everything else (demo narration) is dropped so the
# recorded stream is pure protocol.
KEEP = {"NEW-VIEW", "PROPOSE", "vote", "QC", "LOCK", "DECIDE", "TIMEOUT", "reject",
        "DROP", "CRASH", "RESTART", "EQUIVOCATE", "CENSOR", "converge", "DIVERGE",
        "SAFE", "SAFETY-VIOLATION"}


# --------------------------------------------------------------------------- runner
def record(build, script, seconds, excluded=frozenset()):
    """Run one scenario under a recorder and return its cleaned event list."""
    log.mute(True)                 # silence the console; capture still happens
    log.reset_clock()
    rec = eventlog.start()
    cluster = build()

    async def driver(c):
        await script(c)

    try:
        asyncio.run(cluster.run(seconds, driver, stop_when_done=True))
        cluster.check_convergence(excluded=excluded)       # -> "converge" event
        cluster.check_safety_invariants(excluded=excluded)  # -> "SAFE" event
    finally:
        eventlog.stop()
        log.mute(False)
    return postprocess(rec.events)


def postprocess(events):
    """Strip narration, collapse duplicate DECIDEs, prepend a start frame, and
    stretch the timeline so playback is comfortably watchable."""
    kept, seen_decide = [], set()
    for e in events:
        if e["kind"] not in KEEP:
            continue
        if e["kind"] == "DECIDE":
            if e.get("node") in seen_decide:      # every replica logs the same
                continue                          # commit — keep just the first
            seen_decide.add(e.get("node"))
        kept.append(e)
    kept.sort(key=lambda e: e["t"])

    # stretch to a watchable total (≈0.4s/event, clamped) while preserving the
    # relative timing (so concurrent votes still look concurrent).
    span = (kept[-1]["t"] - kept[0]["t"]) if kept else 0
    target = min(20.0, max(9.0, len(kept) * 0.4))
    scale = (target / span) if span > 1e-6 else 1.0
    t0 = kept[0]["t"] if kept else 0.0
    for e in kept:
        e["t"] = round((e["t"] - t0) * scale, 3)

    start = {"t": 0.0, "replica": None, "kind": "start",
             "balances": dict(BAL0), "detail": f"initial balances {BAL0}"}
    return [start] + kept


# --------------------------------------------------------------------------- scenarios
def sc_happy():
    txs = [Transfer("A", "B", 30, 1), Transfer("D", "C", 40, 2),
           Transfer("B", "A", 20, 3), Transfer("C", "D", 15, 4)]

    def build():
        return Cluster(fixed_leader=0, delay=0.06, seed=1, make_state=lambda: Ledger(BAL0))

    async def script(c):
        for t in txs:
            c.client.submit(t)
        while len(c.states[0].history) < len(txs):
            await asyncio.sleep(0.05)

    return record(build, script, seconds=8.0)


def sc_double_spend():
    T1, T2 = Transfer("A", "B", 80, 1), Transfer("A", "C", 80, 2)

    def build():
        return Cluster(fixed_leader=0, delay=0.06, seed=3, make_state=lambda: Ledger(BAL0))

    async def script(c):
        c.replicas[0].submit(T1)
        c.replicas[0].submit(T2)
        while len(c.states[0].history) < 2:
            await asyncio.sleep(0.05)

    return record(build, script, seconds=8.0)


def sc_crash():
    def build():
        return Cluster(fixed_leader=None, timeouts=True, base_timeout=0.45,
                       delay=0.05, seed=7, make_state=lambda: Ledger(BAL0))

    async def script(c):
        n = 0

        async def feed(k, gap):
            nonlocal n
            for _ in range(k):
                c.client.submit(Transfer("A", "B", 5, n)); n += 1
                await asyncio.sleep(gap)

        await feed(3, 0.35)
        c.crash(2)                       # R2 (also a leader every 4th view) dies
        await feed(5, 0.35)              # rotation keeps the other 3 committing

    return record(build, script, seconds=12.0, excluded={2})


def sc_byzantine():
    def build():
        return Cluster(fixed_leader=None, timeouts=True, base_timeout=0.3, delay=0.05,
                       seed=5, make_state=lambda: Ledger(BAL0),
                       byzantine={1: (EquivocatingReplica, {})})

    async def script(c):
        for i in range(6):
            c.client.submit(Transfer("A", "B", 3, i))
            await asyncio.sleep(0.3)
        await asyncio.sleep(1.5)         # let honest leaders finish committing

    return record(build, script, seconds=12.0, excluded={1})


def sc_chained():
    T1, T2 = Transfer("A", "B", 80, 1), Transfer("A", "C", 80, 2)

    def build():
        return Cluster(fixed_leader=0, delay=0.05, seed=1, replica_cls=ChainedReplica,
                       make_state=lambda: Ledger(BAL0))

    def both_committed(c):
        cmds = {node.cmd for node in c.replicas[0].committed}
        return c.all_converged() and T1 in cmds and T2 in cmds

    async def script(c):
        c.replicas[0].submit(T1)
        c.replicas[0].submit(T2)
        for i in range(6):               # fillers give the pipeline its 3-chain
            c.replicas[0].submit(Transfer("D", "A", 1, 1000 + i))
            await asyncio.sleep(0.12)
        for _ in range(120):
            if both_committed(c):
                break
            await asyncio.sleep(0.04)

    return record(build, script, seconds=10.0)


SCENARIOS = {
    "double-spend": sc_double_spend,
    "happy": sc_happy,
    "crash": sc_crash,
    "byzantine": sc_byzantine,
    "chained": sc_chained,
}


def main():
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    out_path = os.path.join(here, "gui", "runs.js")
    runs = {}
    for name, fn in SCENARIOS.items():
        events = fn()
        runs[name] = events
        print(f"  {name:14s} {len(events):3d} events  ({events[-1]['t']:.1f}s)")

    body = ",\n".join(f'  {json.dumps(k)}: {json.dumps(v)}' for k, v in runs.items())
    js = ("// Generated by `python -m tools.gen_gui_runs` — real recorded protocol\n"
          "// runs, one per scenario, in the GUI event schema. Regenerate anytime.\n"
          "window.HOTSTUFF_RUNS = {\n" + body + "\n};\n")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(js)
    print(f"wrote {out_path}  ({len(js)} bytes)")


if __name__ == "__main__":
    main()
