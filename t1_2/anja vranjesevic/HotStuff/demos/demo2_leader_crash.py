"""Demo 2 — Pacemaker: leader crash, rotation, view change, catch-up.

Rotating leaders (leader(view) = view % 4) with per-view timeouts. Midway we
crash replica R2 (which also takes its turn as leader); the remaining 3 keep
committing — each time it is R2's turn the view simply times out and rotates to
the next leader. Later R2 restarts as a fresh process and catches up via the
sync channel. Throughout, all correct replicas keep identical logs.

    python -m demos.demo2_leader_crash
"""
import asyncio

from hotstuff.log import event, reset_clock
from demos.harness import Cluster

CRASH = 2


def main() -> None:
    reset_clock()
    cluster = Cluster(fixed_leader=None, timeouts=True, base_timeout=0.3,
                      delay=0.01, seed=7)

    async def script(c: Cluster) -> None:
        seq = 0

        async def feed(count: int, gap: float) -> None:
            nonlocal seq
            for _ in range(count):
                c.client.submit({"op": "pay", "seq": seq})
                seq += 1
                await asyncio.sleep(gap)

        await feed(10, 0.08)                       # ~0.8s healthy
        c.crash(CRASH)
        await feed(16, 0.08)                       # ~1.3s with R2 down (rotation covers it)
        surviving = c.check_convergence(excluded={CRASH})
        event("demo", "check", f"3 survivors agree while R2 down: {surviving}")
        c.restart(CRASH)
        await feed(12, 0.08)                       # R2 rejoins and rides the live stream
        caught_up = await c.drive_until_converged()  # nudge until R2 reaches the head
        event("demo", "check", f"R2 fully caught up: {caught_up}")

    asyncio.run(cluster.run(seconds=8.0, script=script, stop_when_done=True))

    counts = {r.id: len(r.committed) for r in cluster.replicas}
    for rid, c in counts.items():
        event("demo", "log", f"R{rid} committed {c}")

    assert cluster.check_convergence(), "correct replicas diverged"
    assert cluster.all_converged(), "logs not byte-identical after catch-up"
    assert counts[CRASH] >= 8, "restarted replica did not catch up"
    event("demo", "PASS", f"rotation survived crash; all 4 replicas identical at {counts[CRASH]} committed OK")


if __name__ == "__main__":
    main()
