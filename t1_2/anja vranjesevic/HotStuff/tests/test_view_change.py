"""M2 — liveness under a crashed leader + catch-up after restart."""
import asyncio

from hotstuff.log import mute
from demos.harness import Cluster

mute(True)


def test_rotation_survives_crash_and_catches_up():
    cluster = Cluster(fixed_leader=None, timeouts=True, base_timeout=0.15,
                      delay=0.008, seed=11)

    async def script(c):
        seq = 0

        async def feed(count, gap):
            nonlocal seq
            for _ in range(count):
                c.client.submit({"op": "pay", "seq": seq}); seq += 1
                await asyncio.sleep(gap)

        await feed(10, 0.06)
        before = min(len(r.committed) for r in c.replicas if r.id != 2)
        c.crash(2)
        await feed(16, 0.06)
        assert c.check_convergence(excluded={2})       # 3 survivors agree
        survived = min(len(r.committed) for r in c.replicas if r.id != 2)
        assert survived > before, "survivors made no progress while a leader was down"
        c.restart(2)
        await feed(10, 0.06)
        # Keep nudging until R2 has ridden the DECIDE stream back up to the head.
        assert await c.drive_until_converged(), "R2 failed to catch up"

    asyncio.run(cluster.run(6.0, script, stop_when_done=True))

    assert cluster.check_convergence()                 # all four prefix-consistent (safety)
    counts = [len(r.committed) for r in cluster.replicas]
    assert counts[2] >= 8, f"restarted replica did not catch up: {counts}"
    assert min(counts) == max(counts), f"logs differ in length after catch-up: {counts}"
    # every replica's full log is byte-identical, not just a shared prefix
    logs = [[n.hash for n in r.committed] for r in cluster.replicas]
    assert all(log == logs[0] for log in logs)


def test_no_spurious_timeouts_under_continuous_load():
    # A healthy rotating cluster kept continuously busy should never time out:
    # every view decides well within the timeout, so the pacemaker never fires.
    cluster = Cluster(fixed_leader=None, timeouts=True, base_timeout=0.3,
                      delay=0.005, seed=12)
    fired = []
    for r in cluster.replicas:
        orig = r._on_timeout
        r._on_timeout = (lambda v, o=orig, rid=r.id: (fired.append(rid), o(v)))

    async def script(c):
        # Feed for the whole run so the cluster is never idle (idle rotation via
        # timeout is expected and would be a false positive here).
        for i in range(40):
            c.client.submit({"op": "pay", "seq": i}); await asyncio.sleep(0.05)

    asyncio.run(cluster.run(2.0, script))
    assert cluster.check_convergence()
    assert fired == [], f"unexpected timeouts fired under load: {fired}"
