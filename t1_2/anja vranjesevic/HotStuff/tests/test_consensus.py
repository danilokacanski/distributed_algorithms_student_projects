"""M1 — end-to-end: submit N commands, every correct replica ends with the same
executed log (byte-identical node hashes, same order)."""
import asyncio

from hotstuff.log import mute
from demos.harness import Cluster

mute(True)  # keep the test output quiet


def _run(cluster, seconds, cmds):
    async def script(c):
        for cmd in cmds:
            c.client.submit(cmd)
    asyncio.run(cluster.run(seconds, script))


def test_fixed_leader_all_logs_identical():
    cluster = Cluster(fixed_leader=0, delay=0.005, seed=3)
    cmds = [{"op": "noop", "seq": i} for i in range(15)]
    _run(cluster, 4.0, cmds)

    assert cluster.check_convergence()
    logs = [[n.hash for n in r.committed] for r in cluster.replicas]
    assert all(log == logs[0] for log in logs)
    assert len(logs[0]) == len(cmds)
    assert len(cluster.client.accepted) == len(cmds)


def test_rotating_leader_all_logs_identical():
    # leader(view) = view % 4; every view a different replica leads.
    cluster = Cluster(fixed_leader=None, delay=0.005, seed=4)
    cmds = [{"op": "noop", "seq": i} for i in range(12)]
    _run(cluster, 4.0, cmds)

    assert cluster.check_convergence()
    logs = [[n.hash for n in r.committed] for r in cluster.replicas]
    assert all(log == logs[0] for log in logs)
    assert len(logs[0]) == len(cmds)
