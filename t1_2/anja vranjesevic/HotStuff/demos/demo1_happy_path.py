"""Demo 1 — Basic HotStuff happy path, fixed leader, perfect network.

20 commands are submitted; leader R0 drives each through PREPARE → PRE-COMMIT →
COMMIT → DECIDE. Expected: all 20 commit in order and every replica's executed
log is byte-identical.

    python -m demos.demo1_happy_path
"""
import asyncio

from hotstuff.log import event, reset_clock
from demos.harness import Cluster

NUM_CMDS = 20


def main() -> None:
    reset_clock()
    cluster = Cluster(fixed_leader=0, delay=0.008, seed=1)

    async def script(c: Cluster) -> None:
        for i in range(NUM_CMDS):
            c.client.submit({"op": "noop", "seq": i})

    asyncio.run(cluster.run(seconds=4.0, script=script))

    event("demo", "result", f"client accepted {len(cluster.client.accepted)}/{NUM_CMDS} commands")
    for r in cluster.replicas:
        event("demo", "log", f"R{r.id} committed {len(r.committed)} nodes")

    assert cluster.check_convergence(), "replicas diverged"
    assert len(cluster.client.accepted) == NUM_CMDS, "not all commands committed"
    assert all(len(r.committed) == NUM_CMDS for r in cluster.replicas), "wrong commit count"
    event("demo", "PASS", "all 4 replicas executed identical 20-command logs OK")


if __name__ == "__main__":
    main()
