"""Simulated network — one `asyncio.Queue` inbox per replica.

No sockets, no serialization: replicas are asyncio tasks in one process and we
pass `Msg` objects by reference. The bus adds configurable delay/jitter and can
drop messages, which is exactly the knob the fault demos need (M2/M4). A
`crash`ed replica's inbox is simply never read again by its (stopped) task; the
bus can also globally drop everything to/from a partitioned node.
"""
from __future__ import annotations

import asyncio
import random
from typing import Optional

from .log import event
from .types import Msg


class Network:
    def __init__(self, n: int, delay: float = 0.01, jitter: float = 0.0,
                 drop_prob: float = 0.0, rng: Optional[random.Random] = None):
        self.n = n
        self.inboxes: dict[int, asyncio.Queue] = {i: asyncio.Queue() for i in range(n)}
        self.delay = delay
        self.jitter = jitter
        self.drop_prob = drop_prob
        self.rng = rng or random.Random()
        self.partitioned: set[int] = set()  # nodes cut off from the network
        self._deliveries: list[asyncio.Task] = []

    def partition(self, node_id: int) -> None:
        """Cut a node off entirely (models a crash / total network partition)."""
        self.partitioned.add(node_id)

    def heal(self, node_id: int) -> None:
        self.partitioned.discard(node_id)

    async def _deliver(self, dst: int, msg: Msg) -> None:
        if dst in self.partitioned or msg.sender in self.partitioned:
            return
        if self.rng.random() < self.drop_prob:
            event("net", "DROP", f"n{msg.sender}->n{dst} {msg.type.value}")
            return
        await asyncio.sleep(self.delay + self.rng.uniform(0, self.jitter))
        await self.inboxes[dst].put(msg)

    def send(self, dst: int, msg: Msg) -> None:
        """Point-to-point. Returns immediately; delivery runs as its own task."""
        self._deliveries.append(asyncio.ensure_future(self._deliver(dst, msg)))

    def broadcast(self, msg: Msg) -> None:
        """Deliver to every replica *including the sender* — so a leader plays
        the replica role uniformly (it receives and votes on its own proposal)."""
        for i in range(self.n):
            self.send(i, msg)

    async def recv(self, node_id: int) -> Msg:
        return await self.inboxes[node_id].get()
