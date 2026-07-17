"""Simulated network: one asyncio.Queue per node.

The network routes messages with a configurable base delay, random jitter
and loss rate, and keeps per-phase traffic counters that the tracer turns
into the complexity report at the end of a run.
"""

from __future__ import annotations


import asyncio
import random
from collections import Counter
from collections.abc import Iterable

from .messages import Message, MsgType


class Network:
    def __init__(
        self,
        rng: random.Random,
        base_delay_ms: float = 5.0,
        jitter_ms: float = 10.0,
        loss_rate: float = 0.0,
        tracer=None,
    ):
        self.rng = rng
        self.base_delay_ms = base_delay_ms
        self.jitter_ms = jitter_ms
        self.loss_rate = loss_rate
        self.tracer = tracer
        self.queues: dict[str, asyncio.Queue] = {}
        self.sent: Counter = Counter()
        self.dropped: Counter = Counter()
        self._tasks: set[asyncio.Task] = set()

    def register(self, node_id: str) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self.queues[node_id] = queue
        return queue

    def send(self, msg: Message, recipient: str, extra_delay_ms: float = 0.0) -> None:
        """Fire-and-forget unicast with simulated delay and loss."""
        if recipient not in self.queues:
            return
        self.sent[msg.type] += 1
        if self.tracer is not None:
            self.tracer.on_send(msg, recipient)
        if self.loss_rate > 0 and self.rng.random() < self.loss_rate:
            self.dropped[msg.type] += 1
            if self.tracer is not None:
                self.tracer.on_drop(msg, recipient)
            return
        delay = (self.base_delay_ms + self.rng.uniform(0, self.jitter_ms) + extra_delay_ms) / 1000.0
        task = asyncio.get_running_loop().create_task(self._deliver(msg, recipient, delay))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def broadcast(self, msg: Message, recipients: Iterable[str], extra_delay_ms: float = 0.0) -> None:
        for recipient in recipients:
            if recipient != msg.sender:
                self.send(msg, recipient, extra_delay_ms)

    async def _deliver(self, msg: Message, recipient: str, delay: float) -> None:
        await asyncio.sleep(delay)
        queue = self.queues.get(recipient)
        if queue is not None:
            queue.put_nowait(msg)

    def total_sent(self) -> int:
        return sum(self.sent.values())

    def consensus_traffic(self) -> int:
        """Messages spent on consensus itself (client traffic excluded)."""
        return sum(count for mtype, count in self.sent.items() if mtype not in (MsgType.REQUEST, MsgType.REPLY))
