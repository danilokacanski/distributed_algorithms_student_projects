"""Client: sends the request to the primary and waits for replies.

The whole network is considered to have reached consensus once the client
collects (n - 1) / 2 + 1 replies with the same digest from distinct nodes.
When a wait times out the client alerts every node by re-broadcasting the
request, which arms the commit watchdogs that can end in a view change.
"""

from __future__ import annotations


import asyncio
from dataclasses import dataclass

from .config import SimulationConfig
from .messages import Message, MsgType, payload_digest
from .params import ConsensusParams
from .trace import Tracer


@dataclass
class ClientOutcome:
    success: bool
    digest: str
    replies: int
    attempts: int


class Client:
    def __init__(self, cfg: SimulationConfig, params: ConsensusParams, network, tracer: Tracer):
        self.id = "client"
        self.cfg = cfg
        self.params = params
        self.net = network
        self.trace = tracer
        self.inbox = network.register(self.id)
        self._replies: dict[str, set[str]] = {}
        self._quorum_events: dict[str, asyncio.Event] = {}
        self._reader: asyncio.Task | None = None

    def start(self) -> None:
        self._reader = asyncio.get_running_loop().create_task(self._read_replies())

    def stop(self) -> None:
        if self._reader is not None:
            self._reader.cancel()

    async def _read_replies(self) -> None:
        while True:
            msg = await self.inbox.get()
            if msg.type is not MsgType.REPLY:
                continue
            senders = self._replies.setdefault(msg.digest, set())
            senders.add(msg.sender)
            if len(senders) >= self.params.reply_quorum:
                event = self._quorum_events.get(msg.digest)
                if event is not None:
                    event.set()

    async def request(self, payload: str, primary: str, all_nodes: tuple[str, ...]) -> ClientOutcome:
        digest = payload_digest(payload)
        self._replies.setdefault(digest, set())
        event = self._quorum_events.setdefault(digest, asyncio.Event())

        msg = Message(MsgType.REQUEST, self.id, 0, 0, digest=digest, payload=payload)
        self.trace.event("CLIENT", f"request '{payload}' d={digest} -> primary {primary}")
        self.net.send(msg, primary)

        attempts = 0
        for attempt in range(1, self.cfg.client_retries + 1):
            attempts = attempt
            try:
                await asyncio.wait_for(event.wait(), timeout=self.cfg.client_timeout_ms / 1000.0)
                replies = len(self._replies[digest])
                self.trace.event(
                    "CLIENT",
                    f"accepted d={digest}: {replies} replies >= quorum {self.params.reply_quorum}",
                )
                return ClientOutcome(True, digest, replies, attempts)
            except asyncio.TimeoutError:
                if attempt < self.cfg.client_retries:
                    self.trace.event(
                        "CLIENT",
                        f"no quorum for d={digest} after {self.cfg.client_timeout_ms:.0f}ms - "
                        f"alerting all nodes (attempt {attempt + 1})",
                    )
                    for node in all_nodes:
                        self.net.send(msg, node)

        return ClientOutcome(False, digest, len(self._replies[digest]), attempts)
