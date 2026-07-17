"""Minimal client — submits commands and accepts a result once f+1 replicas
report the same outcome (Algorithm 2 line 33: replicas reply to the client;
f+1 matching replies means at least one correct replica decided it)."""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from .replica import Replica, cmd_key


class Client:
    def __init__(self, replicas: list[Replica], f: int):
        self.replicas = replicas
        self.f = f
        self._replies: dict[str, dict[int, Any]] = defaultdict(dict)
        self.accepted: dict[str, Any] = {}   # cmd_key -> agreed result

    def submit(self, cmd: Any) -> None:
        """Hand the command to every replica (whichever is leader will propose)."""
        for r in self.replicas:
            r.submit(cmd)

    def on_execute(self, rid: int, cmd: Any, result: Any) -> None:
        """Callback replicas invoke on DECIDE. Accept on f+1 matching replies."""
        k = cmd_key(cmd)
        if k in self.accepted:
            return
        self._replies[k][rid] = result
        tally: dict[str, int] = defaultdict(int)
        for res in self._replies[k].values():
            tally[repr(res)] += 1
        if max(tally.values()) >= self.f + 1:
            self.accepted[k] = result
