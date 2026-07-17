"""
network.py
----------
Mrezni sloj koji prenosi poruke izmedju cvorova.

Modelira:
  - kasnjenje poruka (razlicito u "dobrom" i "losem" periodu, tj. posle/pre GST)
  - gubitak poruka (verovatnoca drop_prob)
  - pale cvorove (crash): ne salju i ne primaju nista

Implementacija: jedna posebna nit (dispatcher) cuva hip dogadjaja isporuke
poredjanih po vremenu isporuke i isporucuje poruku u inbox primaoca kada dodje vreme.
"""

from __future__ import annotations

import heapq
import random
import threading
import time
from typing import Dict, List, Tuple

from messages import Message


class Network:
    def __init__(self, cfg, logger):
        self.cfg = cfg
        self.logger = logger
        self.nodes: Dict[int, "object"] = {}
        self.crashed = set(cfg.crashed)

        self._heap: List[Tuple[float, int, int, Message]] = []
        self._seq = 0
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._running = True
        self._rng = random.Random(cfg.seed)

        self.start_time = 0.0
        self._dispatcher = threading.Thread(target=self._dispatch_loop,
                                            name="Network", daemon=True)

        self.sent = 0
        self.delivered = 0
        self.dropped = 0

    # ------------------------------------------------------------------ #
    def register(self, node) -> None:
        self.nodes[node.id] = node

    def start(self) -> None:
        self.start_time = time.time()
        self._dispatcher.start()

    def stop(self) -> None:
        self._running = False
        with self._cv:
            self._cv.notify_all()
        self._dispatcher.join(timeout=1.0)

    def stats(self) -> Dict[str, int]:
        return {"sent": self.sent, "delivered": self.delivered, "dropped": self.dropped}

    # ------------------------------------------------------------------ #
    def broadcast(self, sender_id: int, msg: Message) -> None:
        for rid in list(self.nodes.keys()):
            self.send_to(sender_id, rid, msg)

    def send_to(self, sender_id: int, recipient_id: int, msg: Message) -> None:
        if sender_id in self.crashed:
            return
        delay, dropped = self._conditions()
        if dropped:
            with self._lock:
                self.dropped += 1
            return
        deliver_at = time.time() + delay
        with self._cv:
            self.sent += 1
            heapq.heappush(self._heap, (deliver_at, self._seq, recipient_id, msg))
            self._seq += 1
            self._cv.notify()

    # ------------------------------------------------------------------ #
    def _conditions(self) -> Tuple[float, bool]:
        now = time.time() - self.start_time
        if now < self.cfg.gst:
            delay = self._rng.uniform(self.cfg.bad_delay_min, self.cfg.bad_delay_max)
        else:
            delay = self._rng.uniform(self.cfg.min_delay, self.cfg.max_delay)
        dropped = self._rng.random() < self.cfg.drop_prob
        return delay, dropped

    def _dispatch_loop(self) -> None:
        while self._running:
            with self._cv:
                if not self._heap:
                    self._cv.wait(timeout=0.1)
                    continue
                deliver_at, seq, rid, msg = self._heap[0]
                now = time.time()
                if deliver_at > now:
                    self._cv.wait(timeout=min(0.1, deliver_at - now))
                    continue
                heapq.heappop(self._heap)
            if rid not in self.crashed and rid in self.nodes:
                self.nodes[rid].inbox.put(msg)
                with self._lock:
                    self.delivered += 1