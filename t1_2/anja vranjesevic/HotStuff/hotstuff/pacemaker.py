"""Pacemaker — leader selection + (M2) per-view timeout / view-change trigger.

Paper §4.4: HotStuff deliberately factors liveness out of safety. The Pacemaker
decides *who* leads a view and *when* to give up on it; the replica's safety
rules never depend on it being correct. M1 uses a fixed leader; M2 switches on
round-robin rotation and timeouts.
"""
from __future__ import annotations

import asyncio
from typing import Optional


class Pacemaker:
    def __init__(self, n: int, fixed_leader: Optional[int] = None,
                 base_timeout: float = 0.5, timeouts: bool = False):
        self.n = n
        self.fixed_leader = fixed_leader   # M1: pin every view to this replica
        self.base_timeout = base_timeout
        self.timeouts = timeouts           # M2: arm per-view timeout / view-change

    def leader(self, view: int) -> int:
        """leader(view) = view % n  (round-robin) — paper §4.4. M1 pins to one."""
        if self.fixed_leader is not None:
            return self.fixed_leader
        return view % self.n

    def is_leader(self, view: int, node_id: int) -> bool:
        return self.leader(view) == node_id

    def timeout_for(self, view: int, consecutive_failures: int) -> float:
        """Exponential back-off (§4.4): each failed view doubles the patience so
        that once the network is synchronous a correct leader is given enough
        time to drive a decision."""
        return self.base_timeout * (2 ** consecutive_failures)
