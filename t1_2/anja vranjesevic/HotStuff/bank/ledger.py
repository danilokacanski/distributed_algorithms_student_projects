"""The settlement ledger: accounts + a deterministic `apply(TRANSFER)`.

Design point that is the heart of the whole project:

    Consensus ORDERS the commands; the state machine JUDGES them.

HotStuff decides *the order* of transfers. Whether a given transfer succeeds is
then a pure, deterministic function of that order — evaluated identically on every
replica. A transfer that fails (insufficient funds, replayed nonce) is still
committed and still occupies its slot in the log; it just applies as a REJECT.
That is why two replicas can never disagree about the outcome: they judge the
same command against the same prior state.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Transfer:
    """A settlement instruction between two banks. `nonce` makes each transfer
    from a bank unique, so replays are detectable (and hashes are distinct)."""
    sender: str
    to: str
    amount: int
    nonce: int

    def __repr__(self) -> str:
        return f"TRANSFER({self.sender}->{self.to} {self.amount} #{self.nonce})"


# Outcomes are plain strings so they show up readably in logs and compare cheaply
# across replicas.
OK = "OK"
INSUFFICIENT = "REJECT(insufficient_funds)"
NONCE_REUSED = "REJECT(nonce_reused)"
MALFORMED = "REJECT(malformed)"


class Ledger:
    def __init__(self, balances: dict[str, int]):
        self.balances: dict[str, int] = dict(balances)
        self._initial_total = sum(balances.values())
        self.used_nonces: set[tuple[str, int]] = set()
        self.history: list[tuple[Transfer, str]] = []   # (cmd, outcome), in commit order

    def apply(self, cmd: object) -> str:
        """Deterministically apply one committed command. MUST be a pure function
        of (current state, cmd) — no randomness, no wall-clock, no ordering other
        than the one consensus already fixed."""
        if not isinstance(cmd, Transfer) or cmd.amount <= 0 or cmd.sender == cmd.to:
            self.history.append((cmd, MALFORMED))
            return MALFORMED

        key = (cmd.sender, cmd.nonce)
        if key in self.used_nonces:
            outcome = NONCE_REUSED                       # replay / double-submit
        elif self.balances.get(cmd.sender, 0) < cmd.amount:
            outcome = INSUFFICIENT                       # the double-spend loser
            self.used_nonces.add(key)                    # nonce is consumed either way
        else:
            self.balances[cmd.sender] -= cmd.amount
            self.balances[cmd.to] = self.balances.get(cmd.to, 0) + cmd.amount
            self.used_nonces.add(key)
            outcome = OK

        self.history.append((cmd, outcome))
        return outcome

    # ---- invariants a checker can assert after any run ----
    def total(self) -> int:
        return sum(self.balances.values())

    def check_invariants(self) -> bool:
        """Money is conserved and no account goes negative — must hold on every
        correct replica regardless of command order."""
        return self.total() == self._initial_total and all(v >= 0 for v in self.balances.values())

    def __repr__(self) -> str:
        return f"Ledger({self.balances})"
