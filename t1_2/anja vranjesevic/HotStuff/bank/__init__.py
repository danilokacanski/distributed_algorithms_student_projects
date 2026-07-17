"""Interbank settlement ledger — the replicated state machine HotStuff drives.

The consensus core is application-agnostic; this is the only "application". Its
one job is to be a *deterministic* function of the committed command sequence, so
that every correct replica, applying the same ordered log, reaches the same state.
"""
