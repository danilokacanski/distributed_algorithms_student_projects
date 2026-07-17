"""Structured terminal output (rich): per-phase logs, events and summaries.

Three levels:
    quiet   - nothing during the run, only the final outcome
    normal  - setup tables, key events per phase, outcome + traffic report
    verbose - additionally every single message send/drop
"""

from __future__ import annotations


import time

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .hashring import Membership
from .messages import Message, MsgType
from .params import ConsensusParams

PHASE_STYLES = {
    MsgType.REQUEST: "white",
    MsgType.PREPREPARE1: "cyan",
    MsgType.IN_PREPARE1: "blue",
    MsgType.IN_PREPARE2: "magenta",
    MsgType.OUT_PREPARE: "yellow",
    MsgType.COMMIT: "green",
    MsgType.PREPREPARE2: "bright_cyan",
    MsgType.REPLY: "bright_green",
    MsgType.VIEW_CHANGE: "dark_orange",
}

EVENT_STYLES = {
    "PHASE": "bold cyan",
    "MODEL-1": "bold red",
    "MODEL-2": "bold yellow",
    "VIEW-CHANGE": "bold dark_orange",
    "BYZANTINE": "red",
    "BLOCK": "bold green",
    "CLIENT": "bold white",
    "TIMEOUT": "red",
    "INFO": "dim",
}


class Tracer:
    def __init__(self, level: str = "normal", console: Console | None = None):
        if level not in ("quiet", "normal", "verbose"):
            raise ValueError(f"unknown trace level: {level}")
        self.level = level
        self.console = console or Console(highlight=False)
        self._t0: float | None = None
        self._seen_keys: set[str] = set()

    # -- lifecycle ---------------------------------------------------------

    def start_clock(self) -> None:
        self._t0 = time.monotonic()

    def _stamp(self) -> str:
        if self._t0 is None:
            return "[dim][  setup ][/dim]"
        ms = (time.monotonic() - self._t0) * 1000
        return f"[dim][{ms:>7.0f}ms][/dim]"

    # -- setup output ------------------------------------------------------

    def banner(self, name: str, description: str, params: ConsensusParams, seed: int) -> None:
        if self.level == "quiet":
            return
        r, t = params.tolerance_interval
        body = (
            f"[bold]{description or name}[/bold]\n\n"
            f"n = {params.n} nodes   m = {params.m} per group   R = {params.R} groups\n"
            f"E = {params.E} byzantine/group   w = {params.w} faulty groups tolerated\n"
            f"fault-tolerance interval [bold][{r}, {t}][/bold]   "
            f"(1/3 of n = {params.n / 3:.1f})\n"
            f"vote threshold (R-w)*m = {params.vote_threshold}   "
            f"reply quorum = {params.reply_quorum}   seed = {seed}"
        )
        self.console.print(Panel(body, title=f"NBFT simulator - {name}", border_style="cyan"))

    def layout(self, membership: Membership, byzantine: dict[str, str]) -> None:
        if self.level == "quiet":
            return
        table = Table(title=f"Network layout (view {membership.view})", box=box.SIMPLE_HEAVY)
        table.add_column("role", style="bold")
        table.add_column("nodes")

        def paint(nid: str) -> str:
            mark = f"[red]{nid} ({byzantine[nid]})[/red]" if nid in byzantine else nid
            return mark

        table.add_row("primary", paint(membership.primary))
        for g, members in enumerate(membership.groups):
            rep = membership.representatives[g]
            cells = ", ".join(f"[underline]{paint(nid)}[/underline]" if nid == rep else paint(nid) for nid in members)
            table.add_row(f"group {g}", cells + "  [dim](underlined = representative)[/dim]" if g == 0 else cells)
        if membership.ungrouped:
            table.add_row("ungrouped", ", ".join(paint(nid) for nid in membership.ungrouped))
        self.console.print(table)

    # -- runtime output ----------------------------------------------------

    def on_send(self, msg: Message, recipient: str) -> None:
        if self.level != "verbose":
            return
        style = PHASE_STYLES.get(msg.type, "white")
        self.console.print(
            f"{self._stamp()} [{style}]{msg.type.value:<12}[/{style}] {msg.sender} -> {recipient}  {msg.short()}"
        )

    def on_drop(self, msg: Message, recipient: str) -> None:
        if self.level != "verbose":
            return
        self.console.print(
            f"{self._stamp()} [red strike]{msg.type.value:<12}[/red strike] {msg.sender} -> {recipient}  LOST"
        )

    def event(self, tag: str, text: str, key: str | None = None) -> None:
        """Log a notable event. When `key` is given, only the first event
        with that key is printed at normal level (the rest only in verbose) -
        used for choruses like every node voting for the same view change."""
        if self.level == "quiet":
            return
        if key is not None and self.level != "verbose":
            if key in self._seen_keys:
                return
            self._seen_keys.add(key)
        style = EVENT_STYLES.get(tag, "white")
        self.console.print(f"{self._stamp()} [{style}]{tag:<12}[/{style}] {text}")

    # -- final output ------------------------------------------------------

    def traffic_report(self, sent: dict, dropped: dict, params: ConsensusParams, rounds: int = 1) -> None:
        if self.level == "quiet":
            return
        table = Table(title="Message traffic", box=box.SIMPLE_HEAVY)
        table.add_column("phase")
        table.add_column("sent", justify="right")
        table.add_column("lost", justify="right")
        for mtype in MsgType:
            if sent.get(mtype, 0) or dropped.get(mtype, 0):
                style = PHASE_STYLES.get(mtype, "white")
                table.add_row(
                    f"[{style}]{mtype.value}[/{style}]",
                    str(sent.get(mtype, 0)),
                    str(dropped.get(mtype, 0)),
                )
        total = sum(sent.values())
        consensus = sum(c for t, c in sent.items() if t not in (MsgType.REQUEST, MsgType.REPLY))
        n, m, r = params.n, params.m, params.R
        nbft_theory = 2 * (n - 1) + 2 * (m - 1) * r + r * r
        pbft_theory = (n - 1) + (n - 1) ** 2 + n * (n - 1)
        table.add_section()
        table.add_row("[bold]total[/bold]", f"[bold]{total}[/bold]", "")
        self.console.print(table)
        per_round = consensus / max(rounds, 1)
        self.console.print(
            f"consensus traffic: [bold]{consensus}[/bold] msgs / {rounds} round(s) = "
            f"[bold]{per_round:.0f}[/bold] per round   "
            f"(Formula 4: NBFT ~ {nbft_theory}, PBFT would need ~ {pbft_theory})"
        )

    def outcome(self, success: bool, text: str) -> None:
        style = "green" if success else "red"
        title = "CONSENSUS REACHED" if success else "CONSENSUS FAILED"
        self.console.print(Panel(text, title=title, border_style=style))
