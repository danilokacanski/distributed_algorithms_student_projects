"""
logger.py
---------
Thread-safe logovanje rada simulacije i ispis zavrsnog rezimea.

Posto svaki cvor radi u svojoj niti, ispis mora biti zasticen bravom kako se linije
ne bi preplitale. Logger takode belezi odluke cvorova da bi na kraju mogao da proveri
i prikaze svojstva algoritma: Saglasnost (Agreement), Validnost i Terminaciju.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from messages import value_id

# ANSI boje (VS Code terminal ih podrzava). Mogu da se iskljuce sa --no-color.
_PALETTE = [
    "\033[36m",  # cyan
    "\033[32m",  # green
    "\033[33m",  # yellow
    "\033[35m",  # magenta
    "\033[34m",  # blue
    "\033[31m",  # red
    "\033[96m",  # bright cyan
    "\033[92m",  # bright green
]
_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"


@dataclass
class Decision:
    node_id: int
    height: int
    value: Optional[str]
    round: int
    at: float


class SimLogger:
    def __init__(self, start_time: float, verbose: bool = False, color: bool = True):
        self.start = start_time
        self.verbose = verbose       # ako je True, loguju se i prijemi (recv) svake poruke
        self.color = color
        self.lock = threading.Lock()
        self.decisions: List[Decision] = []

    # ------------------------------------------------------------------ #
    def _c(self, code: str) -> str:
        return code if self.color else ""

    def _tag(self, node_id: int) -> str:
        col = self._c(_PALETTE[node_id % len(_PALETTE)])
        return f"{col}[N{node_id}]{self._c(_RESET)}"

    def log(self, node_id: int, kind: str, text: str) -> None:
        """Glavni metod za logovanje jednog dogadjaja."""
        if not self.verbose and kind == "recv":
            return  # prijemi se prikazuju samo u verbose modu (da log ne bude prebukiran)
        t = time.time() - self.start
        with self.lock:
            print(f"{self._c(_DIM)}[+{t:6.3f}s]{self._c(_RESET)} "
                  f"{self._tag(node_id)} {text}", flush=True)

    def record_decision(self, node_id: int, height: int,
                        value: Optional[str], round: int) -> None:
        at = time.time() - self.start
        with self.lock:
            self.decisions.append(Decision(node_id, height, value, round, at))

    # ------------------------------------------------------------------ #
    def print_summary(self, cfg, correct: List[int],
                     net_stats: Dict[str, int]) -> None:
        """Ispisi zavrsni rezime i proveri svojstva algoritma."""
        line = "=" * 64
        sub = "-" * 64
        b = self._c(_BOLD)
        r = self._c(_RESET)

        with self.lock:
            print()
            print(f"{b}{line}{r}")
            print(f"{b} REZIME SIMULACIJE{r}")
            print(f"{b}{line}{r}")
            print(f" Cvorova: {cfg.n}   (f={cfg.f}, kvorum 2f+1 = {2 * cfg.f + 1})")
            print(f" Pali (crash):     {set(cfg.crashed) if cfg.crashed else '-'}")
            print(f" Vizantijski:      {set(cfg.byzantine) if cfg.byzantine else '-'}"
                  + (f" (mod: {cfg.byzantine_mode})" if cfg.byzantine else ""))
            print(f" Visina (blokova): {cfg.num_heights}")
            print(sub)

            # Grupisi odluke po visini i ispisi
            ok_agreement = True
            ok_validity = True
            ok_termination = True

            for h in range(cfg.num_heights):
                vals = {}
                for d in self.decisions:
                    if d.height == h and d.node_id in correct:
                        vals[d.node_id] = d
                # Terminacija: svi korektni cvorovi su odlucili na ovoj visini
                if len(vals) < len(correct):
                    ok_termination = False
                # Saglasnost: sve odluke iste
                decided_values = {d.value for d in vals.values()}
                if len(decided_values) > 1:
                    ok_agreement = False
                # Validnost: nijedna odluka nije nil
                if any(d.value is None for d in vals.values()):
                    ok_validity = False

                print(f" Visina H{h}:")
                for nid in sorted(correct):
                    if nid in vals:
                        d = vals[nid]
                        vid = value_id(d.value)
                        print(f"   N{nid}: odlucio {vid}  (runda {d.round}, "
                              f"vrednost='{d.value}')  @ +{d.at:.3f}s")
                    else:
                        print(f"   N{nid}: (nije odlucio)")
            print(sub)

            def mark(ok: bool) -> str:
                if not self.color:
                    return "OK " if ok else "NIJE"
                return ("\033[92mOK \033[0m" if ok else "\033[91mNIJE\033[0m")

            print(f" SAGLASNOST  (Agreement):   {mark(ok_agreement)}  "
                  f"- nijedna dva korektna cvora ne odlucuju razlicito")
            print(f" VALIDNOST   (Validity):    {mark(ok_validity)}  "
                  f"- odlucena vrednost je validna (nije nil)")
            print(f" TERMINACIJA (Termination): {mark(ok_termination)}  "
                  f"- svi korektni cvorovi su odlucili")
            print(sub)
            print(f" Poruka poslato: {net_stats.get('sent', 0)}   "
                  f"isporuceno: {net_stats.get('delivered', 0)}   "
                  f"izgubljeno: {net_stats.get('dropped', 0)}")
            total_t = time.time() - self.start
            print(f" Ukupno vreme: {total_t:.2f}s")
            print(f"{b}{line}{r}")