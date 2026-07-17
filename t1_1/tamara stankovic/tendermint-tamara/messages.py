"""
messages.py
-----------
Tipovi poruka i struktura poruke koju cvorovi razmenjuju u Tendermint protokolu.

cvorovi razmenjuju tri vrste poruka:
  - PROPOSAL   : predlog vrednosti (bloka) od strane proposera tekuce runde
  - PREVOTE    : prvi krug glasanja, nosi id(v) (ili nil)
  - PRECOMMIT  : drugi krug glasanja, nosi id(v) (ili nil)

Vazna ideja iz rada: samo PROPOSAL nosi celu vrednost v (blok, koji moze biti velik),
dok PREVOTE i PRECOMMIT nose samo id(v), tj. kratak, fiksan identifikator (hes).
Vazi: ako je id(v) == id(v'), onda je v == v'.
"""

from __future__ import annotations

import hashlib
import itertools
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class MessageType(Enum):
    PROPOSAL = "PROPOSAL"
    PREVOTE = "PREVOTE"
    PRECOMMIT = "PRECOMMIT"


# Specijalna "nil" vrednost (odsustvo vrednosti). U Pythonu je predstavljamo sa None.
NIL = None


def value_id(value: Optional[str]) -> Optional[str]:
    """id(v): kratak identifikator vrednosti (hes prvih 8 karaktera SHA-256).

    id(nil) = nil. Garancija: id(v) == id(v')  =>  v == v'.
    """
    if value is None:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


# Globalni brojac da svaka poruka ima jedinstven uid (korisno za logovanje/debug).
_counter = itertools.count()


@dataclass
class Message:
    """Jedna protokol-poruka.

    Polja:
      type        - PROPOSAL / PREVOTE / PRECOMMIT
      height      - h, instanca konsenzusa (visina bloka)
      round       - r, redni broj runde u okviru date visine
      sender      - id cvora posiljaoca (digitalni potpis je apstrahovan: znamo ko je poslao)
      value       - CELA vrednost; popunjeno SAMO za PROPOSAL, inace None
      value_id    - id(v); za glasove nosi id vrednosti (ili None za nil),
                    za PROPOSAL nosi id predlozene vrednosti
      valid_round - validRound koji proposer salje uz PROPOSAL (linija 19 u Alg. 1);
                    -1 znaci "nova" vrednost koja nije ranije zakljucana
    """

    type: MessageType
    height: int
    round: int
    sender: int
    value: Optional[str] = None
    value_id: Optional[str] = None
    valid_round: int = -1
    uid: int = field(default_factory=lambda: next(_counter))

    def short(self) -> str:
        """Kratak, citljiv prikaz poruke za logove."""
        vid = self.value_id if self.value_id is not None else "nil"
        if self.type == MessageType.PROPOSAL:
            return (f"PROPOSAL(h={self.height}, r={self.round}, "
                    f"v={vid}, vr={self.valid_round}, from=N{self.sender})")
        return (f"{self.type.value}(h={self.height}, r={self.round}, "
                f"id={vid}, from=N{self.sender})")