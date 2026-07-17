"""
node.py
-------
Cvor (proces) koji izvrsava Tendermint konsenzus. 

Kljucna ideja arhitekture (zbog zahteva specifikacije za KONKURENTNO izvrsavanje):
svaki cvor je ZASEBNA NIT (threading.Thread). Nit:
  - prima poruke iz svog sanduceta (inbox, thread-safe Queue),
  - cuva ih u lokalnom dnevniku poruka (message_log),
  - proverava "upon" pravila iz algoritma i reaguje (salje poruke, menja stanje),
  - okida tajmaute kada isteknu (OnTimeoutPropose/Prevote/Precommit).

Bezbednost niti (thread-safety): SVE stanje cvora menja iskljucivo NJEGOVA sopstvena
nit. Jedini ulaz spolja je inbox (Queue, vec sinhronizovan). Zato nigde ne trebaju
dodatne brave oko stanja cvora, sto kod cini jednostavnijim i blizim pseudokodu.

Glasacka moc: cvorovi mogu imati razlicitu glasacku moc (voting power). Proposer se
tada bira otezanim kruznim rasporedom (weighted round-robin), a kvorum 2f+1 se racuna
po ZBIRU glasacke moci posiljalaca. Kada svi cvorovi imaju jednaku moc (podrazumevano),
sve se svodi na prostu rotaciju i brojanje cvorova.

Mapiranje "upon" pravila na metode (broj linije je iz Algoritma 1 u radu):
  linija 22 -> _rule_proposal_initial   (predlog sa validRound = -1)
  linija 28 -> _rule_proposal_with_vr   (predlog sa validRound >= 0)
  linija 34 -> _rule_prevote_timer      (2f+1 PREVOTE bilo cega -> zakazi tajmaut)
  linija 36 -> _rule_lock_precommit     (2f+1 PREVOTE za id(v) -> zakljucaj + PRECOMMIT)
  linija 44 -> _rule_prevote_nil        (2f+1 PREVOTE nil -> PRECOMMIT nil)
  linija 47 -> _rule_precommit_timer    (2f+1 PRECOMMIT bilo cega -> zakazi tajmaut)
  linija 49 -> _rule_decide             (predlog + 2f+1 PRECOMMIT za id(v) -> ODLUKA)
  linija 55 -> _rule_round_skip         (f+1 poruka iz vise runde -> preskoci rundu)
"""

from __future__ import annotations

import itertools
import queue
import random
import threading
import time
from typing import Optional, Set

from messages import Message, MessageType, value_id

# Koraci unutar runde (propose < prevote < precommit), kodirani brojem radi poredjenja.
STEP_PROPOSE, STEP_PREVOTE, STEP_PRECOMMIT = 0, 1, 2
STEP_NAME = {STEP_PROPOSE: "propose", STEP_PREVOTE: "prevote", STEP_PRECOMMIT: "precommit"}

# Marker za "bilo koja vrednost" (zvezdica * iz rada) prilikom brojanja poruka.
_ANY = object()


class Node(threading.Thread):
    """Ispravan (correct) Tendermint cvor."""

    def __init__(self, node_id: int, cfg, network, logger,
                 stop_event: threading.Event):
        super().__init__(name=f"N{node_id}", daemon=True)
        self.id = node_id
        self.cfg = cfg
        self.network = network
        self.log = logger
        self.stop_event = stop_event

        # Sanduce za dolazne poruke. Mrezni sloj ubacuje poruke ovde (put),
        # a nit cvora ih cita (get). Queue je vec thread-safe.
        self.inbox: queue.Queue = queue.Queue()

        # --- stanje algoritma (inicijalizacija, linije 2-9 u radu) ---
        self.h = 0                       # h_p   : trenutna visina (instanca konsenzusa)
        self.round = 0                   # round_p
        self.step = STEP_PROPOSE         # step_p
        self.decisions: dict[int, Optional[str]] = {}   # decision_p[]
        self.locked_value: Optional[str] = None         # lockedValue_p
        self.locked_round = -1                          # lockedRound_p
        self.valid_value: Optional[str] = None          # validValue_p
        self.valid_round = -1                           # validRound_p

        # Lokalni dnevnik svih primljenih poruka (na osnovu njega se proveravaju pravila).
        self.message_log: list[Message] = []

        # --- pomocno: zakazani tajmauti i "for the first time" oznake ---
        # svaki tajmaut: (rok_monotonic, vrsta, h, r)
        self._timers: list[tuple[float, str, int, int]] = []
        self._fired_prevote_timer: Set[tuple[int, int]] = set()    # pravilo 34
        self._fired_precommit_timer: Set[tuple[int, int]] = set()  # pravilo 47
        self._fired_lock: Set[tuple[int, int]] = set()             # pravilo 36
        self._value_counter = itertools.count()

        self.done = False                # True kada su sve trazene visine resene

    # ====================================================================
    #  Pomocne funkcije (glasacka moc, proposer, validnost, sveza vrednost)
    # ====================================================================
    @property
    def quorum(self) -> int:
        """Kvorum 2f+1, izrazen u glasackoj moci. Kada svi cvorovi imaju moc 1,
        to je prosto broj cvorova (2f+1)."""
        return 2 * self.cfg.f + 1

    @property
    def f_plus_1(self) -> int:
        return self.cfg.f + 1

    def proposer(self, height: int, rnd: int) -> int:
        """proposer(h, r): izbor proposera otezanim kruznim rasporedom (weighted
        round-robin), srazmerno glasackoj moci cvorova, kao u radu.

        Postupak: svakom cvoru se svake runde prioritet uveca za njegovu glasacku
        moc; lider je cvor sa najvecim prioritetom, kome se potom oduzme UKUPNA moc.
        Pri izjednacenim prioritetima bira se manji id. Globalni korak je (height+rnd).

        Kada svi cvorovi imaju jednaku moc, ovo se svodi na prostu rotaciju
        (height + rnd) % n.
        """
        powers = [self.cfg.power_of(i) for i in range(self.cfg.n)]
        total = sum(powers)
        pri = [0] * self.cfg.n
        leader = 0
        for _ in range(height + rnd + 1):
            for i in range(self.cfg.n):
                pri[i] += powers[i]
            leader = max(range(self.cfg.n), key=lambda i: (pri[i], -i))
            pri[leader] -= total
        return leader

    def is_proposer(self, height: int, rnd: int) -> bool:
        return self.proposer(height, rnd) == self.id

    def valid(self, value: Optional[str]) -> bool:
        """valid() predikat iz rada. Vrednost je nevalidna ako pocinje sa INVALID."""
        return value is not None and not value.startswith("INVALID")

    def get_value(self) -> str:
        """Sveza vrednost koju lider predlaze (u praksi: blok transakcija)."""
        return f"V[h{self.h}:r{self.round}:N{self.id}#{next(self._value_counter)}]"

    # --- brojanje glasova ---
    def _voters(self, mtype: MessageType, height: int, rnd: int, vid=_ANY) -> Set[int]:
        """
        Skup posiljalaca poruka datog tipa za (height, rnd) i (opciono) dati id(v).
        Svaki posiljalac se broji najvise jednom: ako vizantijski cvor posalje vise
        konfliktnih glasova, on je i dalje samo JEDAN posiljalac (njegova glasacka moc).
        """
        senders: Set[int] = set()
        for m in self.message_log:
            if m.type is not mtype or m.height != height or m.round != rnd:
                continue
            if vid is not _ANY and m.value_id != vid:
                continue
            senders.add(m.sender)
        return senders

    def _voting_power(self, senders) -> int:
        """Zbir glasacke moci datih posiljalaca (za jednaku moc = njihov broj)."""
        return sum(self.cfg.power_of(s) for s in senders)

    def _has_quorum(self, mtype, height, rnd, vid=_ANY) -> bool:
        """True ako posiljaoci odgovarajucih poruka imaju zbirnu glasacku moc
        najmanje 2f+1 (aggregate voting power iz rada)."""
        return self._voting_power(self._voters(mtype, height, rnd, vid)) >= self.quorum

    def _find_proposal(self, height: int, rnd: int) -> Optional[Message]:
        """Prvi PROPOSAL koji imamo za (height, rnd) od proposera te runde."""
        prop = self.proposer(height, rnd)
        for m in self.message_log:
            if (m.type is MessageType.PROPOSAL and m.height == height
                    and m.round == rnd and m.sender == prop):
                return m
        return None

    def _find_proposal_for_id(self, height: int, rnd: int, vid: str) -> Optional[Message]:
        prop = self.proposer(height, rnd)
        for m in self.message_log:
            if (m.type is MessageType.PROPOSAL and m.height == height
                    and m.round == rnd and m.sender == prop
                    and value_id(m.value) == vid):
                return m
        return None

    # ====================================================================
    #  Slanje poruka (izdvojeno da bi vizantijski cvor mogao da prepise)
    # ====================================================================
    def broadcast(self, msg: Message) -> None:
        """Posalji istu poruku svim cvorovima (ukljucujuci sebe). Tacka prepisivanja
        za vizantijski cvor (ekvivokacija)."""
        self.network.broadcast(self.id, msg)

    def _mk(self, mtype: MessageType, value=None, vid=None, valid_round=-1) -> Message:
        return Message(mtype, self.h, self.round, self.id,
                       value=value, value_id=vid, valid_round=valid_round)

    def _send_proposal(self, value: str, valid_round: int):
        msg = self._mk(MessageType.PROPOSAL, value=value,
                       vid=value_id(value), valid_round=valid_round)
        self.log.log(self.id, "send",
                     f"PROPOSAL salje  v={value_id(value)} (vr={valid_round})  "
                     f"[h={self.h} r={self.round}]")
        self.broadcast(msg)

    def _send_prevote(self, vid: Optional[str]):
        msg = self._mk(MessageType.PREVOTE, vid=vid)
        self.log.log(self.id, "send",
                     f"PREVOTE  salje  {vid if vid else 'nil'}  [h={self.h} r={self.round}]")
        self.broadcast(msg)

    def _send_precommit(self, vid: Optional[str]):
        msg = self._mk(MessageType.PRECOMMIT, vid=vid)
        self.log.log(self.id, "send",
                     f"PRECOMMIT salje {vid if vid else 'nil'}  [h={self.h} r={self.round}]")
        self.broadcast(msg)

    # ====================================================================
    #  StartRound (linije 11-21 u radu)
    # ====================================================================
    def start_round(self, rnd: int):
        self.round = rnd
        self.step = STEP_PROPOSE
        prop = self.proposer(self.h, rnd)
        self.log.log(self.id, "round",
                     f"START runda r={rnd}  (proposer = N{prop}"
                     f"{'  <= JA' if prop == self.id else ''})")
        if self.is_proposer(self.h, rnd):
            # Ako vec imamo validValue, ponovo predlazemo nju (uz validRound), inace svezu vrednost.
            if self.valid_value is not None:
                proposal = self.valid_value
            else:
                proposal = self.get_value()
            self._send_proposal(proposal, self.valid_round)
        else:
            self._schedule_timeout(self._timeout_propose(rnd), "propose", self.h, rnd)

    # ====================================================================
    #  Tajmauti  (timeoutX(r) = init + r * delta, kao u radu)
    # ====================================================================
    def _timeout_propose(self, r: int) -> float:
        return self.cfg.init_timeout_propose + r * self.cfg.timeout_delta

    def _timeout_prevote(self, r: int) -> float:
        return self.cfg.init_timeout_prevote + r * self.cfg.timeout_delta

    def _timeout_precommit(self, r: int) -> float:
        return self.cfg.init_timeout_precommit + r * self.cfg.timeout_delta

    def _schedule_timeout(self, delay: float, kind: str, h: int, r: int):
        self._timers.append((time.monotonic() + delay, kind, h, r))

    def _check_timeouts(self) -> bool:
        now = time.monotonic()
        due = [t for t in self._timers if t[0] <= now]
        if not due:
            return False
        self._timers = [t for t in self._timers if t[0] > now]
        changed = False
        for (_deadline, kind, h, r) in due:
            if kind == "propose":
                changed |= self._on_timeout_propose(h, r)
            elif kind == "prevote":
                changed |= self._on_timeout_prevote(h, r)
            elif kind == "precommit":
                changed |= self._on_timeout_precommit(h, r)
        return changed

    def _on_timeout_propose(self, h, r) -> bool:        # linije 57-60
        if h == self.h and r == self.round and self.step == STEP_PROPOSE:
            self.log.log(self.id, "timeout", f"TAJMAUT propose -> PREVOTE nil  [h={h} r={r}]")
            self._send_prevote(None)
            self.step = STEP_PREVOTE
            return True
        return False

    def _on_timeout_prevote(self, h, r) -> bool:        # linije 61-64
        if h == self.h and r == self.round and self.step == STEP_PREVOTE:
            self.log.log(self.id, "timeout", f"TAJMAUT prevote -> PRECOMMIT nil  [h={h} r={r}]")
            self._send_precommit(None)
            self.step = STEP_PRECOMMIT
            return True
        return False

    def _on_timeout_precommit(self, h, r) -> bool:      # linije 65-67
        if h == self.h and r == self.round:
            self.log.log(self.id, "timeout", f"TAJMAUT precommit -> nova runda r={r + 1}  [h={h}]")
            self.start_round(self.round + 1)
            return True
        return False

    # ====================================================================
    #  "upon" pravila
    # ====================================================================
    def try_rules(self) -> bool:
        """Jednom prodje kroz sva pravila redom; vraca da li se nesto promenilo."""
        changed = False
        changed |= self._rule_proposal_initial()    # 22
        changed |= self._rule_proposal_with_vr()    # 28
        changed |= self._rule_prevote_timer()       # 34
        changed |= self._rule_lock_precommit()      # 36
        changed |= self._rule_prevote_nil()         # 44
        changed |= self._rule_precommit_timer()     # 47
        changed |= self._rule_decide()              # 49
        changed |= self._rule_round_skip()          # 55
        return changed

    def _rule_proposal_initial(self) -> bool:        # linija 22
        if self.step != STEP_PROPOSE:
            return False
        m = self._find_proposal(self.h, self.round)
        if m is None or m.valid_round != -1:
            return False
        v = m.value
        if self.valid(v) and (self.locked_round == -1 or self.locked_value == v):
            self._send_prevote(value_id(v))
        else:
            self._send_prevote(None)
        self.step = STEP_PREVOTE
        return True

    def _rule_proposal_with_vr(self) -> bool:        # linija 28
        if self.step != STEP_PROPOSE:
            return False
        m = self._find_proposal(self.h, self.round)
        if m is None:
            return False
        vr = m.valid_round
        if vr < 0 or vr >= self.round:
            return False
        v = m.value
        # Treba 2f+1 PREVOTE za id(v) u rundi vr (dokaz da je v bila moguca odluka).
        if not self._has_quorum(MessageType.PREVOTE, self.h, vr, value_id(v)):
            return False
        if self.valid(v) and (self.locked_round <= vr or self.locked_value == v):
            self._send_prevote(value_id(v))
        else:
            self._send_prevote(None)
        self.step = STEP_PREVOTE
        return True

    def _rule_prevote_timer(self) -> bool:           # linija 34
        if self.step != STEP_PREVOTE:
            return False
        key = (self.h, self.round)
        if key in self._fired_prevote_timer:
            return False
        if not self._has_quorum(MessageType.PREVOTE, self.h, self.round, _ANY):
            return False
        self._fired_prevote_timer.add(key)
        self._schedule_timeout(self._timeout_prevote(self.round), "prevote", self.h, self.round)
        return True

    def _rule_lock_precommit(self) -> bool:          # linija 36
        if self.step < STEP_PREVOTE:                 # uslov: step >= prevote
            return False
        key = (self.h, self.round)
        if key in self._fired_lock:
            return False
        m = self._find_proposal(self.h, self.round)
        if m is None or not self.valid(m.value):
            return False
        v = m.value
        if not self._has_quorum(MessageType.PREVOTE, self.h, self.round, value_id(v)):
            return False
        self._fired_lock.add(key)
        if self.step == STEP_PREVOTE:
            self.locked_value = v
            self.locked_round = self.round
            self.log.log(self.id, "lock",
                         f"ZAKLJUCAVA v={value_id(v)} (lockedRound={self.round}) -> PRECOMMIT")
            self._send_precommit(value_id(v))
            self.step = STEP_PRECOMMIT
        self.valid_value = v
        self.valid_round = self.round
        return True

    def _rule_prevote_nil(self) -> bool:             # linija 44
        if self.step != STEP_PREVOTE:
            return False
        if not self._has_quorum(MessageType.PREVOTE, self.h, self.round, None):
            return False
        self._send_precommit(None)
        self.step = STEP_PRECOMMIT
        return True

    def _rule_precommit_timer(self) -> bool:         # linija 47
        key = (self.h, self.round)
        if key in self._fired_precommit_timer:
            return False
        if not self._has_quorum(MessageType.PRECOMMIT, self.h, self.round, _ANY):
            return False
        self._fired_precommit_timer.add(key)
        self._schedule_timeout(self._timeout_precommit(self.round), "precommit", self.h, self.round)
        return True

    def _rule_decide(self) -> bool:                  # linija 49
        if self.h in self.decisions:
            return False
        # Odluka moze nastati na osnovu PRECOMMIT-a iz BILO koje runde te visine.
        rounds = sorted({
            m.round for m in self.message_log
            if m.type is MessageType.PRECOMMIT and m.height == self.h
        })
        for r in rounds:
            ids = {
                m.value_id for m in self.message_log
                if m.type is MessageType.PRECOMMIT and m.height == self.h
                and m.round == r and m.value_id is not None
            }
            for vid in ids:
                if not self._has_quorum(MessageType.PRECOMMIT, self.h, r, vid):
                    continue
                prop = self._find_proposal_for_id(self.h, r, vid)
                if prop is None or not self.valid(prop.value):
                    continue
                # ----- ODLUKA -----
                decided_h = self.h
                self.decisions[decided_h] = prop.value
                self.log.log(self.id, "decide",
                             f"ODLUKA na visini H{decided_h}: {vid} "
                             f"(runda {r}, vrednost='{prop.value}')")
                self.log.record_decision(self.id, decided_h, prop.value, r)
                self.h += 1
                if self.h < self.cfg.num_heights:
                    self._reset_for_new_height()
                    self.start_round(0)
                else:
                    self.done = True
                    self.log.log(self.id, "info",
                                 f"sve ({self.cfg.num_heights}) visine resene")
                return True
        return False

    def _rule_round_skip(self) -> bool:              # linija 55
        best = None
        higher = {
            m.round for m in self.message_log
            if m.height == self.h and m.round > self.round
        }
        for r in higher:
            senders = {
                m.sender for m in self.message_log
                if m.height == self.h and m.round == r
            }
            # f+1 se racuna po zbiru glasacke moci (garantuje bar jedan ispravan cvor).
            if self._voting_power(senders) >= self.f_plus_1 and (best is None or r > best):
                best = r
        if best is not None:
            self.log.log(self.id, "skip",
                         f"f+1 (po moci) iz runde {best} -> preskace iz r={self.round} u r={best}")
            self.start_round(best)
            return True
        return False

    def _reset_for_new_height(self):
        """Resetuje stanje za novu visinu. Cuva poruke ciji je height >= nove visine
        (mogli smo vec primiti poruke za sledecu visinu pre nego sto smo presli na nju)."""
        self.round = 0
        self.step = STEP_PROPOSE
        self.locked_value = None
        self.locked_round = -1
        self.valid_value = None
        self.valid_round = -1
        self.message_log = [m for m in self.message_log if m.height >= self.h]
        self._timers.clear()
        self._fired_prevote_timer.clear()
        self._fired_precommit_timer.clear()
        self._fired_lock.clear()

    # ====================================================================
    #  Glavna petlja niti
    # ====================================================================
    def _drain_inbox(self) -> bool:
        """Pokupi sve trenutno dostupne poruke iz sanduceta u dnevnik. Vraca True ako
        je bar jedna stigla."""
        try:
            m = self.inbox.get(timeout=self.cfg.poll_interval)
        except queue.Empty:
            return False
        self.message_log.append(m)
        self.log.log(self.id, "recv", f"prima {m.short()}")
        while True:
            try:
                m = self.inbox.get_nowait()
            except queue.Empty:
                break
            self.message_log.append(m)
            self.log.log(self.id, "recv", f"prima {m.short()}")
        return True

    def run(self):
        self.start_round(0)
        while not self.stop_event.is_set():
            got = self._drain_inbox()
            if self.done:
                # Vec smo resili sve visine; samo praznimo sanduce dok ne stigne stop.
                continue
            changed = self._check_timeouts()
            if got or changed:
                # Vrtimo pravila do fiksne tacke (jedno pravilo moze omoguciti drugo).
                guard = 0
                while self.try_rules():
                    guard += 1
                    if guard > 1000:
                        self.log.log(self.id, "info", "upozorenje: prekinuta petlja pravila")
                        break


# ======================================================================
#  Vizantijski (Byzantine) cvor
# ======================================================================
class ByzantineNode(Node):
    """
    Cvor koji NE postuje protokol i aktivno pokusava da prevari ostale.

    Podrzani modovi (cfg.byzantine_mode):
      - "equivocate" : kada je proposer, salje DVA razlicita predloga razlicitim
                       grupama cvorova (dupli predlog). Cilj je da pocepa prevote
                       glasove tako da nijedna vrednost ne dobije kvorum 2f+1.
      - "vote_nil"   : uvek glasa nil (PREVOTE/PRECOMMIT nil) i ne predlaze nista
                       korisno. Opstruktivan cvor.
      - "random"     : nasumicno bira izmedju normalnog ponasanja i ekvivokacije.

    Pointa demonstracije (zasto BFT i dalje radi):
      - BEZBEDNOST (safety) se NE narusava. Po Lemi 1 iz rada, svaka dva kvoruma od
        2f+1 dele bar jedan ISPRAVAN cvor, a ispravan cvor salje samo jedan glas po
        rundi. Zato dve razlicite vrednosti nikada ne mogu obe dobiti kvorum u istoj
        rundi -> ispravni cvorovi se ne mogu raziciti.
      - ZIVOST (liveness) se odrzava. U rundi gde je proposer ISPRAVAN cvor, svi
        ispravni cvorovi prevote-uju istu vrednost, zakljucaju je i odluce.

    Posto je logika slanja izdvojena u metodu broadcast() (i pomocne _send_*),
    vizantijski cvor samo prepise broadcast(). Sve ostalo (prijem, pravila,
    tajmauti) nasledi nepromenjeno od baznog Node.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rng = random.Random(self.cfg.seed + 1000 + self.id)

    def broadcast(self, msg: Message) -> None:
        mode = self.cfg.byzantine_mode

        # Glasovi (PREVOTE/PRECOMMIT) u vote_nil modu uvek idu kao nil.
        if mode == "vote_nil" and msg.type in (MessageType.PREVOTE, MessageType.PRECOMMIT):
            nil_msg = Message(msg.type, msg.height, msg.round, self.id, value_id=None)
            self.network.broadcast(self.id, nil_msg)
            return

        # Ekvivokacija predloga: kada saljemo PROPOSAL, posaljemo dva razlicita.
        if msg.type is MessageType.PROPOSAL and mode in ("equivocate", "random"):
            if mode == "random" and self._rng.random() < 0.5:
                return super().broadcast(msg)  # ovaj put se ponasa "normalno"
            self._equivocate_proposal(msg.valid_round)
            return

        # Sve ostalo: normalno emitovanje.
        super().broadcast(msg)

    def _equivocate_proposal(self, valid_round: int):
        # Dve razlicite (ali obe formalno validne) vrednosti.
        v_a = f"BYZ-A[h{self.h}:r{self.round}]"
        v_b = f"BYZ-B[h{self.h}:r{self.round}]"

        others = [nid for nid in range(self.cfg.n) if nid != self.id]
        half = max(1, len(others) // 2)
        group_a = others[:half]
        group_b = others[half:] or others[:half]

        self.log.log(self.id, "byz",
                     f"EKVIVOKACIJA: '{value_id(v_a)}' -> {['N'+str(x) for x in group_a]}  i  "
                     f"'{value_id(v_b)}' -> {['N'+str(x) for x in group_b]}  [h={self.h} r={self.round}]")

        for target in group_a:
            m = Message(MessageType.PROPOSAL, self.h, self.round, self.id,
                        value=v_a, value_id=value_id(v_a), valid_round=valid_round)
            self.network.send_to(self.id, target, m)
        for target in group_b:
            m = Message(MessageType.PROPOSAL, self.h, self.round, self.id,
                        value=v_b, value_id=value_id(v_b), valid_round=valid_round)
            self.network.send_to(self.id, target, m)