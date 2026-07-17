"""
config.py
---------
Konfiguracija simulacije i unapred definisani scenariji 

Sve sto je bitno za demonstraciju moze da se promeni ovde ili preko komandne linije

"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Tuple


@dataclass
class SimConfig:
    # --- velicina sistema ---
    n: int = 4                       # ukupan broj cvorova
    f: int = 1                       # gornja granica vizantijske glasacke moci; mora n > 3f
    num_heights: int = 1             # koliko visina (blokova) treba odluciti pre zaustavljanja

    # --- glasacka moc po cvoru ---
    # Ako je prazno, svi cvorovi imaju moc 1 (prosta rotacija). Ako je zadato, duzina
    # mora biti n, i proposer se bira weighted round-robin (srazmerno moci).
    voting_power: Tuple[int, ...] = ()

    # --- tajmeri (u sekundama); stvarni tajmer = init + round * delta ---
    init_timeout_propose: float = 0.6
    init_timeout_prevote: float = 0.6
    init_timeout_precommit: float = 0.6
    timeout_delta: float = 0.3

    # --- mreza: kasnjenje u "dobrom periodu" (posle GST) ---
    min_delay: float = 0.02
    max_delay: float = 0.08
    drop_prob: float = 0.0           # verovatnoca gubitka poruke (0 = pouzdana isporuka)

    # --- delimicna sinhronija: pre GST kasnjenja su velika (asinhroni "losi period") ---
    gst: float = 0.0                 # sekundi od pocetka; 0 = uvek dobar period
    bad_delay_min: float = 0.7
    bad_delay_max: float = 1.5

    # --- greske ---
    crashed: Tuple[int, ...] = ()    # cvorovi koji su "pali" (cute) od pocetka
    byzantine: Tuple[int, ...] = ()  # cvorovi koji se ponasaju vizantijski
    byzantine_mode: str = "equivocate"   # equivocate | vote_nil | random

    # --- ostalo ---
    seed: int = 42
    poll_interval: float = 0.01      # koliko cesto cvor proverava tajmere/poruke
    max_runtime: float = 30.0        # bezbednosna granica trajanja simulacije

    def power_of(self, node_id: int) -> int:
        """Glasacka moc cvora. Ako voting_power nije zadat, svi imaju moc 1."""
        if not self.voting_power:
            return 1
        return self.voting_power[node_id]

    def total_power(self) -> int:
        """Ukupna glasacka moc sistema (to je 'n' iz rada)."""
        return sum(self.power_of(i) for i in range(self.n))

    def validate(self) -> None:
        # n iz rada je UKUPNA glasacka moc (za jednaku moc = broj cvorova).
        assert self.total_power() > 3 * self.f, \
            f"Mora vaziti n > 3f, gde je n ukupna glasacka moc ({self.total_power()}), a f={self.f}."
        if self.voting_power:
            assert len(self.voting_power) == self.n, \
                f"voting_power mora imati tacno n={self.n} elemenata."
            assert all(pw >= 0 for pw in self.voting_power), \
                "Glasacka moc ne moze biti negativna."
        for s in self.crashed:
            assert 0 <= s < self.n, f"Nevalidan id palog cvora: {s}"
        for s in self.byzantine:
            assert 0 <= s < self.n, f"Nevalidan id vizantijskog cvora: {s}"
        assert not (set(self.crashed) & set(self.byzantine)), \
            "Cvor ne moze biti i pao i vizantijski."
        # Vizantijska (faulty) glasacka moc mora biti unutar granice f.
        byz_power = sum(self.power_of(s) for s in self.byzantine)
        assert byz_power <= self.f, \
            f"Vizantijska glasacka moc ({byz_power}) mora biti <= f ({self.f})."


# Unapred pripremljeni scenariji. Svaki demonstrira jedan tip ponasanja/greske.
SCENARIOS = {
    # 1) Idealan slucaj: nema gresaka, odluka u rundi 0.
    "happy": SimConfig(),

    # 2) Lider (proposer runde 0) ne odgovara -> tajmaut -> prelaz u rundu 1 -> odluka.
    #    proposer(h=0, r=0) = cvor 0, pa rusimo bas njega.
    "crash-proposer": SimConfig(crashed=(0,)),

    # 3) Pao jedan "obican" cvor (ne proposer). Preostala 3 (= 2f+1) ipak postizu konsenzus.
    "crash-follower": SimConfig(crashed=(1,)),

    # 4) Vizantijski cvor koji ekvivokira (salje protivrecne poruke razlicitim cvorovima).
    #    Bezbednost se cuva, a sistem se oporavi u sledecoj rundi.
    "byzantine": SimConfig(byzantine=(0,), byzantine_mode="equivocate"),

    # 5) Promenljiva (umerena) kasnjenja u mrezi. Konsenzus se i dalje postize.
    "delays": SimConfig(min_delay=0.05, max_delay=0.45),

    # 6) Delimicna sinhronija: asinhroni "losi period" pre GST, pa pouzdan period posle.
    #    Runde se vrte (nil glasovi) dok ne nastupi dobar period, pa se onda odlucuje.
    "asynchrony": SimConfig(gst=2.0, bad_delay_min=0.8, bad_delay_max=1.6,
                            init_timeout_propose=0.5, init_timeout_prevote=0.5,
                            init_timeout_precommit=0.5, timeout_delta=0.3,
                            max_runtime=30.0),

    # 7) Vise blokova zaredom (sekvenca instanci konsenzusa).
    "multi-height": SimConfig(num_heights=3),

    # 8) Razlicita glasacka moc: moci [3,2,1,1] -> ukupno 7 (= n iz rada), f=2, kvorum 2f+1 = 5.
    #    Proposer se bira weighted round-robin (N0 je lider srazmerno cesce od N2/N3).
    "weighted": SimConfig(n=4, f=2, voting_power=(3, 2, 1, 1)),

    # 9) Razlicita moc + vizantijski cvor male moci (N2, moc 1 <= f=2). Bezbednost ocuvana i sa tezinama.
    "weighted-byz": SimConfig(n=4, f=2, voting_power=(3, 2, 1, 1),
                              byzantine=(2,), byzantine_mode="equivocate"),

    # 10) Razlicita moc + pad NAJJACEG cvora (N0, proposer runde 0). Sistem preskace u
    #     rundu 1 gde je novi lider (N1), pa se u logu vidi smena proposera kroz runde.
    #     f=1 (kvorum 3) da bi preostala moc (4) i dalje dostizala kvorum posle pada N0.
    "weighted-crash": SimConfig(n=4, f=1, voting_power=(3, 2, 1, 1), crashed=(0,)),
}


def get_scenario(name: str, **overrides) -> SimConfig:
    """Vrati kopiju scenarija sa eventualnim izmenama (npr. iz komandne linije)."""
    if name not in SCENARIOS:
        raise KeyError(f"Nepoznat scenario '{name}'. Dostupni: {', '.join(SCENARIOS)}")
    cfg = SCENARIOS[name]
    if overrides:
        cfg = replace(cfg, **overrides)
    cfg.validate()
    return cfg