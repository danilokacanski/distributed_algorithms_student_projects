"""
main.py
-------
Pokretac (orchestrator) simulacije Tendermint konsenzusa.

Sta radi:
  1. Sastavi konfiguraciju iz izabranog scenarija (+ eventualni override-i iz CLI).
  2. Napravi logger, mrezu i cvorove (ispravne i, po potrebi, vizantijske).
  3. Pokrene mrezu (dispatcher nit) i niti svih cvorova koji NISU pali.
  4. Ceka dok svi ISPRAVNI cvorovi ne odluce sve trazene visine (ili dok ne istekne
     bezbednosna granica max_runtime).
  5. Zaustavi sve, sacheka niti i ispise zavrsni rezime sa proverom svojstava
     (Saglasnost, Validnost, Terminacija).

Primeri (pokretati iz foldera tendermint-tamara/):
    python main.py --scenario happy
    python main.py --scenario crash-proposer
    python main.py --scenario byzantine --verbose
    python main.py --scenario weighted
    python main.py --scenario multi-height
    python main.py --list
    python main.py --scenario happy --nodes 7 --f 2 --no-color > log.txt
"""

from __future__ import annotations

import argparse
import time

from config import SimConfig, SCENARIOS, get_scenario
from logger import SimLogger
from network import Network
from node import Node, ByzantineNode


SCENARIO_OPIS = {
    "happy":         "Idealni uslovi, bez gresaka. Odluka vec u rundi 0.",
    "crash-proposer": "Proposer runde 0 (N0) je pao. Tajmaut -> runda 1 -> odluka (zivost).",
    "crash-follower": "Pao jedan obican cvor (N1). Preostala 3 (=2f+1) ipak odlucuju.",
    "byzantine":     "N0 ekvivokira (dupli predlog). Bezbednost ocuvana, odluka u kasnijoj rundi.",
    "delays":        "Promenljiva kasnjenja u mrezi, ali manja od tajmauta. Konsenzus se postize.",
    "asynchrony":    "Asinhroni 'losi period' pre GST, pa pouzdan period. Runde se vrte do GST.",
    "multi-height":  "Tri bloka (visine) zaredom: sekvenca instanci konsenzusa.",
    "weighted":      "Razlicita glasacka moc [3,2,1,1] (ukupno 7, f=2, kvorum 5). Weighted round-robin izbor lidera.",
    "weighted-byz":  "Razlicita moc + vizantijski cvor MALE moci (N2). Bezbednost ocuvana i sa tezinama.",
    "weighted-crash": "Razlicita moc + pad najjaceg cvora (N0). Vidi se smena proposera kroz runde.",
}


def correct_nodes(cfg: SimConfig) -> list[int]:
    """Ispravni cvorovi: nisu ni pali ni vizantijski."""
    bad = set(cfg.crashed) | set(cfg.byzantine)
    return [i for i in range(cfg.n) if i not in bad]


def print_proposer_schedule(cfg: SimConfig) -> None:
    """Ako je zadata razlicita glasacka moc, ispisi raspored proposera po rundama
    (weighted round-robin) da se vidi da se jaci cvor bira srazmerno cesce."""
    if not cfg.voting_power:
        return
    import threading
    from collections import Counter
    # Probni cvor, samo da pozovemo istu proposer() funkciju (ne pokrece se).
    probe = Node(0, cfg, None, None, threading.Event())
    powers = ", ".join(f"N{i}={cfg.power_of(i)}" for i in range(cfg.n))
    print(f"Glasacka moc: {powers}  (ukupno {cfg.total_power()})")
    print("Raspored proposera po rundama (weighted round-robin):")
    leaders = [probe.proposer(0, r) for r in range(cfg.total_power())]
    line = "  "
    for r, ld in enumerate(leaders):
        line += f"r{r} -> N{ld}    "
        if (r + 1) % 4 == 0:
            print(line.rstrip())
            line = "  "
    if line.strip():
        print(line.rstrip())
    cnt = Counter(leaders)
    summary = "   ".join(f"N{i}: {cnt.get(i, 0)}x" for i in range(cfg.n))
    print(f"  => {summary}   (srazmerno moci)")
    print("-" * 64)


def run_scenario(cfg: SimConfig, verbose: bool, color: bool) -> bool:
    """Pokrene jednu simulaciju. Vraca True ako su svi ispravni cvorovi odlucili sve visine."""
    cfg.validate()
    start = time.time()
    logger = SimLogger(start, verbose=verbose, color=color)
    network = Network(cfg, logger)

    # Napravi cvorove: vizantijski tamo gde je naznaceno, inace ispravni.
    import threading
    stop_event = threading.Event()
    nodes = []
    for i in range(cfg.n):
        if i in cfg.byzantine:
            node = ByzantineNode(i, cfg, network, logger, stop_event)
        else:
            node = Node(i, cfg, network, logger, stop_event)
        nodes.append(node)
        network.register(node)

    correct = correct_nodes(cfg)

    # Najava scenarija.
    print(f"Pokrecem scenario sa n={cfg.n}, f={cfg.f}, kvorum 2f+1={2*cfg.f+1}, "
          f"visina={cfg.num_heights}")
    print(f"Pali: {set(cfg.crashed) if cfg.crashed else '-'}   "
          f"Vizantijski: {set(cfg.byzantine) if cfg.byzantine else '-'}"
          + (f" (mod={cfg.byzantine_mode})" if cfg.byzantine else ""))
    print("-" * 64)
    print_proposer_schedule(cfg)

    # Pokreni mrezu, pa niti svih cvorova koji NISU pali (pali cvorovi cute = ne pokrecemo ih).
    network.start()
    for node in nodes:
        if node.id not in cfg.crashed:
            node.start()

    # Cekaj da svi ISPRAVNI cvorovi odluce sve visine (ili istek vremena).
    deadline = start + cfg.max_runtime
    all_done = False
    while time.time() < deadline:
        if all(len(nodes[i].decisions) >= cfg.num_heights for i in correct):
            all_done = True
            break
        time.sleep(0.02)

    # Zaustavi sve.
    stop_event.set()
    for node in nodes:
        if node.is_alive():
            node.join(timeout=1.0)
    network.stop()

    logger.print_summary(cfg, correct, network.stats())
    if not all_done:
        print("  (napomena: istekla je granica max_runtime pre nego sto su svi odlucili)")
    return all_done


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Simulacija Tendermint BFT konsenzusa (Algoritam 1).")
    p.add_argument("--scenario", default="happy",
                   help="ime scenarija (vidi --list). Podrazumevano: happy")
    p.add_argument("--list", action="store_true", help="ispisi dostupne scenarije i izadji")
    p.add_argument("--nodes", type=int, default=None, help="broj cvorova n (override)")
    p.add_argument("--f", type=int, default=None, help="tolerancija f (override); mora n>3f")
    p.add_argument("--num-heights", type=int, default=None, help="broj visina/blokova (override)")
    p.add_argument("--verbose", action="store_true", help="loguj i prijem svake poruke")
    p.add_argument("--no-color", action="store_true", help="iskljuci ANSI boje")
    p.add_argument("--seed", type=int, default=None, help="seme generatora slucajnih brojeva")
    p.add_argument("--init-timeout", type=float, default=None,
                   help="pocetni tajmaut (sva tri koraka) u sekundama")
    p.add_argument("--timeout-delta", type=float, default=None,
                   help="prirast tajmauta po rundi u sekundama")
    return p


def main():
    args = build_parser().parse_args()

    if args.list:
        print("Dostupni scenariji:")
        for name in SCENARIOS:
            print(f"  {name:16s} - {SCENARIO_OPIS.get(name, '')}")
        return

    # Sastavi override-e iz CLI.
    overrides = {}
    if args.nodes is not None:
        overrides["n"] = args.nodes
    if args.f is not None:
        overrides["f"] = args.f
    if args.num_heights is not None:
        overrides["num_heights"] = args.num_heights
    if args.seed is not None:
        overrides["seed"] = args.seed
    if args.init_timeout is not None:
        overrides["init_timeout_propose"] = args.init_timeout
        overrides["init_timeout_prevote"] = args.init_timeout
        overrides["init_timeout_precommit"] = args.init_timeout
    if args.timeout_delta is not None:
        overrides["timeout_delta"] = args.timeout_delta

    cfg = get_scenario(args.scenario, **overrides)
    ok = run_scenario(cfg, verbose=args.verbose, color=not args.no_color)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()