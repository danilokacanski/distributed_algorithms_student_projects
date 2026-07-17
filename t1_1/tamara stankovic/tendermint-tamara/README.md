# Tendermint BFT — simulator konsenzusa

Simulator Tendermint algoritma za vizantijski tolerantan konsenzus (

Svaki čvor je **zasebna nit** (`threading.Thread`) i komunicira **isključivo porukama**.
Pokriveni su scenariji sa padom čvorova, kašnjenjima, GST, vizantijskim ponašanjem,
više visina (blokova) i **različitom glasačkom moći** (weighted round-robin).

## Zahtevi
- Python 3.9+ (koristi se samo standardna biblioteka; nema spoljnih zavisnosti)

## Pokretanje
Iz foldera projekta:

```bash
python main.py --scenario happy
python main.py --list                       # spisak svih scenarija
python main.py --scenario byzantine --verbose
python main.py --scenario weighted          # razlicita glasacka moc
```

## Scenariji
| Scenario | Opis |
|---|---|
| `happy` | Idealni uslovi, odluka već u rundi 0. |
| `crash-proposer` | Pao proposer runde 0 (N0); oporavak u rundi 1 (živost). |
| `crash-follower` | Pao običan čvor; preostalih 2f+1 ipak odlučuje. |
| `byzantine` | N0 ekvivokira (dva predloga); bezbednost očuvana. |
| `delays` | Umerena kašnjenja manja od tajmauta. |
| `asynchrony` | Asinhroni period pre GST, pa stabilizacija. |
| `multi-height` | Tri bloka (visine) zaredom. |
| `weighted` | Različita glasačka moć [3,2,1,1] (ukupno 7, f=2, kvorum 5); weighted round-robin. |
| `weighted-byz` | Različita moć + vizantijski čvor male moći; bezbednost očuvana. |
| `weighted-crash` | Različita moć + pad najjačeg čvora (N0); vidi se smena proposera kroz runde. |
Scenario weighted-crash je demonstracioni crash scenario u kome je f podešen tako da preostala glasačka moć može dostići kvorum; koristi se za prikaz smene proposera, a ne kao standardni slučaj maksimalne vizantijske tolerancije.

## Opcije komandne linije
| Opcija | Značenje |
|---|---|
| `--scenario IME` | izbor scenarija (podrazumevano `happy`) |
| `--list` | ispiši dostupne scenarije |
| `--nodes N` | broj čvorova (override) |
| `--f F` | tolerancija f, mora n > 3f (override) |
| `--num-heights K` | broj visina/blokova (override) |
| `--init-timeout S` | početni tajmaut u sekundama (override) |
| `--timeout-delta S` | prirast tajmauta po rundi (override) |
| `--seed N` | seme generatora slučajnih brojeva |
| `--verbose` | loguj i prijem svake poruke |
| `--no-color` | isključi ANSI boje (npr. za upis u fajl) |

Primer kombinovanja: `python main.py --scenario happy --nodes 7 --f 2`

## Struktura projekta
| Fajl | Uloga |
|---|---|
| `messages.py` | Tipovi i struktura poruka; funkcija `id(v)`. |
| `node.py` | Logika algoritma: stanje, „upon“ pravila, tajmauti; klase `Node` i `ByzantineNode`. |
| `network.py` | Prenos poruka; kašnjenja, gubici, GST. |
| `config.py` | Parametri simulacije, glasačka moć i scenariji. |
| `logger.py` | Thread-safe logovanje i provera svojstava u rezimeu. |
| `main.py` | Pokretanje simulacije i orkestracija niti. |

## Glasačka moć (weighted round-robin)
Ako `voting_power` u `config.py` nije zadat, svi čvorovi imaju moć 1 i sve se ponaša kao
prosta rotacija. Kada je zadata različita moć:
- proposer se bira **srazmerno moći** (čvor sa moći 3 je lider 3× češće);
- kvorum `2f+1` i prag `f+1` računaju se po **zbiru glasačke moći**, ne po broju čvorova;
- `n` iz rada je **ukupna** glasačka moć, a `f` je gornja granica vizantijske moći (n > 3f).

## Provera svojstava
Po završetku svake simulacije ispisuje se rezime sa automatskom proverom tri svojstva:
**Saglasnost (Agreement)**, **Validnost (Validity)** i **Terminacija (Termination)**.