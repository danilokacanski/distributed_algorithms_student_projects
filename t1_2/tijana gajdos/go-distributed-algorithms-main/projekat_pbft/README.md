#Practical Byzantine Fault Tolerance (PBFT)

Implementacija **PBFT normalnog toka rada + pojednostavljene promene view-a** (view change), na osnovu rada:

> Miguel Castro, Barbara Liskov — *Practical Byzantine Fault Tolerance and
> Proactive Recovery*, ACM Transactions on Computer Systems, Vol. 20, No. 4,
> novembar 2002.

---

## Kako pokrenuti

```bash
cd projekat_pbft
go run .
```


## Šta je PBFT?

PBFT je algoritam konsenzusa zasnovan na replikaciji mašine stanja (state
machine replication) koji toleriše **vizantijske (Byzantine) greške**: do
`f` od ukupno `N = 3f+1` replika može da se ponaša proizvoljno (padne,
laže, dogovara se sa drugima, šalje kontradiktorne poruke), a sistem i
dalje garantuje:

| Svojstvo | Opis | 
|---|---|
| **Bezbednost** (safety, linearizabilnost) | Sve ispravne replike izvršavaju isti niz klijentskih zahteva, čak i kada je primary neispravan. |
| **Živost** (liveness) | Klijentski zahtevi se na kraju uvek obrade, pod uslovom da kašnjenja poruka na kraju postanu ograničena i da je manje od N/3 replika neispravno. |

## Algoritam (normalan tok rada, Sekcija 4.3 rada)

1. **Klijent** šalje `REQUEST` replikama.
2. **Primary** dodeljuje redni broj `n` i multicast-uje `PRE-PREPARE(v,n,d)`.
3. **Backup** replike prihvataju poruku i multicast-uju `PREPARE(v,n,d,i)`.
4. Čim replika prikupi `PRE-PREPARE` + `2f` odgovarajućih `PREPARE` poruka
   (takozvani **prepared certifikat**, ukupno 2f+1 glasova), multicast-uje
   `COMMIT(v,n,i)`.
5. Čim replika prikupi `2f+1` odgovarajućih `COMMIT` poruka (**committed
   certifikat**), izvršava zahtev (redosledom rednih brojeva) i šalje
   odgovor klijentu.
6. Klijent prihvata rezultat čim dobije `f+1` podudarnih odgovora.

Ukoliko je primary spor, srušen ili se ponaša vizantijski, backup replike
istekom tajmera pokreću **protokol promene view-a** (Sekcija 4.5): šalju
`VIEW-CHANGE`, i čim sledeći primary prikupi `2f+1` takvih poruka,
multicast-uje `NEW-VIEW` i nastavlja sa dodeljivanjem zahteva.

## Pojednostavljenja

Izričito dozvoljena specifikacijom projekta ("manja pojednostavljenja su
poželjna/dozvoljena ukoliko ne menjaju osnovnu logiku algoritma"):

- Nema čuvanja stanja (checkpoints) / sabiranja otpadaka (garbage
  collection) / ograničenja log-a (Sekcija 4.4).
- Nema prenosa stanja (state transfer) ni proaktivnog oporavka (proactive
  recovery) (Sekcije 5, 6.2).
- Promena view-a nosi samo zahtev na koji se čekalo, umesto kompletnih P/Q
  sertifikata; `NEW-VIEW` se prihvata čim ga podrži 2f+1 različitih
  pošiljalaca `VIEW-CHANGE` poruka (bez `VIEW-CHANGE-ACK`).
- Digest-i su skraćeni SHA-256 otisci, umesto pune MAC autentikacije
  poruka — integritet i odsustvo duplikata poruka obezbeđuje ponovo
  iskorišćeni sloj `link.PerfectLink`.
- Svaka replika (ne samo backup-ovi) odmah upisuje sopstveni `PREPARE`
  glas, a primary dodatno šalje i eksplicitnu `PREPARE` poruku — ovo
  održava evidenciju glasova simetričnom, uz i dalje puni zahtev za
  kvorumom od 2f+1.

Nijedno od ovoga ne menja osnovni argument bezbednosti zasnovan na preseku
kvoruma.

---

## Struktura projekta

```
projekat_pbft/
├── main.go                     # Pokreće sva četiri demonstraciona scenarija
├── go.mod                      # Modul (zavisi od week03_04_parallel)
├── pbft/
│   ├── types.go                 # Tipovi poruka, ClientRequest, PBFTMessage
│   ├── quorum.go                 # f, 2f+1, f+1, izbor primary-ja
│   ├── util.go                    # except() pomoćna funkcija
│   ├── broadcast.go                # Broadcast + EquivocatingBroadcast
│   ├── recorder.go                  # Thread-safe evidencija za provere
│   ├── checkers.go                   # Saglasnost / bez dvostrukog izvršenja / napredak view-a / kvorum klijenta
│   ├── replica.go                     # ReplicaNode: pre-prepare/prepare/commit/izvršenje
│   ├── viewchange.go                   # Tajmer + promena view-a + new-view
│   ├── byzantine.go                     # Vizantijski (equivocating) primary
│   └── client.go                         # ClientNode
└── examples/
    ├── normal_case.go            # 4 replike, bez grešaka
    ├── primary_crash.go           # Pad lidera -> promena view-a
    ├── byzantine_primary.go        # Zlonamerni lider -> promena view-a
    └── omission_backup.go           # Nepouzdana/spora replika, bez potrebe za promenom view-a
```


## Šta je iskorišćeno iz week03_04_parallel

| Komponenta | Namena | 
|---|---|
| `runtime.Runtime` | Okvir za izvršavanje (gorutine, ruter, monitor neaktivnosti) |
| `process.Process` | Interfejs koji implementiraju `ReplicaNode` / `ClientNode` |
| `link.FairLossLink` / `StubbornLink` / `PerfectLink` | Transport, retransmisija, deduplikacija |
| `failures.CrashFailure` | Srušen / nedostupan lider (Primer 2) |
| `failures.NoFailure` | Osnovni scenario (Primeri 1, 3) |
| `failures.OmissionFailure` | Spor/nepouzdan backup (Primer 4) |
| `runtime.Trace` | Beleženje događaja (`WithVerbose(true)`) |

`failures.ByzantineFailure` namerno **nije** iskorišćen za scenario sa
zlonamernim primary-jem: equivokacija zahteva slanje dve zaista različite
poruke različitim primaocima, što odlučuje sama PBFT logika
(`pbft.ReplicaNode.Equivocate`), a ne naknadno izmenjivanje poruke.

---

## Primeri

1. **Normalan rad** (`RunNormalCase`) — 4 replike, bez grešaka. Pun tok
   pre-prepare/prepare/commit, sve replike izvršavaju zahtev, klijent
   dobija f+1 odgovora.
2. **Pad primary-ja** (`RunPrimaryCrash`) — primary se ruši pre nego što
   uspe da dodeli zahtev; backup replike istekom tajmera biraju novog
   primary-ja putem promene view-a, zahtev se završava u view-u 1.
3. **Zlonamerni primary** (`RunByzantinePrimary`) — primary equivocira
   (šalje sukobljene `PRE-PREPARE` poruke); nijedan digest ne dostiže
   kvorum 2f+1, replike istekom tajmera sprovode promenu view-a koja bira
   ispravnog primary-ja, koji uspešno ponovo predlaže zahtev.
4. **Spor backup / gubitak poruka** (`RunOmissionBackup`) — 7 replika
   (f=2), jednoj replici se gubi 90% poruka; sistem i dalje izvrši zahtev
   preko preostalog kvoruma 2f+1, bez ikakve promene view-a.

Svaki primer se na kraju završava blokom **PBFT Property Checks**: 
saglasnost ukupnog redosleda (bezbednost), odsustvo dvostrukog izvršenja,
napredak view-a (živost protokola promene view-a) i kvorum klijenta.
