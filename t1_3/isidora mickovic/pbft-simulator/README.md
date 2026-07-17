# PBFT Simulator

Simulator algoritma Practical Byzantine Fault Tolerance (PBFT), rađen po radu
Castro & Liskov, "Practical Byzantine Fault Tolerance and Proactive Recovery" (2002).

## Zahtevi

- Go 1.21+ (testirano na Go 1.26)

## Pokretanje

```bash
go run . -scenario=<ime_scenarija>
```

Dostupni scenariji:

| Scenario       | Opis                                                                 |
|----------------|-----------------------------------------------------------------------|
| `normal`       | Normalan rad, 4 čvora, f=1, bez grešaka                              |
| `viewchange`   | Primarni pada nasred runde - prenos P/Q skupa kroz view change       |
| `nullfallback` | Primarni pada pre nego što iko postane prepared - decision procedura bira NULL |
| `byzantine`    | Primarni šalje različite (iskrivljene) poruke različitim replikama (equivocation) |
| `exceedsf`     | Pada 2 od 4 čvora (> f=1) - sistem ne postiže konsenzus (n ≥ 3f+1)    |

Podrazumevani scenario (bez `-scenario` flaga) je `normal`.

## Struktura projekta

- `message/` - definicije tipova poruka i njihovih struktura
- `node/`    - implementacija replike (PRE-PREPARE/PREPARE/COMMIT, view change)
- `client/`  - klijent koji šalje zahteve i čeka f+1 poklapajućih odgovora
- `network/` - povezuje čvorove kroz Go kanale, upravlja pokretanjem/gašenjem
- `scenarios.go` - definicije demo scenarija
- `main.go`  - ulazna tačka, bira scenario preko `-scenario` flaga

## Napomene

Detaljan opis algoritma, arhitekture i svesnih pojednostavljenja nalazi se
u seminarskom radu.