# Tijana — PBFT simulator (Go)

---

## +:

- goroutine, lock-free arhitektura
- obuhvata sve korake pbfta tacno
- detekcija duplog glasanja i automatska provera 4 svojstva
- nadogradjuju se abstrakcije sa vezbi
---

## -:

- timeout se povecava linearno umesto eksponencijalno

- `handleViewChange` prihvata `PendingReq` bez verifikacije

PREDLOG OCENE: 10