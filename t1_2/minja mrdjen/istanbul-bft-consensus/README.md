# IBFT Consensus Simulation in Go

Ovaj projekat predstavlja edukativnu simulaciju Istanbul Byzantine Fault Tolerance (IBFT) konsenzus algoritma u programskom jeziku Go.

Implementacija prikazuje:
- normalan tok IBFT algoritma kroz `PRE-PREPARE`, `PREPARE` i `COMMIT` faze,
- promenu runde pomoću `ROUND-CHANGE` mehanizma,
- ponašanje sistema u prisustvu vizantijskih čvorova,
- `f + 1 ROUND-CHANGE` pravilo,
- `Qcommit` dokaz odluke i catch-up mehanizam za čvor koji kasni.

## Struktura projekta

```text
ibft-consensus-go/
│
├── cmd/
│   └── ibft-sim/
│       └── main.go
│
└── internal/
    └── ibft/
        ├── node.go
        ├── network.go
        ├── types.go
        ├── leader.go
        ├── quorum.go
        ├── justification.go
        ├── timeout.go
        ├── validation.go
        └── checkers.go
