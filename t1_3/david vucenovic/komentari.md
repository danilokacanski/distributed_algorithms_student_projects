# David Vučenović — NBFT simulator (Python)

---

## +: 

- odradjene sve faze kako treba sa view-change-om
- 'asyncio event loop' i 'awaitable inbox' pa nema blokirajucih thread-ova
- odradjeni i Model 1 (node decision) i Model 2 (threshold voting) 
- 'VoteLedger' tacno implementira threshold vote-counting
- 'RoundState' per-(view, seq) odradjen tako da postoji pravilna izolacija između instanci.

---

## -:

- potpisi su simulirani
- echo-quorum ne uzima u obzir NBFT 2-tier fault model, treba koristiti prag '(w+1) * localQuorum' 

PREDLOG OCENE: 10