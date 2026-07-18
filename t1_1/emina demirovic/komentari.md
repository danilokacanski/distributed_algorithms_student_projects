# Emina — Distributed Experiment Platform (C# / Raft)

---

## +:

- Odradjen Raft: izbori, log replikacija, commit, state machine i SQLite baza podataka
- Odradjen safety property pri commit-u
- Cuvaju se u memoriji stanja pri padu
- Idempotentnost komandi, proverava se AppliedCommands tabela
- 14 test klasa odradjeno

---

## -:

- nema leader forwardinga, follower ce vratiti NotLeader, ali klijent mora SAM da ponovo posalje zahtev na liderovu adresu, umesto toga moze HTTP redirect ili interno prosledjivanje zahteva lidera

PREDLOG OCENE: 10
