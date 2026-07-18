# Aleksandra Ilijoski — IBFT simulator (Go)

---

## +:

- kriptografija odradjena kako treba
- ROUND-CHANGE precizno indeksirane
- f+1 fast-forward pravilo odradjeno
- 7 testnih scenarija odradjeno
- odradjen prelazak na sledeci lambda blok sa resetom runde, sto omogucava visebloknu simulacij.

---

## ✗ Greške


- 'PrepareVotes' i 'CommitVotes' su indeksirani po vrednosti a ne i po rundi

- 'Broadcast' salje i na vlastiti 'MsgChan'

PREDLOG OCENE: 10