# Ognjen Perić — HotStuff BFT simulator (Go)

---

## +:

- kriptografija kako treba
- pravi threshold signature workflow
- poruke iz budućih view-ova se kesiraju, pa pri promeni view-a, se procesiraju sve buffered poruke za novi view
- bankarska state machin-a sa 4 tipa transkacija
- relna simulacija mreznih uslova
- cvor koji prima QC za nepoznat blok salje fetch zahtev pooiljaocu.
- jako dobar dijagram za vizuelizaciju

---

## -:

- 'certificates', 'NewViews', 'Votes', 'FormedQC' mape rastu neograniceno

- 'PendingCommands' se nikad se ne cisti, treba ukloniti komandu iz slajsa odmah po 'executeLocked'

PREDLOG OCENE: 10