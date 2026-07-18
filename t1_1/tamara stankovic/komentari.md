# Tamara — Tendermint BFT simulator

---

## +:

- Svaki 'upon' rule je tabelarno mapiran u dokumentaciji na metodu sa brojem linije iz rada
- Sve je thread safe
- 10 scenarija i auto-verifikacija osobina za svaki
- Clean OOP pristup

---

## -:

- 'drop_prob' se ne iskljucuje posle GST, laka izena da radi, znala je u teoriji da to treba tako ipak tako
- weighted round-robin se simulira od nule svaki put umesto da je inkremetalan ili nekako drugacije

- '_rule_round_skip' bira najvecu rundu, a treba najmanja takodje znala da to tako treba u teoriji

PREDLOG OCENE: 10