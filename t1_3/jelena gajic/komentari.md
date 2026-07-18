# Jelena Gajić — NBFT simulator (C++)

---

## +:

- visenitna C++ arhitektura,nema busy-waita
- detljano objasnjenja hash ring topologija
- 4 vizantijska moda i 4 testna scenaija
- 'reset()' potpuno reinicijalizuje stanje, pa se scenariji se mogu ponavljati bez restartovanja programa.
- 'validGroupSignatures' verifikuje da svaki potpisnik u setu stvarno pripada deklarisanoj grupi, sto sprecava lazne sertifikate
- 'nodeDecision' mehanizam, gde cvor koji detektuje neispravan in_prepare2 eskalira ka predstavnicima ostalih grupa bez cekanja svog predstavnika
- jako dobro teorijsko znanje, konstatno referenciranje na rad tokom izvodjenja odbrane 
---

## -:

- sistem nije ziv pri tihom primary-ju, svesno odradjeno i radjeno je u cpp 

- hardkodovana kriptografija

PREDLOG OCENE: 10