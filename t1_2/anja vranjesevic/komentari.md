# Anja Vranješević — HotStuff BFT (Python, asyncio)

## +:
- odradjen i basic i chained hotstuff
-koristi Ed25519, onako kako bi trebalo
- safeNode odadjen onako kako bi trebalo
- uradjen i catch up mehanizam
- dobro faktorisan kod
- 8 testnih scenarija koji proveravaju sve potrebno
- odlicna vizuelizacija u web aplikaciji

---

## -:

- 'on_new_view' prihvata 'm.sender' bez kriptografske provere, jako lako izmenjivo, znala je da tako treba na odbrain

- O(n) kompleksnost u QC  (O(1))

PREDLOG OCENE: 10