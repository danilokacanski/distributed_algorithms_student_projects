# Jednostavna NBFT simulacija

Ovo je implementacija algoritma iz rada **Improved Fault-Tolerant Consensus Based on the PBFT Algorithm**.

Svaki `Node` ima sopstvenu nit i red poruka. Kod zadrzava consistent-hash grupisanje, sest NBFT faza, node-decision broadcast i threshold vote-counting, ali simulira potpise samo skupom ID-jeva cvorova.

## Kompajliranje

```powershell
g++ -std=c++17 -O2 -Wall -Wextra -pedantic -pthread src/main.cpp src/Node.cpp src/Network.cpp -o consensus_simple.exe
```

## Pokretanje

```powershell
.\consensus_simple.exe normal 17 0 4
.\consensus_simple.exe byzantine_wrong_value 13 4 4
.\consensus_simple.exe faulty_rep_low 16 1 4
.\consensus_simple.exe primary_silent 16 1 4
```

Parametri su: scenario, broj ispravnih cvorova, broj Byzantine cvorova i velicina grupe.
