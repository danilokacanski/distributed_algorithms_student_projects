# Isidora Micković — PBFT simulator (Go)

---

## +:

- najpotpuniji view-change odradjen tacno po knjizi
- 'decisionProcedure' implementira oba uslova (A i B)
- uradjen i view-change-ack protokol
- 'tryVerifyNewView': svaka replika nezavisno pokrece 'decisionProcedure' i verifikuje da se new-view poklapa, tako da se odbija new-view koji nije konzistentan sa decision procedurom
- 'FetchViewChange' mehanizam
- za duplikat klijentskog zahteva koji je vec izvrsen, salje cached reply bez ponovnog izvršavanja 

---

## -:


- timeout nije eksponencijalan, ali je znala na odbrani da to treba

- 'tryBuildNewView' koristi '2*n.F-1' umesto ;'2*n.F' ack-ova mada je imala objasnjenje za to sa kojim nisam uspeo da se slozim do kraja, tako da je ovo svesno odradjeno

- 'decisionProcedure' uvek vraća 'h = 0' pa samim tim 'LowWaterMark' nikad nije ažuriran, kaze da nisu znale ni ona ni koleginica koje je radila istu temu kako da odrade to tako da je svesno preskoceno

PREDLOG OCENE: 10