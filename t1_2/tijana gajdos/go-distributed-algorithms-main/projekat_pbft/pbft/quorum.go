package pbft

import "github.com/danilokacanski/da/week03_04_parallel/process"

// MaxFaulty vraca f, maksimalan broj bizantijskih replika koje sistem
// tolerise od n replika prema otpornosti zahtev N=3f+1 (sekcija 4.1 :
// otpornost BFT-a je optimalna: najmanje 3f+1 replika je obavezno)

func MaxFaulty(n int) int {
	return (n - 1) / 3
}

// QuorumSize vraca velicinu 'jakog' kvorum sertifikata: 2f+1 istih poruka
// od razlicitih replika. Bilo koja dva jaka kvoruma velicine 2f+1
// od 3f+1 replika se sece u barem jednoj korektnoj replici (sekcija 4.1: presek svojstvo)

func QuorumSize(n int) int {
	f := MaxFaulty(n)
	return 2*f + 1
}

//WeakQuorumSize vraca velicinu 'slabog' sertifikata: f+1 poruka
// dokazujuci da barem jedna ispravna replika salje poruku. Koristi
// klijent da prihvati odgovor (sekcija 4.2) i novi primary da prihvati
// dovoljno VIEW-CHANGE glasova da nastavi dalje (sekcija 4.5.1)

func WeakQuorumSize(n int) int {
	f := MaxFaulty(n)
	return f + 1
}

//primaryIndex implementira pravilo iz rada p=v mod /R/ (sekcija 4.1)
func primaryIndex(v, n int) int {
	return v % n
}

//PrimaryFor vraca ID primary replike za view v, kojem je dat
// STABLE (uvek se provlaci istim redom) lista ID-eva od svih replika
func PrimaryFor(v int, allIDs []process.ProcessID) process.ProcessID {
	return allIDs[primaryIndex(v, len(allIDs))]
}
