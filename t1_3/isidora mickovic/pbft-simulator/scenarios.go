package main

import (
	"fmt"
	"time"

	"pbft-simulator/client"
	"pbft-simulator/network"
	"pbft-simulator/node"
)

func banner(title, desc string) {
	fmt.Println("========================================")
	fmt.Println(title)
	fmt.Println(desc)
	fmt.Println("========================================")
	fmt.Println()
}

func scenarioNormal() {
	banner("SCENARIO: Normalan rad (bez grešaka)",
		"4 čvora, f=1, klijent šalje jedan zahtev. Svi čvorovi ga izvršavaju bez view change-a.")

	f := 1
	n := 3*f + 1
	net := network.NewNetwork(n, f, map[int]node.FaultType{})
	net.Start()

	c := client.NewClient(100, net.Channels, f)
	net.RegisterClient(c.ID, c.Inbox)
	c.SendRequest("SET x=5", 3*time.Second, 3)

	time.Sleep(1 * time.Second)
	net.Stop()
}

func scenarioPrimaryCrashViewChange() {
	banner("SCENARIO: Pad primarnog nasred runde - prenos P/Q skupa kroz view change",
		"Primarni (Node 0) namerno propušta Node 3 pri slanju PRE-PREPARE, pa pada.\n"+
			"Node 1 i Node 2 stignu da postanu PREPARED (imaju P zapis) pre nego što COMMIT kvorum (treba 3) stigne da se formira.\n"+
			"View change treba da PRENESE taj isti zahtev (ne 'null') u novi view - decision procedura (Figure 4, uslov A1/A2)\n"+
			"pronalazi dovoljno P/Q dokaza da je zahtev bio na putu da se prihvati.")

	f := 1
	n := 3*f + 1
	net := network.NewNetwork(n, f, map[int]node.FaultType{})
	net.Start()

	net.Nodes[0].SkipPeers[3] = true

	c := client.NewClient(100, net.Channels, f)
	net.RegisterClient(c.ID, c.Inbox)

	go c.SendRequest("SET x=5", 4*time.Second, 3)

	time.Sleep(150 * time.Millisecond)
	fmt.Println("[scenario] gasim primarnog (Node 0) - Node 1 i Node 2 su verovatno već PREPARED")
	net.Nodes[0].Stop()

	time.Sleep(6 * time.Second)
	net.Stop()
}

func scenarioNullFallback() {
	banner("SCENARIO: Pad primarnog PRE nego što ijedan backup postane PREPARED - NULL fallback",
		"Primarni (Node 0) stiže samo do Node-a 1 (propušta 2 i 3), pa pada.\n"+
			"Node 1 ima Q zapis (pre-prepared) ali NIKAD ne skupi dovoljno PREPARE glasova za P zapis.\n"+
			"Decision procedura (uslov B) treba da izabere NULL - dokaz da protokol ne 'izmišlja' zahtev\n"+
			"koji nikad nije stvarno bio prepared kod dovoljno replika.")

	f := 1
	n := 3*f + 1
	net := network.NewNetwork(n, f, map[int]node.FaultType{})
	net.Start()

	net.Nodes[0].SkipPeers[2] = true
	net.Nodes[0].SkipPeers[3] = true

	c := client.NewClient(100, net.Channels, f)
	net.RegisterClient(c.ID, c.Inbox)

	go c.SendRequest("SET x=5", 4*time.Second, 3)

	time.Sleep(150 * time.Millisecond)
	fmt.Println("[scenario] gasim primarnog (Node 0) - samo Node 1 je video PRE-PREPARE")
	net.Nodes[0].Stop()

	time.Sleep(6 * time.Second)
	net.Stop()
}

func scenarioByzantinePrimary() {
	banner("SCENARIO: Vizantijski primarni (equivocation)",
		"Primarni (Node 0) šalje različitim replikama različite (iskrivljene) verzije poruka.\n"+
			"Nijedna replika ne dostiže kvorum za isti digest -> tajmeri ističu -> view change,\n"+
			"sve dok pošten primarni ne preuzme i uspešno završi rundu.")

	f := 1
	n := 3*f + 1
	faults := map[int]node.FaultType{0: node.ByzantineFault}
	net := network.NewNetwork(n, f, faults)
	net.Start()

	c := client.NewClient(100, net.Channels, f)
	net.RegisterClient(c.ID, c.Inbox)
	c.SendRequest("SET x=5", 3*time.Second, 5)

	time.Sleep(2 * time.Second)
	net.Stop()
}

func scenarioExceedsF() {
	banner("SCENARIO: Prekoračen broj tolerisanih grešaka (f+1 pada)",
		"f=1, n=4, gasimo 2 čvora (uključujući primarnog) - to je VIŠE od f=1.\n"+
			"Očekivano: sistem NE uspeva da postigne konsenzus, klijent nikad ne dobija odgovor\n"+
			"ni posle retransmisija. Demonstrira granicu bezbednosne garancije (n >= 3f+1).")

	f := 1
	n := 3*f + 1
	net := network.NewNetwork(n, f, map[int]node.FaultType{})
	net.Start()

	fmt.Println("[scenario] gasim Node 0 (primarni) i Node 1 - 2 od 4 čvora, prekoračuje f=1")
	net.Nodes[0].Stop()
	net.Nodes[1].Stop()

	c := client.NewClient(100, net.Channels, f)
	net.RegisterClient(c.ID, c.Inbox)
	c.SendRequest("SET x=5", 3*time.Second, 3)

	time.Sleep(2 * time.Second)
	net.Stop()
}