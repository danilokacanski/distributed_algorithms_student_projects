/*package main

import (
	"fmt"
	"time"

	"pbft-simulator/client"
	"pbft-simulator/network"
	"pbft-simulator/node"
)

func main() {
	f := 2               
	totalNodes := 3*f + 1 

	faults := map[int]node.FaultType{
		//3: node.ByzantineFault,
		//2: node.ByzantineFault,
		//1: node.ByzantineFault, 
		//3: node.CrashFault,
		//0: node.SlowLeaderFault,
		0: node.ByzantineFault,
		2: node.DelayFault,
	}

	fmt.Printf("Pokrećem PBFT simulaciju: %d čvorova, f=%d, kvorum=%d\n\n", totalNodes, f, 2*f+1)

	net := network.NewNetwork(totalNodes, f, faults)
	net.Start()

	// ako neki čvor ima CrashFault, gasimo ga odmah (simulira da je pao pre početka runde)
	for id, ft := range faults {
		if ft == node.CrashFault {
			fmt.Printf("[Node %d] simulira pad - gašenje čvora\n", id)
			net.Nodes[id].Stop()
		}
	}

	c := client.NewClient(100, net.Channels, f)
	net.RegisterClient(c.ID, c.Inbox)
	c.SendRequest("SET x=5", 3*time.Second, 3)

	// dajemo vremena da se poruke izmenjaju kroz mrežu
	time.Sleep(12 * time.Second)

	fmt.Println("\nSimulacija završena.")
	net.Stop()
}*/

/*package main

import (
	"fmt"
	"time"

	"pbft-simulator/client"
	"pbft-simulator/network"
	"pbft-simulator/node"
)

func main() {
	f := 2
	totalNodes := 3*f + 1

	// ---- KONFIGURACIJA SCENARIJA (menjaj ovde za odbranu) ----
	faults := map[int]node.FaultType{
		0: node.CrashFault,
		// prazno - crash simuliramo ručno ispod, nasred protokola, ne od početka
	}
	// ------------------------------------------------------------

	fmt.Printf("Pokrećem PBFT simulaciju: %d čvorova, f=%d, kvorum=%d\n\n", totalNodes, f, 2*f+1)

	net := network.NewNetwork(totalNodes, f, faults)
	net.Start()

	for id, ft := range faults {
		if ft == node.CrashFault {
			fmt.Printf("[Node %d] simulira pad - gašenje čvora\n", id)
			net.Nodes[id].Stop()
		}
	}

	c := client.NewClient(100, net.Channels, f)
	net.RegisterClient(c.ID, c.Inbox)

	// simuliramo iznenadni pad 2 čvora NASRED protokola (posle PRE-PREPARE/PREPARE faze,
	// pre nego što COMMIT kvorum stigne da se formira) - da testiramo da li P skup
	// ispravno beleži prepared sertifikate pre view change-a
	go func() {
		time.Sleep(300 * time.Millisecond) // dovoljno da view change 0->1 završi i Node 1 stigne da pošalje PRE-PREPARE
		fmt.Println("[main] simuliram pad NOVOG primarnog (Node 1) nasred njegove runde")
		net.Nodes[1].Stop()
	}()

	c.SendRequest("SET x=5", 3*time.Second, 3)

	time.Sleep(12 * time.Second)

	fmt.Println("\nSimulacija završena.")
	net.Stop()
}*/

/*package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"pbft-simulator/client"
	"pbft-simulator/network"
	"pbft-simulator/node"
)

func main() {
	f := 2
	totalNodes := 3*f + 1

	faults := map[int]node.FaultType{
		// dodaj ovde po potrebi, npr. 0: node.ByzantineFault,
	}

	fmt.Printf("Pokrećem PBFT simulaciju: %d čvorova, f=%d, kvorum=%d\n\n", totalNodes, f, 2*f+1)

	net := network.NewNetwork(totalNodes, f, faults)
	net.Start()

	for id, ft := range faults {
		if ft == node.CrashFault {
			fmt.Printf("[Node %d] simulira pad - gašenje čvora\n", id)
			net.Nodes[id].Stop()
		}
	}

	c := client.NewClient(100, net.Channels, f)
	net.RegisterClient(c.ID, c.Inbox)

	fmt.Println("\nDostupne komande:")
	fmt.Println("  request <tekst>   - klijent šalje novi zahtev (npr: request SET x=5)")
	fmt.Println("  kill <id>         - gasi čvor sa datim ID-jem (simulira pad)")
	fmt.Println("  exit              - završava simulaciju")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		cmd := parts[0]

		switch cmd {
		case "exit", "quit":
			fmt.Println("Gašenje simulacije...")
			net.Stop()
			return

		case "kill":
			if len(parts) < 2 {
				fmt.Println("Upotreba: kill <id>")
				continue
			}
			id, err := strconv.Atoi(parts[1])
			if err != nil || id < 0 || id >= totalNodes {
				fmt.Printf("Nevažeći ID čvora: %s\n", parts[1])
				continue
			}
			fmt.Printf(">>> Gasim Node %d\n", id)
			net.Nodes[id].Stop()

		case "request":
			if len(parts) < 2 {
				fmt.Println("Upotreba: request <tekst>")
				continue
			}
			payload := parts[1]
			go c.SendRequest(payload, 3*time.Second, 3)

		default:
			fmt.Printf("Nepoznata komanda: %s\n", cmd)
		}
	}
}*/

package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	scenario := flag.String("scenario", "normal", "scenario: normal, viewchange, nullfallback, byzantine, exceedsf")
	flag.Parse()

	switch *scenario {
	case "normal":
		scenarioNormal()
	case "viewchange":
		scenarioPrimaryCrashViewChange()
	case "nullfallback":
		scenarioNullFallback()
	case "byzantine":
		scenarioByzantinePrimary()
	case "exceedsf":
		scenarioExceedsF()
	default:
		fmt.Printf("Nepoznat scenario: %s\n", *scenario)
		fmt.Println("Dostupni: normal, viewchange, nullfallback, byzantine, exceedsf")
		os.Exit(1)
	}
}