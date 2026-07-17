package main

import (
	"fmt"

	"github.com/minjamrdjen/ibft-consensus-go/internal/ibft"
)

func main() {
	fmt.Println("IBFT simulacija sa rundama i vizantijskim ponasanjem")

	var totalNodes int
	var byzantineCount int

	fmt.Print("Unesi ukupan broj cvorova: ")
	_, err := fmt.Scan(&totalNodes)
	if err != nil {
		fmt.Println("Greska: moras uneti ceo broj.")
		return
	}

	fmt.Print("Unesi broj vizantijskih cvorova: ")
	_, err = fmt.Scan(&byzantineCount)
	if err != nil {
		fmt.Println("Greska: moras uneti ceo broj.")
		return
	}

	if totalNodes <= 0 {
		fmt.Println("Broj cvorova mora biti veci od 0.")
		return
	}

	if byzantineCount < 0 {
		fmt.Println("Broj vizantijskih cvorova ne moze biti negativan.")
		return
	}

	if byzantineCount > totalNodes {
		fmt.Println("Broj vizantijskih cvorova ne moze biti veci od ukupnog broja cvorova.")
		return
	}

	validators := make([]int, totalNodes)
	for i := 0; i < totalNodes; i++ {
		validators[i] = i
	}

	byzantineNodes := make(map[int]bool)
	byzantineMode := ibft.ByzantineNone

	if byzantineCount > 0 {
		var modeChoice int

		fmt.Println("Izaberi ponasanje vizantijskih cvorova:")
		fmt.Println("1 - SILENT: cvor ignorise sve poruke")
		fmt.Println("2 - EQUIVOCATE: vizantijski lider salje razlicite vrednosti razlicitim cvorovima")
		fmt.Println("3 - BAD_VOTE: vizantijski validator salje glas za pogresnu vrednost")
		fmt.Print("Unos: ")

		_, err = fmt.Scan(&modeChoice)
		if err != nil {
			fmt.Println("Greska: moras uneti ceo broj.")
			return
		}

		switch modeChoice {
		case 1:
			byzantineMode = ibft.ByzantineSilent
		case 2:
			byzantineMode = ibft.ByzantineEquivocate
		case 3:
			byzantineMode = ibft.ByzantineBadVote
		default:
			fmt.Println("Nepoznat izbor. Koristi se SILENT rezim.")
			byzantineMode = ibft.ByzantineSilent
		}

		fmt.Printf("Unesi ID-jeve vizantijskih cvorova, od 0 do %d:\n", totalNodes-1)

		for len(byzantineNodes) < byzantineCount {
			var id int

			_, err := fmt.Scan(&id)
			if err != nil {
				fmt.Println("Greska: moras uneti ceo broj.")
				return
			}

			if id < 0 || id >= totalNodes {
				fmt.Printf("Nevalidan ID cvora: %d. ID mora biti od 0 do %d.\n", id, totalNodes-1)
				continue
			}

			if byzantineNodes[id] {
				fmt.Printf("Node %d je vec oznacen kao vizantijski. Unesi drugi ID.\n", id)
				continue
			}

			byzantineNodes[id] = true
		}
	}

	var timeoutMode int

	fmt.Println("Izaberi nacin simulacije timeout-a:")
	fmt.Println("1 - timeout istice svim ispravnim cvorovima")
	fmt.Println("2 - timeout istice samo za f+1 ispravnih cvorova")
	fmt.Print("Unos: ")

	_, err = fmt.Scan(&timeoutMode)
	if err != nil {
		fmt.Println("Greska: moras uneti ceo broj.")
		return
	}

	if timeoutMode != 1 && timeoutMode != 2 {
		fmt.Println("Nepoznat izbor. Koristi se rezim 1.")
		timeoutMode = 1
	}

	var catchUpMode int
	var delayedNodeID int

	fmt.Println("Da li zelis da simuliras Qcommit catch-up scenario?")
	fmt.Println("1 - Ne")
	fmt.Println("2 - Da, jedan ispravan cvor privremeno kasni")
	fmt.Print("Unos: ")

	_, err = fmt.Scan(&catchUpMode)
	if err != nil {
		fmt.Println("Greska: moras uneti ceo broj.")
		return
	}

	if catchUpMode != 1 && catchUpMode != 2 {
		fmt.Println("Nepoznat izbor. Koristi se rezim 1.")
		catchUpMode = 1
	}

	if catchUpMode == 2 {
		fmt.Printf("Unesi ID ispravnog cvora koji ce kasniti, od 0 do %d:\n", totalNodes-1)

		for {
			_, err = fmt.Scan(&delayedNodeID)
			if err != nil {
				fmt.Println("Greska: moras uneti ceo broj.")
				return
			}

			if delayedNodeID < 0 || delayedNodeID >= totalNodes {
				fmt.Printf("Nevalidan ID cvora: %d. ID mora biti od 0 do %d.\n", delayedNodeID, totalNodes-1)
				continue
			}

			if byzantineNodes[delayedNodeID] {
				fmt.Println("Za catch-up scenario izaberi ispravan cvor, ne vizantijski.")
				continue
			}

			break
		}
	}

	maxFaulty := ibft.MaxFaultyNodes(totalNodes)
	quorum := ibft.QuorumSize(totalNodes)

	if byzantineCount > maxFaulty {
		fmt.Printf("\nUPOZORENJE: Za %d cvorova IBFT moze da tolerise najvise %d vizantijskih cvorova.\n",
			totalNodes, maxFaulty)
		fmt.Println("Ova konfiguracija prelazi granicu tolerancije, pa se ocekuje da konsenzus ne uspe.")
		fmt.Println("Uslov za toleranciju je: n >= 3f + 1")
	}

	lambda := 1
	value := "block-1"

	network := ibft.NewNetwork()

	for _, id := range validators {
		node := ibft.NewNode(id, validators)

		if byzantineNodes[id] {
			node.Byzantine = true
			node.ByzantineMode = byzantineMode
		}

		network.AddNode(node)
	}
	for _, id := range validators {
		go network.Nodes[id].Run(network)
	}

	if catchUpMode == 2 {
		network.SetNodeDelayed(delayedNodeID, true)
	}

	maxRounds := totalNodes

	fmt.Println()
	fmt.Printf("Lambda / instanca konsenzusa: %d\n", lambda)
	fmt.Printf("Ukupan broj cvorova: %d\n", totalNodes)
	fmt.Printf("Broj vizantijskih cvorova: %d\n", byzantineCount)
	fmt.Printf("Maksimalno tolerisanih vizantijskih cvorova: %d\n", maxFaulty)
	fmt.Printf("Quorum size: %d\n", quorum)

	if byzantineCount > 0 {
		fmt.Printf("Vizantijski rezim: %s\n", byzantineMode)
		fmt.Print("Vizantijski cvorovi: ")

		for _, id := range validators {
			if byzantineNodes[id] {
				fmt.Printf("Node %d ", id)
			}
		}

		fmt.Println()
	} else {
		fmt.Println("Vizantijski cvorovi: nema")
	}

	if catchUpMode == 2 {
		fmt.Printf("Qcommit catch-up scenario: Node %d privremeno kasni\n", delayedNodeID)
	}

	fmt.Println()

	round := 1

	fmt.Printf("\n========== START INSTANCE lambda=%d ==========\n", lambda)

	for _, id := range validators {
		network.Nodes[id].Start(lambda, value, network)
	}

	network.Run()

	if catchUpMode == 2 {
		fmt.Printf("\nQcommit catch-up scenario: Node %d je propustio pocetne poruke.\n", delayedNodeID)
		network.SetNodeDelayed(delayedNodeID, false)
		fmt.Printf("Node %d ce kasnije poslati ROUND-CHANGE, a odluceni cvorovi treba da mu posalju Qcommit.\n",
			delayedNodeID)
	}

	for !ibft.ConsensusAchieved(network.Nodes, validators) && round < maxRounds {
		nextRound := round + 1

		fmt.Printf("\nKonsenzus nije postignut u round %d. Simulira se istek timeout-a.\n",
			round)

		simulateTimeouts(network, validators, timeoutMode)

		network.Run()

		if ibft.ConsensusAchieved(network.Nodes, validators) {
			fmt.Printf("\nKonsenzus je postignut u round %d.\n", nextRound)
			break
		}

		if !ibft.CorrectNodesInRound(network.Nodes, validators, nextRound) {
			fmt.Println("\nROUND-CHANGE nije uspeo: nema dovoljno ispravnih cvorova za prelazak u sledecu rundu.")
			break
		}

		round = nextRound
	}

	if !ibft.ConsensusAchieved(network.Nodes, validators) && round >= maxRounds {
		fmt.Println("\nDostignut je maksimalan broj rundi. Konsenzus nije postignut.")
	}

	fmt.Println("\nKrajnje stanje cvorova:")

	for _, id := range validators {
		node := network.Nodes[id]

		acceptedValueText := node.AcceptedValue
		if acceptedValueText == "" {
			acceptedValueText = "nema"
		}

		acceptedRoundText := "nema"
		if node.AcceptedRound >= 0 {
			acceptedRoundText = fmt.Sprintf("%d", node.AcceptedRound)
		}

		preparedValueText := node.PreparedValue
		if preparedValueText == ibft.NoPreparedValue {
			preparedValueText = "nema"
		}

		preparedRoundText := "nema"
		if node.PreparedRound != ibft.NoPreparedRound {
			preparedRoundText = fmt.Sprintf("%d", node.PreparedRound)
		}

		fmt.Printf("Node %d | byzantine=%v | mode=%s | lambda=%d | currentRound=%d | state=%s | decided=%v | acceptedValue=%s | acceptedRound=%s | preparedValue=%s | preparedRound=%s | qcommit=%d | timer=%s\n",
			node.ID,
			node.Byzantine,
			node.ByzantineMode,
			node.Lambda,
			node.CurrentRound,
			node.State,
			node.Decided,
			acceptedValueText,
			acceptedRoundText,
			preparedValueText,
			preparedRoundText,
			len(node.DecisionCertificate),
			node.TimerState,
		)
	}

	ibft.CheckConsensus(network.Nodes, validators)
}

func simulateTimeouts(network *ibft.Network, validators []int, timeoutMode int) {
	nodesToTimeout := len(validators)

	if timeoutMode == 2 {
		nodesToTimeout = ibft.RoundChangeSetSize(len(validators))
		fmt.Printf("Timeout se simulira samo za f+1=%d ispravnih cvorova.\n", nodesToTimeout)
	} else {
		fmt.Println("Timeout se simulira za sve ispravne cvorove.")
	}

	triggered := 0

	for _, id := range validators {
		node := network.Nodes[id]

		if node.ByzantineMode != ibft.ByzantineNone {
			continue
		}

		if node.Decided {
			continue
		}

		node.OnTimeout(network)
		triggered++

		if timeoutMode == 2 && triggered >= nodesToTimeout {
			break
		}
	}

	fmt.Printf("Timeout je aktiviran na %d ispravnih cvorova.\n", triggered)
}
