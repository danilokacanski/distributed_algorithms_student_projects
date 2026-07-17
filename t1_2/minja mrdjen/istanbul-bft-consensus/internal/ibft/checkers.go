package ibft

import "fmt"

func ConsensusAchieved(nodes map[int]*Node, validators []int) bool {
	decidedValue := ""
	correctNodes := 0

	for _, id := range validators {
		node := nodes[id]

		if node.ByzantineMode != ByzantineNone {
			continue
		}

		correctNodes++

		if !node.Decided {
			return false
		}

		if node.DecisionValue == "" {
			return false
		}

		if decidedValue == "" {
			decidedValue = node.DecisionValue
			continue
		}

		if node.DecisionValue != decidedValue {
			return false
		}
	}

	return correctNodes > 0
}

func CheckConsensus(nodes map[int]*Node, validators []int) {
	fmt.Println("\nProvera konsenzusa:")

	correctNodes := 0
	decidedCorrectNodes := 0
	decidedValue := ""
	safetyOk := true

	for _, id := range validators {
		node := nodes[id]

		if node.ByzantineMode != ByzantineNone {
			continue
		}

		correctNodes++

		if !node.Decided {
			continue
		}

		decidedCorrectNodes++

		if node.DecisionValue == "" {
			safetyOk = false
			fmt.Printf("SAFETY: greska - Node %d je oznacen kao decided, ali nema DecisionValue\n", node.ID)
			continue
		}

		if decidedValue == "" {
			decidedValue = node.DecisionValue
			continue
		}

		if node.DecisionValue != decidedValue {
			safetyOk = false
			fmt.Printf("SAFETY: greska - Node %d je odlucio %s, a prethodna odlucena vrednost je %s\n",
				node.ID, node.DecisionValue, decidedValue)
		}
	}

	fmt.Printf("Ispravnih cvorova: %d\n", correctNodes)
	fmt.Printf("Ispravnih cvorova koji su odlucili: %d\n", decidedCorrectNodes)

	if correctNodes == 0 {
		fmt.Println("REZULTAT: Nema ispravnih cvorova za proveru konsenzusa.")
		return
	}

	if decidedCorrectNodes == correctNodes && safetyOk && decidedValue != "" {
		fmt.Printf("REZULTAT: Konsenzus je postignut. Odlucena vrednost je: %s\n", decidedValue)
	} else {
		fmt.Println("REZULTAT: Konsenzus nije postignut.")
	}

	if !safetyOk {
		fmt.Println("SAFETY: NIJE OK - postoje ispravni cvorovi koji su odlucili razlicite vrednosti.")
		return
	}

	if decidedCorrectNodes == 0 {
		fmt.Println("SAFETY: OK - nijedan ispravan cvor jos nije doneo odluku.")
		return
	}

	fmt.Println("SAFETY: OK - nijedna dva ispravna cvora nisu odlucila razlicite vrednosti.")
}

func CorrectNodesInRound(nodes map[int]*Node, validators []int, round int) bool {
	correctNodesInRound := 0

	for _, id := range validators {
		node := nodes[id]

		if node.ByzantineMode != ByzantineNone {
			continue
		}

		if node.CurrentRound == round {
			correctNodesInRound++
		}
	}

	return correctNodesInRound >= QuorumSize(len(validators))
}
