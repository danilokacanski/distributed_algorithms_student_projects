// Practical Byzantine Fault Tolerance (PBFT) Simulator

// Implementira trofazni protokol za normalan slucaj (pre-prepare/prepare/
// commit) i pojednostavljen view-change protokol iz:

//	Miguel Castro, Barbara Liskov — "Practical Byzantine Fault Tolerance
//	and Proactive Recovery", ACM TOCS, Vol. 20, No. 4, November 2002.

// Pokretanje:

// go run .
package main

import (
	"fmt"

	"github.com/danilokacanski/da/week07_08_pbft/examples"
)

func main() {
	fmt.Println("================================================================")
	fmt.Println("  Practical Byzantine Fault Tolerance (PBFT)")
	fmt.Println("  Based on: Castro & Liskov, ACM TOCS 20(4), 2002")
	fmt.Println("  Runtime: goroutines + channels (week03_04_parallel)")
	fmt.Println("================================================================")
	fmt.Println()

	examples.RunNormalCase()
	fmt.Println()

	examples.RunPrimaryCrash()
	fmt.Println()

	examples.RunByzantinePrimary()
	fmt.Println()

	examples.RunOmissionBackup()
	fmt.Println()

	fmt.Println("================================================================")
	fmt.Println("  All examples complete. Review traces and checks above.")
	fmt.Println("================================================================")
}
