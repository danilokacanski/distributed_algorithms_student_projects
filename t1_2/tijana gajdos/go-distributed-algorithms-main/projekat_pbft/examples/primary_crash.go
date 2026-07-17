package examples

import (
	"fmt"
	"time"

	"github.com/danilokacanski/da/week03_04_parallel/failures"
	"github.com/danilokacanski/da/week03_04_parallel/link"
	"github.com/danilokacanski/da/week03_04_parallel/process"
	simrt "github.com/danilokacanski/da/week03_04_parallel/runtime"
	"github.com/danilokacanski/da/week07_08_pbft/pbft"
)

// ============================================================================
// EXAMPLE 2: Primary Crash — view change required
// ============================================================================
//
// 4 replicas (N=4, f=1). R0 is the primary of view 0. R0 crashes shortly
// after the client's request is broadcast, before it can send
// PRE-PREPARE. R1, R2, R3 time out waiting for progress, run the
// view-change protocol, and R1 (primary of view 1) takes over and
// re-proposes the request, which then commits normally.
//
// Demonstrates: fault simulation (crashed/unresponsive leader) and the
// view-change liveness property (Section 4.5).
func RunPrimaryCrash() {
	fmt.Println("================================================================")
	fmt.Println("  EXAMPLE 2: PRIMARY CRASH — view change (4 replicas, f=1)")
	fmt.Println("  R0 (primary of view 0) crashes before ordering the request.")
	fmt.Println("  Reference: Castro & Liskov, Section 4.5")
	fmt.Println("================================================================")

	replicaIDs := []process.ProcessID{"R0", "R1", "R2", "R3"}
	clientID := process.ProcessID("client-1")

	recorder := pbft.NewRecorder()

	fl := link.NewFairLossLink(0.0, 2)
	sl := link.NewStubbornLink(fl)
	pl := link.NewPerfectLink(sl)
	fm := failures.NewCrashFailure()

	rt := simrt.NewRuntime(pl, fm,
		simrt.WithMaxDuration(15*time.Second),
		simrt.WithIdleTimeout(1200*time.Millisecond),
		//simrt.WithIdleTimeout(10*time.Millisecond),
		simrt.WithRetransmitInterval(50*time.Millisecond),
		simrt.WithVerbose(false),
	)

	for _, id := range replicaIDs {
		rt.Register(pbft.NewReplicaNode(id, replicaIDs, recorder, 500*time.Millisecond))
	}

	req := pbft.ClientRequest{ClientID: clientID, Op: "SET x=2", Timestamp: 1}
	rt.Register(pbft.NewClientNode(clientID, replicaIDs, req, recorder))

	// Crash the primary of view 0 (R0) shortly after the client's request
	// has been broadcast, before it can send PRE-PREPARE. Replicas have no
	// failure-detector oracle in PBFT (an accurate failure detector cannot
	// be assumed under Byzantine faults) — they only discover this
	// indirectly, via their view-change timeout.
	//go func() {
	//time.Sleep(80 * time.Millisecond)
	//time.Sleep(800 * time.Millisecond)
	//fmt.Println("  >>> Crashing primary R0 <<<")
	//rt.CrashProcess("R0")
	//}()
	fmt.Println("  >>> Crashing primary R0 before it can order anything <<<")
	rt.CrashProcess("R0")

	rt.Run()

	snap := recorder.Snapshot()
	pbft.RunAllChecks(snap, replicaIDs, len(replicaIDs), clientID)
}
