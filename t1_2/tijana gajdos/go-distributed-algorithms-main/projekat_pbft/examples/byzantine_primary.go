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
// EXAMPLE 3: Byzantine Primary — equivocation
// ============================================================================
//
// 4 replicas (N=4, f=1). R0 is the primary of view 0 and is configured as
// Byzantine: it sends two DIFFERENT PRE-PREPARE messages for the client's
// request (same view/seq, different digests) to two disjoint halves of
// the backups. Neither digest can gather the 2f+1=3 matching PREPARE
// votes needed to commit (the 3 backups split 1-2), so the request stalls
// until the backups time out, trigger a view change, and R1 (the new,
// honest primary) re-proposes the SAME request cleanly.
//
// Demonstrates Byzantine fault simulation and PBFT's core safety
// guarantee: a faulty primary can delay progress (liveness), but can
// NEVER cause two different replicas to execute two different requests at
// the same sequence number (safety) — Section 4.3, "prepared certificate".
func RunByzantinePrimary() {
	fmt.Println("================================================================")
	fmt.Println("  EXAMPLE 3: BYZANTINE PRIMARY — equivocation (4 replicas, f=1)")
	fmt.Println("  R0 sends conflicting PRE-PREPAREs; view change recovers.")
	fmt.Println("  Reference: Castro & Liskov, Section 4.3 (prepared certificate)")
	fmt.Println("================================================================")

	replicaIDs := []process.ProcessID{"R0", "R1", "R2", "R3"}
	clientID := process.ProcessID("client-1")

	recorder := pbft.NewRecorder()

	fl := link.NewFairLossLink(0.0, 3)
	sl := link.NewStubbornLink(fl)
	pl := link.NewPerfectLink(sl)
	fm := failures.NewNoFailure() // R0 stays "alive" — it is Byzantine, not crashed

	rt := simrt.NewRuntime(pl, fm,
		simrt.WithMaxDuration(15*time.Second),
		simrt.WithIdleTimeout(1200*time.Millisecond),
		simrt.WithRetransmitInterval(50*time.Millisecond),
		simrt.WithVerbose(false),
	)

	for _, id := range replicaIDs {
		node := pbft.NewReplicaNode(id, replicaIDs, recorder, 500*time.Millisecond)
		if id == "R0" {
			node.Equivocate = true
		}
		rt.Register(node)
	}

	req := pbft.ClientRequest{ClientID: clientID, Op: "SET x=3", Timestamp: 1}
	rt.Register(pbft.NewClientNode(clientID, replicaIDs, req, recorder))

	rt.Run()

	snap := recorder.Snapshot()
	pbft.RunAllChecks(snap, replicaIDs, len(replicaIDs), clientID)
}
