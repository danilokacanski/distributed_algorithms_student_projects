// Package examples contains self-contained runnable PBFT scenarios.
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
// EXAMPLE 1: Normal Case — no faults
// ============================================================================
//
// 4 replicas (N=4, f=1). No crashes, no Byzantine behavior, no message
// loss. A single client sends one request; it commits through the full
// pre-prepare/prepare/commit pipeline in view 0, every replica executes
// it, and the client collects f+1=2 matching replies.
func RunNormalCase() {
	fmt.Println("================================================================")
	fmt.Println("  EXAMPLE 1: NORMAL CASE — 4 replicas (f=1), no faults")
	fmt.Println("  Reference: Castro & Liskov, Section 4.3 (Fig. 1)")
	fmt.Println("================================================================")

	replicaIDs := []process.ProcessID{"R0", "R1", "R2", "R3"}
	clientID := process.ProcessID("client-1")

	recorder := pbft.NewRecorder()

	fl := link.NewFairLossLink(0.0, 1)
	sl := link.NewStubbornLink(fl)
	pl := link.NewPerfectLink(sl)
	fm := failures.NewNoFailure()

	rt := simrt.NewRuntime(pl, fm,
		simrt.WithMaxDuration(10*time.Second),
		simrt.WithIdleTimeout(700*time.Millisecond),
		simrt.WithRetransmitInterval(50*time.Millisecond),
		simrt.WithVerbose(false),
	)

	for _, id := range replicaIDs {
		rt.Register(pbft.NewReplicaNode(id, replicaIDs, recorder, 800*time.Millisecond))
	}

	req := pbft.ClientRequest{ClientID: clientID, Op: "SET x=1", Timestamp: 1}
	rt.Register(pbft.NewClientNode(clientID, replicaIDs, req, recorder))

	rt.Run()

	snap := recorder.Snapshot()
	pbft.RunAllChecks(snap, replicaIDs, len(replicaIDs), clientID)
}
