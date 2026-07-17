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
// EXAMPLE 4: Slow/Unresponsive Backup — message omission
// ============================================================================
//
// 7 replicas (N=7, f=2), so the system can tolerate losing an ENTIRE
// replica's messages. R6's incoming messages are dropped with high
// probability (simulating a slow, flaky, or partially unresponsive
// backup). Because a 2f+1=5-out-of-7 quorum does not need R6 at all, the
// request still commits normally, with NO view change required.
//
// Demonstrates message delay/loss fault simulation, and that PBFT
// tolerates up to f faulty (here: unresponsive) replicas out of 3f+1
// without sacrificing safety or liveness.
func RunOmissionBackup() {
	fmt.Println("================================================================")
	fmt.Println("  EXAMPLE 4: SLOW BACKUP — message omission (7 replicas, f=2)")
	fmt.Println("  R6's messages are dropped 90% of the time; no view change needed.")
	fmt.Println("  Reference: Castro & Liskov, Section 2 (fair-loss / omission)")
	fmt.Println("================================================================")

	replicaIDs := []process.ProcessID{"R0", "R1", "R2", "R3", "R4", "R5", "R6"}
	clientID := process.ProcessID("client-1")

	recorder := pbft.NewRecorder()

	fl := link.NewFairLossLink(0.0, 4)
	sl := link.NewStubbornLink(fl)
	pl := link.NewPerfectLink(sl)

	fm := failures.NewOmissionFailure(4)
	fm.SetOmissionRate("R6", 0.9) // messages addressed TO R6 are dropped 90% of the time
	//fm.SetOmissionRate("R6", 0.995) //proba

	rt := simrt.NewRuntime(pl, fm,
		simrt.WithMaxDuration(15*time.Second),
		//simrt.WithMaxDuration(1500*time.Second), //proba
		simrt.WithIdleTimeout(1200*time.Millisecond),
		//simrt.WithIdleTimeout(600*time.Millisecond), //proba
		simrt.WithRetransmitInterval(50*time.Millisecond),
		simrt.WithVerbose(false),
	)

	for _, id := range replicaIDs {
		rt.Register(pbft.NewReplicaNode(id, replicaIDs, recorder, 800*time.Millisecond))
	}

	req := pbft.ClientRequest{ClientID: clientID, Op: "SET x=4", Timestamp: 1}
	rt.Register(pbft.NewClientNode(clientID, replicaIDs, req, recorder))

	rt.Run()

	snap := recorder.Snapshot()
	pbft.RunAllChecks(snap, replicaIDs, len(replicaIDs), clientID)
}
