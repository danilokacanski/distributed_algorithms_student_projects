package pbft

import (
	"sync"

	"github.com/danilokacanski/da/week03_04_parallel/process"
)

//ExecutionEntry snima da je replika izvrsila zahtev na datom
//sekvencnom broju, zajedno sa digestom koji je commitovala

type ExecutionEntry struct {
	Seq    int
	Digest string
	Op     Operation
}

// Recorer je thread-safe skup deljen izmedju svih ReplicaNodes,
// ClientNodes i primera. Sakuplja sve sto property checker-ima treba
// nakon sto se simulacija zavrsi: sta je svaka replika izvrsila,
// u kojem view je svaka replika zavrsila i koje odgovore je klijent sakupio
type Recorder struct {
	mu sync.Mutex

	executions  map[process.ProcessID][]ExecutionEntry
	finalView   map[process.ProcessID]int
	viewChanges map[process.ProcessID]int

	clientReplies map[process.ProcessID][]PBFTMessage
}

func NewRecorder() *Recorder {
	return &Recorder{
		executions:    make(map[process.ProcessID][]ExecutionEntry),
		finalView:     make(map[process.ProcessID]int),
		viewChanges:   make(map[process.ProcessID]int),
		clientReplies: make(map[process.ProcessID][]PBFTMessage),
	}
}

func (r *Recorder) RecordExecution(replica process.ProcessID, e ExecutionEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executions[replica] = append(r.executions[replica], e)
}

func (r *Recorder) RecordView(replica process.ProcessID, view int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finalView[replica] = view
}

func (r *Recorder) RecordViewChangeInitiated(replica process.ProcessID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.viewChanges[replica]++
}

func (r *Recorder) RecordClientReply(client process.ProcessID, reply PBFTMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clientReplies[client] = append(r.clientReplies[client], reply)
}

// Snapshot je nepromenljiv prikaaz stanja recorder-a koji koriste checkers
type Snapshot struct {
	Executions    map[process.ProcessID][]ExecutionEntry
	FinalView     map[process.ProcessID]int
	ViewChanges   map[process.ProcessID]int
	ClientReplies map[process.ProcessID][]PBFTMessage
}

func (r *Recorder) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	ex := make(map[process.ProcessID][]ExecutionEntry, len(r.executions))
	for k, v := range r.executions {
		cp := make([]ExecutionEntry, len(v))
		copy(cp, v)
		ex[k] = cp
	}
	fv := make(map[process.ProcessID]int, len(r.finalView))
	for k, v := range r.finalView {
		fv[k] = v
	}
	vc := make(map[process.ProcessID]int, len(r.viewChanges))
	for k, v := range r.viewChanges {
		vc[k] = v
	}
	cr := make(map[process.ProcessID][]PBFTMessage, len(r.clientReplies))
	for k, v := range r.clientReplies {
		cp := make([]PBFTMessage, len(v))
		copy(cp, v)
		cr[k] = cp
	}
	return Snapshot{Executions: ex, FinalView: fv, ViewChanges: vc, ClientReplies: cr}
}
