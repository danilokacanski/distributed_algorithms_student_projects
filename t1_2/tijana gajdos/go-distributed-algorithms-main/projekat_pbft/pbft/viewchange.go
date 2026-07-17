package pbft

import (
	"fmt"

	"github.com/danilokacanski/da/week03_04_parallel/process"
)

// ============================================================================
// VIEW CHANGE (pojednostavljena verzija sekcije 4.5)
// ============================================================================

//Pojednostavljenja:
// - Novi primary jedino ponovo ponavlja jedan zahtev na koji on
// 	 (ili druga replika) i dalje cekaju, umesto celog log prozora
//   [h, h+L]
// - NEW-VIEW se veruje kada nosi >= 2f+1 razlicitih VIEW-CHANGE
//	 posiljalaca u ViewChangeSet
// - Nema high/low water marks: sekvencni brojevi se jednostavno
//   povecavaju kroz views

// Bitni safety/liveness argumenti su ocuvani: zahtev se moze
// commitovati samo sa 2f+1 kvorumom, a novom primary-u treba 2f+1
// kvorum VIEW-CHANGE glasova pre nego sto moze da radi u novom view-u

// onTimeout se aktivira kada replika dovoljno dugo ceka na zahtev
// da napravi napredak. Sumnja na trenutnog primary-a i pocinje
// view change na view+1 (sekcija 4.5.1 liveness)
func (rs *runState) onTimeout() {
	if rs.pendingRequest == nil {
		return //idle timeout - nista se ne ceka, nista se ne radi
	}

	newView := rs.view + 1
	rs.active = false
	rs.node.recorder.RecordViewChangeInitiated(rs.node.id)

	fmt.Printf("  [%s] *** TIMEOUT in view=%d — suspecting primary %s, initiating VIEW-CHANGE to view=%d ***\n",
		rs.node.id, rs.view, rs.primary(), newView)

	rs.castViewChangeVote(newView, rs.node.id)
	Broadcast(rs.send, rs.node.id, rs.node.allIDs, PBFTMessage{
		Kind: KindViewChange, NewView: newView, From: rs.node.id,
		LastExecuted: rs.lastExecuted, PendingReq: rs.pendingRequest,
	})

	//escalating timeout: ako i ovaj view koci (npr novi primary je isto pao)
	// ponovo cemo sumnjati na njega sa duzim timeoutom - pojednostavljena
	// verzija exponential-backoff liveness argumenta iz sekcije 4.5.1

	rs.resetTimer(2 * rs.node.ViewChangeTimeout)
}

// handleViewChange procesuira VIEW-CHANGE glas od druge replike
func (rs *runState) handleViewChange(cm PBFTMessage) {
	nv := cm.NewView
	rs.castViewChangeVote(nv, cm.From)

	if cm.PendingReq != nil && rs.pendingRequest == nil {
		rs.pendingRequest = cm.PendingReq
	}

	votes := len(rs.viewChangeVotes[nv])

	//Replika koja vidi f+1 VIEW-CHANGE glasova za view veci od svog
	// skace unapred i prikljucuje im se umesto da ceka sopstveni timer
	// (sekcija 4.5.1)
	if nv > rs.view && votes >= WeakQuorumSize(rs.node.n) && !rs.viewChangeVotes[nv][rs.node.id] {
		rs.active = false
		rs.castViewChangeVote(nv, rs.node.id)
		Broadcast(rs.send, rs.node.id, rs.node.allIDs, PBFTMessage{
			Kind: KindViewChange, NewView: nv, From: rs.node.id,
			LastExecuted: rs.lastExecuted, PendingReq: rs.pendingRequest,
		})
		votes = len(rs.viewChangeVotes[nv])
	}

	//Ako cemo biti primary od nv i sad imamo 2f+1 kvorum VIEW-CHANGE
	// glasova, zavrsi view change i posalji NEW-VIEW
	if PrimaryFor(nv, rs.node.allIDs) == rs.node.id && votes >= QuorumSize(rs.node.n) && rs.view < nv {
		rs.completeViewChange(nv)
	}
}

func (rs *runState) castViewChangeVote(newView int, from process.ProcessID) {
	if rs.viewChangeVotes[newView] == nil {
		rs.viewChangeVotes[newView] = make(map[process.ProcessID]bool)
	}
	rs.viewChangeVotes[newView][from] = true
}

//completeViewChange cini ovu repliku aktivnim primary-em view-a nv,
// broadcast-uje NEW-VIEW i predlozi neki pending zahtev kao sledeci
// sekvencni broj (sekcija 4.5, 'New-View Message Construction').

func (rs *runState) completeViewChange(nv int) {
	rs.view = nv
	rs.active = true
	rs.nextSeq = rs.lastExecuted + 1
	rs.node.recorder.RecordView(rs.node.id, rs.view)

	proofs := sortedProcessIDs(rs.viewChangeVotes[nv])
	fmt.Printf("  [%s] *** Collected 2f+1 VIEW-CHANGE votes for view=%d — becoming primary, sending NEW-VIEW ***\n",
		rs.node.id, nv)

	Broadcast(rs.send, rs.node.id, rs.node.allIDs, PBFTMessage{
		Kind: KindNewView, NewView: nv, From: rs.node.id, ViewChangeSet: proofs,
	})

	if rs.pendingRequest != nil {
		req := *rs.pendingRequest
		seq := rs.nextSeq
		rs.nextSeq++
		digest := req.Digest()

		entry := newLogEntry(rs.view, seq, digest)
		entry.request = req
		entry.prePrepared = true
		entry.prepares[rs.node.id] = true
		rs.logs[seq] = entry

		fmt.Printf("  [%s] Re-proposing pending request %s as seq=%d in new view=%d\n", rs.node.id, req, seq, rs.view)

		backups := except(rs.node.allIDs, rs.node.id)
		Broadcast(rs.send, rs.node.id, backups, PBFTMessage{
			Kind: KindPrePrepare, View: rs.view, Seq: seq, Digest: digest, Request: req, From: rs.node.id,
		})
		Broadcast(rs.send, rs.node.id, backups, PBFTMessage{
			Kind: KindPrepare, View: rs.view, Seq: seq, Digest: digest, From: rs.node.id,
		})
	}

	rs.resetTimer(rs.node.ViewChangeTimeout)
}

// handleNewView aktivira repliku u novom view-u onda kada vidi NEW-VIEW
// poruku garantovanu od 2f+1 VIEW-CHANGE seta dokaza (zamena u odnosu na
// rad umesto new-view provere sertifikata, sekcija 4.5 'New-View Message Processing')
func (rs *runState) handleNewView(cm PBFTMessage) {
	if cm.From != PrimaryFor(cm.NewView, rs.node.allIDs) {
		return //NEW-VIEW mora doci od novog primary-a
	}
	if len(cm.ViewChangeSet) < QuorumSize(rs.node.n) {
		return // nedovoljno dokaza - odbaci
	}
	if cm.NewView < rs.view || (cm.NewView == rs.view && rs.active) {
		return
	}

	rs.view = cm.NewView
	rs.active = true
	rs.nextSeq = rs.lastExecuted + 1
	rs.node.recorder.RecordView(rs.node.id, rs.view)

	fmt.Printf("  [%s] Accepted NEW-VIEW for view=%d (new primary=%s)\n", rs.node.id, rs.view, cm.From)
	rs.resetTimer(rs.node.ViewChangeTimeout)
}

// sortedProcessIDs vraca deterministicki sortiran isecak glasova
func sortedProcessIDs(set map[process.ProcessID]bool) []process.ProcessID {
	out := make([]process.ProcessID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
