package pbft

import (
	"fmt"

	"github.com/danilokacanski/da/week03_04_parallel/process"
)

//sendEquivocatingPrePrepare implementira bizantijskog primarya koji urusava
// PRE-PREPARE dogovor: deli druge replike u dve grupe i salje svakoj grupi
// PRE-PREPARE za isti (view, seq) ali sa drugacijim request/digest
// 4.3 iz rada

func (rs *runState) sendEquivocatingPrePrepare(seq int, req ClientRequest) {
	fake := req
	fake.Op = req.Op + "_FORGED"

	digestA := req.Digest()
	digestB := fake.Digest()

	//korektnost sistema ne zavisi od toga sta bizantijski primary cuva lokalno
	entry := newLogEntry(rs.view, seq, digestA)
	entry.request = req
	entry.prePrepared = true
	rs.logs[seq] = entry

	backups := except(rs.node.allIDs, rs.node.id)
	groupA, groupB := splitHalves(backups)

	fmt.Printf("  [%s] *** BYZANTINE PRIMARY: equivocating on seq=%d — %v get digest=%s, %v get digest=%s ***\n",
		rs.node.id, seq, groupA, digestA, groupB, digestB)

	EquivocatingBroadcast(rs.send, rs.node.id,
		groupA, PBFTMessage{Kind: KindPrePrepare, View: rs.view, Seq: seq, Digest: digestA, Request: req, From: rs.node.id},
		groupB, PBFTMessage{Kind: KindPrePrepare, View: rs.view, Seq: seq, Digest: digestB, Request: fake, From: rs.node.id},
	)
	//Bizantijski primary ne salje eksplicitan PREPARE ovde uopste - za razliku od
	// iskrenog primary-a (koji dalje uskracuje oba digesta od glasova i koci ih)
}

// splitHalves deli id-eve u dve odvojene, relativno jednake polovine
func splitHalves(ids []process.ProcessID) ([]process.ProcessID, []process.ProcessID) {
	mid := len(ids) / 2
	a := make([]process.ProcessID, mid)
	copy(a, ids[:mid])
	b := make([]process.ProcessID, len(ids)-mid)
	copy(b, ids[mid:])
	return a, b
}
