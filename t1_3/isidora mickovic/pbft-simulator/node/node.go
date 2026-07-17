package node

import (
	"fmt"
	"math/rand"
	"pbft-simulator/message"
	"sync"
	"time"
)

type FaultType int

const (
	NoFault FaultType = iota
	CrashFault
	ByzantineFault
	DelayFault
	SlowLeaderFault
)

type Node struct {
	ID         int
	TotalNodes int
	F          int
	View       int
	Fault      FaultType

	Inbox chan message.Message
	Peers []chan message.Message

	ClientChannels map[int]chan message.Message

	assignedRequests map[string]bool

	mu           sync.Mutex
	prepareVotes map[int]map[int]map[string]map[int]bool // sequence -> view -> digest -> senderID -> bool
	commitVotes  map[int]map[int]map[string]map[int]bool
	commitSent   map[int]map[int]bool
	committed    map[int]bool
	stopCh       chan struct{}

	prePrepareReceived map[int]string
	preparedSent       map[int]bool

	requestLog         map[string]message.Message
	pendingPrePrepares map[string]message.Message
	pendingRequests    map[string]bool
	timerActive        bool
	timerStop          chan struct{}
	seqCounter         int

	activeView bool
	//setP          map[int]PQEntry              // P: sequence -> {digest, view} - zahtevi prepared u prethodnim view-ovima
	//setQ          map[int]PQEntry              // Q: sequence -> {digest, view} - zahtevi pre-prepared u prethodnim view-ovima
	viewChangeLog      map[int]map[int]message.Message
	viewChangeAckCount map[int]map[int]map[int]bool
	sentViewChangeAck  map[int]map[int]bool
	newViewSent        map[int]bool

	preparedView    map[int]int
	prePreparedView map[int]int

	pendingNewView map[int]message.Message

	enteredPrePrepared map[int]int
	SkipPeers          map[int]bool

	executedDigests map[string]message.Message
}

func NewNode(id, totalNodes, f int, fault FaultType) *Node {
	return &Node{
		ID:           id,
		TotalNodes:   totalNodes,
		F:            f,
		View:         0,
		Fault:        fault,
		Inbox:        make(chan message.Message, 100),
		prepareVotes: make(map[int]map[int]map[string]map[int]bool),
		commitVotes:  make(map[int]map[int]map[string]map[int]bool),
		commitSent:   make(map[int]map[int]bool),
		committed:    make(map[int]bool),
		stopCh:       make(chan struct{}),

		prePrepareReceived: make(map[int]string),
		preparedSent:       make(map[int]bool),

		requestLog:         make(map[string]message.Message),
		pendingPrePrepares: make(map[string]message.Message),
		pendingRequests:    make(map[string]bool),

		activeView:         true,
		preparedView:       make(map[int]int),
		prePreparedView:    make(map[int]int),
		viewChangeLog:      make(map[int]map[int]message.Message),
		viewChangeAckCount: make(map[int]map[int]map[int]bool),
		newViewSent:        make(map[int]bool),

		ClientChannels:     make(map[int]chan message.Message),
		assignedRequests:   make(map[string]bool),
		pendingNewView:     make(map[int]message.Message),
		enteredPrePrepared: make(map[int]int),
		SkipPeers:          make(map[int]bool),
		executedDigests:    make(map[string]message.Message),
	}
}

func (n *Node) IsPrimary() bool {
	return n.ID == n.View%n.TotalNodes
}

func (n *Node) Run() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		select {
		case msg := <-n.Inbox:
			n.handleMessage(msg)
			time.Sleep(150 * time.Millisecond)
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) RegisterClientChannel(clientID int, ch chan message.Message) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.ClientChannels[clientID] = ch
}

func (n *Node) Broadcast(msg message.Message) {
	for peerID, peerCh := range n.Peers {
		if peerID == n.ID {
			continue
		}
		if n.SkipPeers[peerID] {
			continue
		}

		outMsg := msg

		switch n.Fault {
		case ByzantineFault:
			if rand.Intn(2) == 0 {
				outMsg.Payload = fmt.Sprintf("CORRUPTED-%d", rand.Intn(1000))
				outMsg.Digest = outMsg.Payload
			}
			n.deliver(peerID, peerCh, outMsg)

		case DelayFault:
			go func(id int, ch chan message.Message, m message.Message) {
				time.Sleep(4 * time.Second)
				n.deliver(id, ch, m)
			}(peerID, peerCh, outMsg)

		case SlowLeaderFault:
			if n.IsPrimary() {
				go func(id int, ch chan message.Message, m message.Message) {
					time.Sleep(6 * time.Second)
					n.deliver(id, ch, m)
				}(peerID, peerCh, outMsg)
				continue
			}
			n.deliver(peerID, peerCh, outMsg)

		default:
			n.deliver(peerID, peerCh, outMsg)
		}
	}
}

func (n *Node) deliver(peerID int, peerCh chan message.Message, msg message.Message) {
	select {
	case peerCh <- msg:
	default:
		fmt.Printf("[Node %d] upozorenje: kanal ka Node %d je pun, poruka odbačena\n", n.ID, peerID)
	}
}

func (n *Node) onRequest(msg message.Message) {
	fmt.Printf("[Node %d] primio REQUEST od klijenta %d: %q (timestamp %d)\n", n.ID, msg.SenderID, msg.Payload, msg.Timestamp)

	n.mu.Lock()
	reply, alreadyExecuted := n.executedDigests[msg.Digest]
	n.mu.Unlock()

	if alreadyExecuted {
		n.mu.Lock()
		clientCh, ok := n.ClientChannels[msg.SenderID]
		n.mu.Unlock()
		if ok {
			fmt.Printf("[Node %d] zahtev %q je već izvršen - ponovo šalje REPLY klijentu %d (rad, Sekcija 4.2)\n", n.ID, msg.Digest, msg.SenderID)
			select {
			case clientCh <- reply:
			default:
			}
		}
		return
	}

	n.mu.Lock()
	n.requestLog[msg.Digest] = msg
	active := n.activeView
	alreadyAssigned := n.assignedRequests[msg.Digest]
	n.mu.Unlock()

	if !active {
		fmt.Printf("[Node %d] view change u toku, zahtev čeka na NEW-VIEW\n", n.ID)
		return
	}

	if n.IsPrimary() {
		if alreadyAssigned {
			return // retransmisija zahteva koji već ima dodeljenu sekvencu
		}
		n.mu.Lock()
		n.seqCounter++
		seq := n.seqCounter
		n.assignedRequests[msg.Digest] = true
		n.mu.Unlock()
		n.startConsensusFor(msg, seq)
		return
	}

	n.mu.Lock()
	pending, hasPending := n.pendingPrePrepares[msg.Digest]
	if hasPending {
		delete(n.pendingPrePrepares, msg.Digest)
	}
	n.mu.Unlock()

	n.startTimerIfNeeded(msg.Digest)

	if hasPending {
		n.enterPreparePhase(pending.Sequence, pending.Digest, pending.View)
	}
}

func (n *Node) SendTo(peerID int, msg message.Message) {
	if peerID < 0 || peerID >= len(n.Peers) {
		return
	}
	select {
	case n.Peers[peerID] <- msg:
	default:
		fmt.Printf("[Node %d] upozorenje: kanal ka Node %d je pun, poruka odbačena\n", n.ID, peerID)
	}
}

func (n *Node) startTimerIfNeeded(digest string) {
	if n.IsPrimary() {
		return
	}

	n.mu.Lock()
	if n.timerActive {
		n.mu.Unlock()
		return
	}
	n.timerActive = true
	n.timerStop = make(chan struct{})
	localStop := n.timerStop
	n.mu.Unlock()

	go func() {
		timeout := 3 * time.Second
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			n.mu.Lock()
			n.timerActive = false
			n.mu.Unlock()
			fmt.Printf("[Node %d] TAJMER ISTEKAO za zahtev %q - primarni ne odgovara na vreme, pokrećem VIEW CHANGE\n", n.ID, digest)
			n.startViewChange()
		case <-localStop:
			return
		case <-n.stopCh:
			return
		}
	}()
}

func (n *Node) stopTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.timerActive {
		close(n.timerStop)
		n.timerActive = false
	}
}

func (n *Node) startViewChange() {
	n.mu.Lock()
	newView := n.View + 1

	var pSet, qSet []message.PQEntry
	for seq, prepared := range n.preparedSent {
		if prepared {
			pSet = append(pSet, message.PQEntry{Sequence: seq, Digest: n.prePrepareReceived[seq], View: n.preparedView[seq]})
		}
	}
	for seq, view := range n.enteredPrePrepared {
		qSet = append(qSet, message.PQEntry{Sequence: seq, Digest: n.prePrepareReceived[seq], View: view})
	}

	n.View = newView
	n.activeView = false
	n.mu.Unlock()

	n.stopTimer()

	vcMsg := message.Message{
		Type:         message.ViewChange,
		View:         newView,
		SenderID:     n.ID,
		LowWaterMark: 0,
		PSet:         pSet,
		QSet:         qSet,
	}

	fmt.Printf("[Node %d] šalje VIEW-CHANGE za view %d\n  P=%v\n  Q=%v\n", n.ID, newView, pSet, qSet)

	n.Broadcast(vcMsg)
	n.onViewChange(vcMsg)

	go func() {
		timeout := 4 * time.Second
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			n.mu.Lock()
			stillWaiting := n.View == newView && !n.activeView
			n.mu.Unlock()
			if stillWaiting {
				fmt.Printf("[Node %d] NEW-VIEW nije stigao na vreme za view %d, prelazim u sledeći view\n", n.ID, newView)
				n.startViewChange()
			}
		case <-n.stopCh:
			return
		}
	}()
}

func (n *Node) correctViewChange(msg message.Message) bool {
	for _, p := range msg.PSet {
		if !(p.View < msg.View) {
			return false
		}
	}
	for _, q := range msg.QSet {
		if !(q.View < msg.View) {
			return false
		}
	}
	return true
}

func viewChangeDigest(msg message.Message) string {
	return fmt.Sprintf("vc-%d-%d-%v-%v", msg.View, msg.SenderID, msg.PSet, msg.QSet)
}

func (n *Node) onViewChange(msg message.Message) {
	if msg.SenderID != n.ID {
		fmt.Printf("[Node %d] primio VIEW-CHANGE od Node %d za view %d\n", n.ID, msg.SenderID, msg.View)
	}

	n.mu.Lock()
	if msg.View < n.View {
		n.mu.Unlock()
		fmt.Printf("[Node %d] ODBACUJE VIEW-CHANGE - view %d je stariji od mog %d\n", n.ID, msg.View, n.View)
		return
	}
	if !n.correctViewChange(msg) {
		n.mu.Unlock()
		fmt.Printf("[Node %d] ODBACUJE VIEW-CHANGE od Node %d - neispravni P/Q skupovi\n", n.ID, msg.SenderID)
		return
	}
	if n.viewChangeLog[msg.View] == nil {
		n.viewChangeLog[msg.View] = make(map[int]message.Message)
	}
	if _, exists := n.viewChangeLog[msg.View][msg.SenderID]; exists {
		n.mu.Unlock()
		return
	}
	n.viewChangeLog[msg.View][msg.SenderID] = msg
	n.mu.Unlock()

	if msg.SenderID != n.ID {
		newPrimary := msg.View % n.TotalNodes
		ackMsg := message.Message{
			Type:      message.ViewChangeAck,
			View:      msg.View,
			SenderID:  n.ID,
			AckTarget: msg.SenderID,
			AckDigest: viewChangeDigest(msg),
		}
		fmt.Printf("[Node %d] šalje VIEW-CHANGE-ACK novom primarnom (Node %d) za VIEW-CHANGE od Node %d\n", n.ID, newPrimary, msg.SenderID)
		n.SendTo(newPrimary, ackMsg)
	}

	if msg.View%n.TotalNodes == n.ID {
		n.tryBuildNewView(msg.View)
	}

	n.tryVerifyNewView(msg.View)
}

func (n *Node) onViewChangeAck(msg message.Message) {
	newPrimary := msg.View % n.TotalNodes
	if n.ID != newPrimary {
		return
	}

	fmt.Printf("[Node %d] primio VIEW-CHANGE-ACK od Node %d za VIEW-CHANGE od Node %d (view %d)\n", n.ID, msg.SenderID, msg.AckTarget, msg.View)

	n.mu.Lock()
	if n.viewChangeAckCount[msg.View] == nil {
		n.viewChangeAckCount[msg.View] = make(map[int]map[int]bool)
	}
	if n.viewChangeAckCount[msg.View][msg.AckTarget] == nil {
		n.viewChangeAckCount[msg.View][msg.AckTarget] = make(map[int]bool)
	}
	if msg.SenderID != msg.AckTarget {
		n.viewChangeAckCount[msg.View][msg.AckTarget][msg.SenderID] = true
	}
	n.mu.Unlock()

	n.tryBuildNewView(msg.View)
}

func (n *Node) tryBuildNewView(newView int) {
	if newView%n.TotalNodes != n.ID {
		return
	}

	n.mu.Lock()
	if n.newViewSent[newView] {
		n.mu.Unlock()
		return
	}

	qualified := make(map[int]message.Message)
	for sender, vcMsg := range n.viewChangeLog[newView] {
		acks := len(n.viewChangeAckCount[newView][sender])
		if sender == n.ID || acks >= 2*n.F-1 {
			qualified[sender] = vcMsg
		}
	}
	n.mu.Unlock()

	if len(qualified) < 2*n.F+1 {
		return
	}

	h, decisions, complete := n.decisionProcedure(qualified)
	if !complete {
		return
	}

	for seq, d := range decisions {
		fmt.Printf("[Node %d | NOVI PRIMARNI] odluka za sekvencu %d -> %q (na osnovu P/Q iz prethodnog view-a)\n", n.ID, seq, d)
	}

	n.mu.Lock()
	n.newViewSent[newView] = true
	n.mu.Unlock()

	var vSet []message.VCEntry
	for sender, vcMsg := range qualified {
		vSet = append(vSet, message.VCEntry{ReplicaID: sender, Digest: viewChangeDigest(vcMsg)})
	}

	var xSet []message.XEntry
	for seq, digest := range decisions {
		payload := ""
		if digest != "null" {
			n.mu.Lock()
			if req, ok := n.requestLog[digest]; ok {
				payload = req.Payload
			}
			n.mu.Unlock()
		}
		xSet = append(xSet, message.XEntry{Sequence: seq, Digest: digest, Payload: payload})
	}

	newViewMsg := message.Message{
		Type:     message.NewView,
		View:     newView,
		SenderID: n.ID,
		VSet:     vSet,
		XSet:     xSet,
	}

	fmt.Printf("[Node %d | NOVI PRIMARNI] šalje NEW-VIEW za view %d (checkpoint h=%d, %d izabranih zahteva)\n", n.ID, newView, h, len(xSet))

	n.Broadcast(newViewMsg)
	n.processNewView(newViewMsg)
}

func (n *Node) decisionProcedure(S map[int]message.Message) (h int, decisions map[int]string, complete bool) {
	h = 0
	quorumSize := 2*n.F + 1
	weakSize := n.F + 1

	seqSet := map[int]bool{}
	for _, vc := range S {
		for _, p := range vc.PSet {
			seqSet[p.Sequence] = true
		}
		for _, q := range vc.QSet {
			seqSet[q.Sequence] = true
		}
	}

	decisions = make(map[int]string)

	for seq := range seqSet {
		chosen := ""
		chosenFound := false

		// Uslov A: postoji poruka m' u S sa P zapisom (seq, d, v') koji zadovoljava A1 i A2
		for _, mPrime := range S {
			var dPrime string
			var vPrime int
			foundP := false
			for _, p := range mPrime.PSet {
				if p.Sequence == seq {
					dPrime, vPrime, foundP = p.Digest, p.View, true
					break
				}
			}
			if !foundP {
				continue
			}

			countA1 := 0
			for _, mDouble := range S {
				ok := true
				for _, p := range mDouble.PSet {
					if p.Sequence == seq && !(p.View < vPrime || (p.View == vPrime && p.Digest == dPrime)) {
						ok = false
					}
				}
				if ok {
					countA1++
				}
			}

			countA2 := 0
			for _, mDouble := range S {
				for _, q := range mDouble.QSet {
					if q.Sequence == seq && q.Digest == dPrime && q.View >= vPrime {
						countA2++
						break
					}
				}
			}

			if countA1 >= quorumSize && countA2 >= weakSize {
				chosen, chosenFound = dPrime, true
				break
			}
		}

		if chosenFound {
			decisions[seq] = chosen
			continue
		}

		// Uslov B: 2f+1 poruka bez P zapisa za ovu sekvencu -> NULL zahtev
		countB := 0
		for _, m := range S {
			hasEntry := false
			for _, p := range m.PSet {
				if p.Sequence == seq {
					hasEntry = true
					break
				}
			}
			if !hasEntry {
				countB++
			}
		}
		if countB >= quorumSize {
			decisions[seq] = "null"
		}
	}

	complete = len(decisions) == len(seqSet)
	return h, decisions, complete
}

func (n *Node) onNewView(msg message.Message) {
	expectedPrimary := msg.View % n.TotalNodes
	if msg.SenderID != expectedPrimary {
		fmt.Printf("[Node %d] ODBACUJE NEW-VIEW - Node %d nije primarni za view %d\n", n.ID, msg.SenderID, msg.View)
		return
	}

	n.mu.Lock()
	tooOld := n.View > msg.View
	n.mu.Unlock()
	if tooOld {
		return
	}

	fmt.Printf("[Node %d] primio NEW-VIEW od Node %d za view %d, proveravam ispravnost odluke\n", n.ID, msg.SenderID, msg.View)

	n.mu.Lock()
	n.pendingNewView[msg.View] = msg
	n.mu.Unlock()

	n.tryVerifyNewView(msg.View)
}

func (n *Node) tryVerifyNewView(view int) {
	n.mu.Lock()
	msg, ok := n.pendingNewView[view]
	if !ok || n.View > view {
		n.mu.Unlock()
		return
	}

	S := make(map[int]message.Message)
	var missingIDs []int
	for _, v := range msg.VSet {
		vcMsg, exists := n.viewChangeLog[view][v.ReplicaID]
		if !exists || viewChangeDigest(vcMsg) != v.Digest {
			missingIDs = append(missingIDs, v.ReplicaID)
			continue
		}
		S[v.ReplicaID] = vcMsg
	}
	n.mu.Unlock()

	if len(missingIDs) > 0 {
		for _, id := range missingIDs {
			fmt.Printf("[Node %d] nedostaje VIEW-CHANGE od Node %d za view %d, tražim retransmisiju od Node %d\n", n.ID, id, view, msg.SenderID)
			fetchMsg := message.Message{
				Type:      message.FetchViewChange,
				View:      view,
				SenderID:  n.ID,
				AckTarget: id,
			}
			n.SendTo(msg.SenderID, fetchMsg)
		}
		return
	}

	if len(S) < 2*n.F+1 {
		fmt.Printf("[Node %d] ODBACUJE NEW-VIEW - V skup nema kvorum (%d < %d)\n", n.ID, len(S), 2*n.F+1)
		n.rejectPendingNewView(view)
		return
	}

	_, myDecisions, complete := n.decisionProcedure(S)
	if !complete || len(myDecisions) != len(msg.XSet) {
		fmt.Printf("[Node %d] ODBACUJE NEW-VIEW - moja decision procedura se ne poklapa sa primarnim\n", n.ID)
		n.rejectPendingNewView(view)
		return
	}
	for _, x := range msg.XSet {
		if myDecisions[x.Sequence] != x.Digest {
			fmt.Printf("[Node %d] ODBACUJE NEW-VIEW - odluka za sekvencu %d se ne poklapa\n", n.ID, x.Sequence)
			n.rejectPendingNewView(view)
			return
		}
	}

	fmt.Printf("[Node %d] NEW-VIEW verifikovan - odluka se poklapa sa mojom sopstvenom decision procedurom\n", n.ID)
	n.mu.Lock()
	delete(n.pendingNewView, view)
	n.mu.Unlock()
	n.processNewView(msg)
}

func (n *Node) rejectPendingNewView(view int) {
	n.mu.Lock()
	delete(n.pendingNewView, view)
	alreadyMoved := n.View > view
	n.mu.Unlock()
	if !alreadyMoved {
		n.startViewChange()
	}
}

func (n *Node) onFetchViewChange(msg message.Message) {
	n.mu.Lock()
	vcMsg, ok := n.viewChangeLog[msg.View][msg.AckTarget]
	n.mu.Unlock()
	if !ok {
		return // ni mi je nemamo
	}
	fmt.Printf("[Node %d] retransmituje VIEW-CHANGE od Node %d (view %d) čvoru %d\n", n.ID, msg.AckTarget, msg.View, msg.SenderID)
	n.SendTo(msg.SenderID, vcMsg)
}

func (n *Node) processNewView(msg message.Message) {
	n.mu.Lock()
	if n.activeView && n.View == msg.View {
		n.mu.Unlock()
		return
	}
	n.View = msg.View
	n.activeView = true
	n.mu.Unlock()

	n.mu.Lock()
	for _, x := range msg.XSet {
		if x.Sequence > n.seqCounter {
			n.seqCounter = x.Sequence
		}
	}
	n.mu.Unlock()

	fmt.Printf("[Node %d] AKTIVAN u view %d, obrađuje %d zahteva iz NEW-VIEW\n", n.ID, msg.View, len(msg.XSet))

	for _, x := range msg.XSet {
		if x.Digest == "null" {
			fmt.Printf("[Node %d] sekvenca %d: NULL zahtev (no-op) po view change proceduri\n", n.ID, x.Sequence)
			continue
		}

		n.mu.Lock()
		if _, exists := n.requestLog[x.Digest]; !exists {
			n.requestLog[x.Digest] = message.Message{Type: message.Request, Digest: x.Digest, Payload: x.Payload}
		}
		n.prePrepareReceived[x.Sequence] = x.Digest
		n.prePreparedView[x.Sequence] = msg.View
		n.mu.Unlock()

		if n.IsPrimary() {
			n.mu.Lock()
			n.assignedRequests[x.Digest] = true
			n.mu.Unlock()
			ppMsg := message.Message{Type: message.PrePrepare, View: msg.View, Sequence: x.Sequence, Digest: x.Digest, SenderID: n.ID, Payload: x.Payload}
			fmt.Printf("[Node %d | NOVI PRIMARNI] ponovo predlaže sekvencu %d u view %d\n", n.ID, x.Sequence, msg.View)
			n.Broadcast(ppMsg)
		} else {
			fmt.Printf("[Node %d] šalje PREPARE za sekvencu %d u novom view %d (na osnovu NEW-VIEW)\n", n.ID, x.Sequence, msg.View)
			n.enterPreparePhase(x.Sequence, x.Digest, msg.View)
		}
	}
}

func (n *Node) startConsensusFor(req message.Message, sequence int) {
	prePrepareMsg := message.Message{
		Type:     message.PrePrepare,
		View:     n.View,
		Sequence: sequence,
		Digest:   req.Digest,
		SenderID: n.ID,
		Payload:  req.Payload,
	}

	fmt.Printf("[Node %d | PRIMARNI] dodeljuje sekvencu %d zahtevu %q, šalje PRE-PREPARE\n", n.ID, sequence, req.Payload)

	n.mu.Lock()
	n.prePrepareReceived[sequence] = req.Digest
	n.prePreparedView[sequence] = n.View
	n.mu.Unlock()

	n.Broadcast(prePrepareMsg)

	n.mu.Lock()
	n.enteredPrePrepared[sequence] = n.View
	n.mu.Unlock()
}

/*
	func (n *Node) StartConsensus(payload string, sequence int) {
		if !n.IsPrimary() {
			fmt.Printf("[Node %d] nije primarni čvor, ne može da pokrene konsenzus\n", n.ID)
			return
		}

		msg := message.Message{
			Type:     message.PrePrepare,
			View:     n.View,
			Sequence: sequence,
			Digest:   payload,
			SenderID: n.ID,
			Payload:  payload,
		}

		fmt.Printf("[Node %d | PRIMARNI] šalje PRE-PREPARE za sekvencu %d: %q\n", n.ID, sequence, payload)
		n.mu.Lock()
		n.prePrepareReceived[sequence] = payload
		n.mu.Unlock()
		n.Broadcast(msg)
	}
*/
func (n *Node) handleMessage(msg message.Message) {
	switch msg.Type {
	case message.Request:
		n.onRequest(msg)
	case message.PrePrepare:
		n.onPrePrepare(msg)
	case message.Prepare:
		n.onPrepare(msg)
	case message.Commit:
		n.onCommit(msg)
	case message.ViewChange:
		n.onViewChange(msg)
	case message.ViewChangeAck:
		n.onViewChangeAck(msg)
	case message.NewView:
		n.onNewView(msg)
	case message.FetchViewChange:
		n.onFetchViewChange(msg)
	}
}

func (n *Node) onPrePrepare(msg message.Message) {
	fmt.Printf("[Node %d] primio PRE-PREPARE od Node %d za sekvencu %d: %q\n", n.ID, msg.SenderID, msg.Sequence, msg.Payload)

	if msg.View != n.View {
		fmt.Printf("[Node %d] ODBACUJE PRE-PREPARE - pogrešan view (poruka: %d, moj: %d)\n", n.ID, msg.View, n.View)
		return
	}

	expectedPrimary := msg.View % n.TotalNodes
	if msg.SenderID != expectedPrimary {
		fmt.Printf("[Node %d] ODBACUJE PRE-PREPARE - Node %d nije primarni za view %d (očekivan Node %d)\n", n.ID, msg.SenderID, msg.View, expectedPrimary)
		return
	}

	n.mu.Lock()
	existingDigest, has := n.prePrepareReceived[msg.Sequence]
	existingView, hasView := n.prePreparedView[msg.Sequence]
	n.mu.Unlock()

	if has && hasView && existingView == msg.View && existingDigest != msg.Digest {
		fmt.Printf("[Node %d] ODBACUJE PRE-PREPARE - konflikt za sekvencu %d - moguće vizantijsko ponašanje primarnog\n", n.ID, msg.Sequence)
		return
	}

	n.mu.Lock()
	n.prePrepareReceived[msg.Sequence] = msg.Digest
	n.prePreparedView[msg.Sequence] = msg.View
	_, hasRequest := n.requestLog[msg.Digest]
	n.mu.Unlock()

	if !hasRequest {
		fmt.Printf("[Node %d] PRE-PREPARE prihvaćen za sekvencu %d, ali REQUEST od klijenta još nije stigao - čekam (rad, Sekcija 4.3)\n", n.ID, msg.Sequence)
		n.mu.Lock()
		n.pendingPrePrepares[msg.Digest] = msg
		n.mu.Unlock()
		return
	}

	n.enterPreparePhase(msg.Sequence, msg.Digest, msg.View)
}

func (n *Node) enterPreparePhase(sequence int, digest string, view int) {
	fmt.Printf("[Node %d] šalje PREPARE za sekvencu %d (digest: %q)\n", n.ID, sequence, digest)

	prepareMsg := message.Message{
		Type:     message.Prepare,
		View:     view,
		Sequence: sequence,
		Digest:   digest,
		SenderID: n.ID,
	}

	n.recordVote(n.prepareVotes, sequence, view, digest, n.ID)
	n.Broadcast(prepareMsg)
	n.checkPrepared(sequence, digest, view)
	n.mu.Lock()
	n.enteredPrePrepared[sequence] = view
	n.mu.Unlock()
}

func (n *Node) recordVote(votes map[int]map[int]map[string]map[int]bool, sequence, view int, digest string, senderID int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if votes[sequence] == nil {
		votes[sequence] = make(map[int]map[string]map[int]bool)
	}
	if votes[sequence][view] == nil {
		votes[sequence][view] = make(map[string]map[int]bool)
	}
	if votes[sequence][view][digest] == nil {
		votes[sequence][view][digest] = make(map[int]bool)
	}
	votes[sequence][view][digest][senderID] = true
}

func (n *Node) countVotes(votes map[int]map[int]map[string]map[int]bool, sequence, view int, digest string) int {
	n.mu.Lock()
	defer n.mu.Unlock()

	return len(votes[sequence][view][digest])
}

func (n *Node) quorum() int {
	return 2*n.F + 1
}

func (n *Node) onPrepare(msg message.Message) {
	fmt.Printf("[Node %d] primio PREPARE od Node %d za sekvencu %d (digest: %q)\n", n.ID, msg.SenderID, msg.Sequence, msg.Digest)

	if msg.View != n.View {
		fmt.Printf("[Node %d] ODBACUJE PREPARE - pogrešan view (poruka: %d, moj: %d)\n", n.ID, msg.View, n.View)
		return
	}

	n.recordVote(n.prepareVotes, msg.Sequence, msg.View, msg.Digest, msg.SenderID)
	n.checkPrepared(msg.Sequence, msg.Digest, msg.View)
}

func (n *Node) checkPrepared(sequence int, digest string, view int) {
	n.mu.Lock()
	prePrepareDigest, hasPrePrepare := n.prePrepareReceived[sequence]
	alreadyCommitSent := n.commitSent[sequence][view]
	n.mu.Unlock()

	if !hasPrePrepare || prePrepareDigest != digest || alreadyCommitSent {
		return
	}

	if n.countVotes(n.prepareVotes, sequence, view, digest) >= 2*n.F {
		n.mu.Lock()
		n.preparedSent[sequence] = true // za P skup (rad, Sekcija 4.5) - pamti da je sequence BAR JEDNOM bio prepared
		n.preparedView[sequence] = view
		if n.commitSent[sequence] == nil {
			n.commitSent[sequence] = make(map[int]bool)
		}
		n.commitSent[sequence][view] = true
		n.mu.Unlock()

		fmt.Printf("[Node %d] PREPARED za sekvencu %d (1 pre-prepare + %d prepare glasova), šalje COMMIT\n", n.ID, sequence, 2*n.F)

		commitMsg := message.Message{
			Type:     message.Commit,
			View:     view,
			Sequence: sequence,
			Digest:   digest,
			SenderID: n.ID,
			Payload:  digest,
		}

		n.recordVote(n.commitVotes, sequence, view, digest, n.ID)
		n.Broadcast(commitMsg)
		n.tryCommit(sequence, digest, view)
	}
}

func (n *Node) tryCommit(sequence int, digest string, view int) {
	if n.countVotes(n.commitVotes, sequence, view, digest) >= n.quorum() {
		n.mu.Lock()
		alreadyCommitted := n.committed[sequence]
		isPrepared := n.preparedSent[sequence]
		ownDigest := n.prePrepareReceived[sequence]
		n.mu.Unlock()

		if !isPrepared || ownDigest != digest {
			if !alreadyCommitted {
				fmt.Printf("[Node %d] kvorum COMMIT postignut za sekvencu %d, ali JA nisam prepared za digest %q - ne izvršavam, čekam sinhronizaciju\n", n.ID, sequence, digest)
			}
			return
		}

		if !alreadyCommitted {
			n.mu.Lock()
			n.committed[sequence] = true
			n.mu.Unlock()

			n.stopTimer()

			fmt.Printf("[Node %d] COMMITTED sekvenca %d, izvršava zahtev: %q\n", n.ID, sequence, digest)

			n.mu.Lock()
			req, hasReq := n.requestLog[digest]
			n.mu.Unlock()

			if hasReq {
				replyMsg := message.Message{
					Type:      message.Reply,
					View:      n.View,
					SenderID:  n.ID,
					Payload:   digest,
					Timestamp: req.Timestamp,
				}

				n.mu.Lock()
				n.executedDigests[digest] = replyMsg
				clientCh, ok := n.ClientChannels[req.SenderID]
				n.mu.Unlock()

				if ok {
					fmt.Printf("[Node %d] šalje REPLY klijentu %d za timestamp %d\n", n.ID, req.SenderID, req.Timestamp)
					select {
					case clientCh <- replyMsg:
					default:
					}
				}
			}
		}
	}
}

func (n *Node) onCommit(msg message.Message) {
	fmt.Printf("[Node %d] primio COMMIT od Node %d za sekvencu %d (digest: %q)\n", n.ID, msg.SenderID, msg.Sequence, msg.Digest)

	if msg.View != n.View {
		fmt.Printf("[Node %d] ODBACUJE COMMIT - pogrešan view (poruka: %d, moj: %d)\n", n.ID, msg.View, n.View)
		return
	}

	n.recordVote(n.commitVotes, msg.Sequence, msg.View, msg.Digest, msg.SenderID)

	n.tryCommit(msg.Sequence, msg.Digest, msg.View)
}

func (n *Node) StopSignal() <-chan struct{} {
	return n.stopCh
}
