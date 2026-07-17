package ibft

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
)

type Node struct {
	ID            int
	Validators    []int
	Byzantine     bool
	ByzantineMode ByzantineMode

	// State variables from Algorithm 1.
	Lambda        int
	CurrentRound  int
	PreparedRound int
	PreparedValue string
	InputValue    string
	TimerState    TimerState

	// Additional implementation state for simulation/debug output.
	State         NodeState
	AcceptedValue string
	AcceptedRound int

	PrepareMessages     map[string]map[int]Message
	CommitMessages      map[string]map[int]Message
	RoundChangeMessages map[int]map[int]Message

	DecisionCertificate []Message
	DecisionValue       string

	RoundChangeSent map[int]bool
	PrePrepareSent  map[int]bool
	CommitSent      bool
	Decided         bool

	Inbox      chan Message
	PublicKey  ed25519.PublicKey
	PrivateKey ed25519.PrivateKey
}

func NewNode(id int, validators []int) *Node {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(fmt.Sprintf("greska pri generisanju kljuceva za Node %d: %v", id, err))
	}

	RegisterPublicKey(id, publicKey)

	return &Node{
		ID:            id,
		Validators:    validators,
		Byzantine:     false,
		ByzantineMode: ByzantineNone,

		Lambda:        1,
		CurrentRound:  1,
		PreparedRound: NoPreparedRound,
		PreparedValue: NoPreparedValue,
		InputValue:    "",
		TimerState:    TimerStopped,

		State:         StateNew,
		AcceptedValue: "",
		AcceptedRound: -1,

		PrepareMessages:     make(map[string]map[int]Message),
		CommitMessages:      make(map[string]map[int]Message),
		RoundChangeMessages: make(map[int]map[int]Message),

		DecisionCertificate: nil,
		DecisionValue:       "",

		RoundChangeSent: make(map[int]bool),
		PrePrepareSent:  make(map[int]bool),
		Inbox:           make(chan Message, 100),
		PublicKey:       publicKey,
		PrivateKey:      privateKey,
	}
}

func (n *Node) Start(lambda int, value string, net *Network) {
	n.Lambda = lambda
	n.CurrentRound = 1
	n.PreparedRound = NoPreparedRound
	n.PreparedValue = NoPreparedValue
	n.InputValue = value

	n.State = StateNew
	n.AcceptedValue = ""
	n.AcceptedRound = -1

	n.PrepareMessages = make(map[string]map[int]Message)
	n.CommitMessages = make(map[string]map[int]Message)
	n.RoundChangeMessages = make(map[int]map[int]Message)

	n.DecisionCertificate = nil
	n.DecisionValue = ""

	n.RoundChangeSent = make(map[int]bool)
	n.PrePrepareSent = make(map[int]bool)
	n.CommitSent = false
	n.Decided = false

	fmt.Printf("Node %d pokrece instancu lambda=%d sa inputValue=%s\n",
		n.ID, n.Lambda, n.InputValue)

	n.StartTimer()

	if Leader(n.Lambda, n.CurrentRound, n.Validators) == n.ID {
		n.Propose(n.InputValue, net, n.CurrentRound)
	}
}

func messageKey(lambda int, round int, value string) string {
	return fmt.Sprintf("lambda=%d|round=%d|value=%s", lambda, round, value)
}

func printableValue(value string) string {
	if value == NoPreparedValue {
		return "⊥"
	}

	return value
}

func (n *Node) signMessage(msg Message) Message {
	msg.Signature = SignMessage(msg, n.PrivateKey)
	return msg
}

func (n *Node) Propose(value string, net *Network, round int) bool {
	if n.ByzantineMode == ByzantineSilent {
		fmt.Printf("Node %d je vizantijski lider u SILENT rezimu i ne predlaze vrednost\n", n.ID)
		return false
	}

	if n.ByzantineMode == ByzantineEquivocate {
		return n.proposeEquivocating(value, net, round)
	}

	leader := Leader(n.Lambda, round, n.Validators)

	if n.ID != leader {
		fmt.Printf("Node %d nije lider za round %d, ne moze da predlozi vrednost\n",
			n.ID, round)
		return false
	}

	if n.CurrentRound != round {
		fmt.Printf("Node %d nije u round %d, vec u round %d\n",
			n.ID, round, n.CurrentRound)
		return false
	}

	if n.PrePrepareSent[round] {
		return false
	}

	proposalValue, highestPrepared, justified := n.proposalValueForRound(round, value)
	if !justified {
		fmt.Printf("Node %d ne moze da predlozi vrednost u round %d jer ROUND-CHANGE quorum nije opravdan\n",
			n.ID, round)
		return false
	}

	if round > 1 {
		if highestPrepared.Found {
			fmt.Printf("Node %d bira vrednost preko HighestPrepared: pr=%d, pv=%s\n",
				n.ID,
				highestPrepared.PreparedRound,
				highestPrepared.PreparedValue,
			)
		} else {
			fmt.Printf("Node %d nema prepared vrednost u opravdanom ROUND-CHANGE quorum-u, koristi inputValue: %s\n",
				n.ID,
				proposalValue,
			)
		}
	}

	prePrepareJustification := []Message(nil)

	if round > 1 {
		prePrepareJustification = roundChangeMessagesToSlice(n.RoundChangeMessages[round])
	}

	msg := n.signMessage(Message{
		Type:          PrePrepare,
		From:          n.ID,
		Lambda:        n.Lambda,
		Round:         round,
		Value:         proposalValue,
		Justification: prePrepareJustification,
	})

	n.StartTimer()
	n.PrePrepareSent[round] = true

	fmt.Printf("Node %d je lider za lambda %d, round %d i predlaze vrednost: %s\n",
		n.ID, n.Lambda, round, proposalValue)

	net.Broadcast(msg)
	return true
}

func (n *Node) proposeEquivocating(value string, net *Network, round int) bool {
	leader := Leader(n.Lambda, round, n.Validators)

	if n.ID != leader {
		fmt.Printf("Node %d je vizantijski, ali nije lider za round %d\n", n.ID, round)
		return false
	}

	if n.CurrentRound != round {
		fmt.Printf("Node %d nije u round %d, vec u round %d\n",
			n.ID, round, n.CurrentRound)
		return false
	}

	if n.PrePrepareSent[round] {
		return false
	}

	fmt.Printf("Node %d je VIZANTIJSKI LIDER i salje razlicite vrednosti razlicitim cvorovima\n", n.ID)

	for _, id := range n.Validators {
		var maliciousValue string

		if id%2 == 0 {
			maliciousValue = value + "-A"
		} else {
			maliciousValue = value + "-B"
		}

		msg := n.signMessage(Message{
			Type:   PrePrepare,
			From:   n.ID,
			Lambda: n.Lambda,
			Round:  round,
			Value:  maliciousValue,
		})

		fmt.Printf("Node %d salje Node %d vrednost %s\n", n.ID, id, maliciousValue)

		net.Send(id, msg)
	}

	n.PrePrepareSent[round] = true
	return true
}

func (n *Node) HandleMessage(msg Message, net *Network) {
	if n.ByzantineMode == ByzantineSilent {
		fmt.Printf("Node %d je vizantijski (%s) i ignorise poruku %s\n",
			n.ID, n.ByzantineMode, msg.Type)
		return
	}

	if n.ByzantineMode == ByzantineEquivocate {
		fmt.Printf("Node %d je vizantijski (%s) i ignorise poruku %s\n",
			n.ID, n.ByzantineMode, msg.Type)
		return
	}

	if n.ByzantineMode == ByzantineBadVote {
		n.handleBadVote(msg, net)
		return
	}

	if !IsValidMessage(msg, n.Validators) {
		fmt.Printf("Node %d odbija nevalidnu poruku %s od Node %d\n",
			n.ID, msg.Type, msg.From)
		return
	}

	if msg.Lambda != n.Lambda {
		fmt.Printf("Node %d ignorise poruku za lambda %d, jer izvrsava lambda %d\n",
			n.ID, msg.Lambda, n.Lambda)
		return
	}

	if n.Decided {
		if msg.Type == RoundChange {
			n.sendDecisionCertificate(msg.From, net)
		}

		return
	}

	switch msg.Type {
	case PrePrepare:
		n.handlePrePrepare(msg, net)

	case Prepare:
		n.handlePrepare(msg, net)

	case Commit:
		n.handleCommit(msg, net)

	case RoundChange:
		n.handleRoundChange(msg, net)

	case DecisionCertificate:
		n.handleDecisionCertificate(msg)
	}
}

func (n *Node) handleBadVote(msg Message, net *Network) {
	if msg.Lambda != n.Lambda {
		return
	}

	if msg.Type != PrePrepare {
		fmt.Printf("Node %d je vizantijski (%s) i ignorise poruku %s\n",
			n.ID, n.ByzantineMode, msg.Type)
		return
	}

	if msg.Round != n.CurrentRound {
		return
	}

	leader := Leader(msg.Lambda, msg.Round, n.Validators)

	if msg.From != leader {
		fmt.Printf("Node %d odbija PRE-PREPARE od Node %d jer lider za round %d je Node %d\n",
			n.ID, msg.From, msg.Round, leader)
		return
	}

	fakeValue := msg.Value + "-FAKE"

	fmt.Printf("Node %d je vizantijski validator i umesto %s salje PREPARE za pogresnu vrednost %s\n",
		n.ID, msg.Value, fakeValue)

	badPrepare := n.signMessage(Message{
		Type:   Prepare,
		From:   n.ID,
		Lambda: msg.Lambda,
		Round:  msg.Round,
		Value:  fakeValue,
	})

	net.Broadcast(badPrepare)
}

func (n *Node) handlePrePrepare(msg Message, net *Network) {
	if msg.Lambda != n.Lambda || msg.Round != n.CurrentRound {
		return
	}

	leader := Leader(msg.Lambda, msg.Round, n.Validators)

	if msg.From != leader {
		fmt.Printf("Node %d odbija PRE-PREPARE od Node %d jer lider za round %d je Node %d\n",
			n.ID, msg.From, msg.Round, leader)
		return
	}

	if !JustifyPrePrepare(msg, len(n.Validators)) {
		fmt.Printf("Node %d odbija PRE-PREPARE od Node %d jer nije opravdan za round %d\n",
			n.ID, msg.From, msg.Round)
		return
	}

	if n.State != StateNew {
		return
	}

	n.AcceptedValue = msg.Value
	n.AcceptedRound = msg.Round
	n.State = StatePrePrepared
	n.StartTimer()

	fmt.Printf("Node %d prihvata PRE-PREPARE za vrednost %s u round %d\n",
		n.ID, msg.Value, msg.Round)

	prepare := n.signMessage(Message{
		Type:   Prepare,
		From:   n.ID,
		Lambda: msg.Lambda,
		Round:  msg.Round,
		Value:  msg.Value,
	})

	net.Broadcast(prepare)
}

func (n *Node) handlePrepare(msg Message, net *Network) {
	if msg.Lambda != n.Lambda || msg.Round != n.CurrentRound {
		return
	}

	if n.AcceptedValue != msg.Value || n.AcceptedRound != msg.Round {
		return
	}

	key := messageKey(msg.Lambda, msg.Round, msg.Value)

	if n.PrepareMessages[key] == nil {
		n.PrepareMessages[key] = make(map[int]Message)
	}

	n.PrepareMessages[key][msg.From] = msg

	count := len(n.PrepareMessages[key])
	quorum := QuorumSize(len(n.Validators))

	fmt.Printf("Node %d ima %d/%d PREPARE poruka za %s u round %d\n",
		n.ID, count, quorum, msg.Value, msg.Round)

	if count >= quorum && n.State == StatePrePrepared && !n.CommitSent {
		n.PreparedRound = msg.Round
		n.PreparedValue = msg.Value

		n.State = StatePrepared
		n.CommitSent = true

		fmt.Printf("Node %d je PREPARED za vrednost %s u round %d i salje COMMIT\n",
			n.ID, n.PreparedValue, n.PreparedRound)

		commit := n.signMessage(Message{
			Type:   Commit,
			From:   n.ID,
			Lambda: msg.Lambda,
			Round:  msg.Round,
			Value:  msg.Value,
		})

		net.Broadcast(commit)
	}
}

func (n *Node) commitCertificateForKey(key string) []Message {
	commitMessages := n.CommitMessages[key]

	certificate := make([]Message, 0, len(commitMessages))

	for _, commitMsg := range commitMessages {
		certificate = append(certificate, commitMsg)
	}

	return certificate
}

func validateCommitCertificate(certificate []Message, lambda int, validatorCount int) (string, int, bool) {
	if len(certificate) < QuorumSize(validatorCount) {
		return "", 0, false
	}

	seen := make(map[int]bool)

	value := ""
	round := 0

	for _, commitMsg := range certificate {
		if !IsValidMessage(commitMsg, validatorsFromCount(validatorCount)) {
			return "", 0, false
		}

		if commitMsg.Type != Commit {
			return "", 0, false
		}

		if commitMsg.Lambda != lambda {
			return "", 0, false
		}

		if value == "" {
			value = commitMsg.Value
			round = commitMsg.Round
		}

		if commitMsg.Value != value || commitMsg.Round != round {
			return "", 0, false
		}

		seen[commitMsg.From] = true
	}

	if len(seen) < QuorumSize(validatorCount) {
		return "", 0, false
	}

	return value, round, true
}

func (n *Node) handleCommit(msg Message, net *Network) {
	if msg.Lambda != n.Lambda || msg.Round != n.CurrentRound {
		return
	}

	if n.AcceptedValue != msg.Value || n.AcceptedRound != msg.Round {
		return
	}

	key := messageKey(msg.Lambda, msg.Round, msg.Value)

	if n.CommitMessages[key] == nil {
		n.CommitMessages[key] = make(map[int]Message)
	}

	n.CommitMessages[key][msg.From] = msg

	count := len(n.CommitMessages[key])
	quorum := QuorumSize(len(n.Validators))

	fmt.Printf("Node %d ima %d/%d COMMIT poruka za %s u round %d\n",
		n.ID, count, quorum, msg.Value, msg.Round)

	if count >= quorum && !n.Decided {
		qcommit := n.commitCertificateForKey(key)

		n.State = StateDecided
		n.Decided = true
		n.DecisionValue = msg.Value
		n.DecisionCertificate = qcommit
		n.StopTimer()

		fmt.Printf(">>> NODE %d JE ODLUCIO VREDNOST: %s uz Qcommit od %d COMMIT poruka <<<\n",
			n.ID, msg.Value, len(qcommit))
	}
}

func (n *Node) prepareJustification() []Message {
	if n.PreparedRound == NoPreparedRound || n.PreparedValue == NoPreparedValue {
		return nil
	}

	key := messageKey(n.Lambda, n.PreparedRound, n.PreparedValue)
	prepareMessages := n.PrepareMessages[key]

	justification := make([]Message, 0, len(prepareMessages))

	for _, prepareMsg := range prepareMessages {
		justification = append(justification, prepareMsg)
	}

	return justification
}

func (n *Node) enterRound(round int) {
	if round < n.CurrentRound {
		return
	}

	n.CurrentRound = round
	n.State = StateNew
	n.AcceptedValue = ""
	n.AcceptedRound = -1

	n.CommitMessages = make(map[string]map[int]Message)
	n.CommitSent = false

	n.StartTimer()
}

func (n *Node) broadcastRoundChange(round int, net *Network) {
	if n.RoundChangeSent[round] {
		return
	}

	n.RoundChangeSent[round] = true

	msg := n.signMessage(Message{
		Type:          RoundChange,
		From:          n.ID,
		Lambda:        n.Lambda,
		Round:         round,
		Value:         "",
		PreparedRound: n.PreparedRound,
		PreparedValue: n.PreparedValue,
		Justification: n.prepareJustification(),
	})

	fmt.Printf("Node %d salje ROUND-CHANGE za round %d sa pr=%d, pv=%s\n",
		n.ID, round, n.PreparedRound, printableValue(n.PreparedValue))

	net.Broadcast(msg)
}

func (n *Node) RequestRoundChange(targetRound int, net *Network) {
	if n.ByzantineMode != ByzantineNone {
		return
	}

	if n.Decided {
		return
	}

	if targetRound <= n.CurrentRound {
		return
	}

	n.enterRound(targetRound)
	n.broadcastRoundChange(targetRound, net)
}

func (n *Node) handleRoundChange(msg Message, net *Network) {
	if msg.Lambda != n.Lambda {
		return
	}

	targetRound := msg.Round

	if targetRound < n.CurrentRound {
		return
	}

	if msg.PreparedRound != NoPreparedRound && msg.PreparedRound >= targetRound {
		fmt.Printf("Node %d odbija ROUND-CHANGE od Node %d jer pr=%d nije manje od r=%d\n",
			n.ID, msg.From, msg.PreparedRound, targetRound)
		return
	}

	if n.RoundChangeMessages[targetRound] == nil {
		n.RoundChangeMessages[targetRound] = make(map[int]Message)
	}

	n.RoundChangeMessages[targetRound][msg.From] = msg

	count := len(n.RoundChangeMessages[targetRound])
	quorum := QuorumSize(len(n.Validators))
	fPlusOne := RoundChangeSetSize(len(n.Validators))

	fmt.Printf("Node %d ima %d/%d ROUND-CHANGE poruka za round %d\n",
		n.ID, count, quorum, targetRound)

	if targetRound > n.CurrentRound && count >= fPlusOne {
		fmt.Printf("Node %d je primio f+1 ROUND-CHANGE poruka za visu rundu %d i prelazi u nju\n",
			n.ID, targetRound)

		n.enterRound(targetRound)
		n.broadcastRoundChange(targetRound, net)
	}

	if count < quorum {
		return
	}

	qrc := n.RoundChangeMessages[targetRound]

	if !JustifyRoundChange(qrc, n.Lambda, targetRound, len(n.Validators)) {
		fmt.Printf("Node %d ima quorum ROUND-CHANGE poruka za round %d, ali Qrc nije opravdan\n",
			n.ID, targetRound)
		return
	}

	if targetRound > n.CurrentRound {
		n.enterRound(targetRound)
	}

	fmt.Printf("Node %d ima opravdan Qrc za round %d\n", n.ID, targetRound)

	leader := Leader(n.Lambda, targetRound, n.Validators)

	if n.ID == leader {
		fmt.Printf("Node %d je lider za round %d i ima opravdan Qrc, pa pokrece novu rundu\n",
			n.ID, targetRound)

		n.Propose(n.InputValue, net, targetRound)
	}
}

func (n *Node) sendDecisionCertificate(to int, net *Network) {
	if !n.Decided {
		return
	}

	if len(n.DecisionCertificate) == 0 {
		return
	}

	msg := n.signMessage(Message{
		Type:          DecisionCertificate,
		From:          n.ID,
		Lambda:        n.Lambda,
		Round:         n.PreparedRound,
		Value:         n.DecisionValue,
		Justification: n.DecisionCertificate,
	})

	fmt.Printf("Node %d je vec odlucio i salje Qcommit cvoru Node %d\n",
		n.ID, to)

	net.Send(to, msg)
}

func (n *Node) handleDecisionCertificate(msg Message) {
	if msg.Lambda != n.Lambda {
		return
	}

	if n.Decided {
		return
	}

	value, round, ok := validateCommitCertificate(
		msg.Justification,
		n.Lambda,
		len(n.Validators),
	)

	if !ok {
		fmt.Printf("Node %d odbija DECISION-CERTIFICATE od Node %d jer Qcommit nije validan\n",
			n.ID, msg.From)
		return
	}

	n.Decided = true
	n.State = StateDecided
	n.DecisionValue = value
	n.DecisionCertificate = msg.Justification
	n.PreparedValue = value
	n.PreparedRound = round
	n.AcceptedValue = value
	n.AcceptedRound = round
	n.StopTimer()

	fmt.Printf(">>> NODE %d JE SUSTIGAO ODLUKU PREKO Qcommit: %s <<<\n",
		n.ID, value)
}

func (n *Node) Run(net *Network) {
	for msg := range n.Inbox {
		n.HandleMessage(msg, net)
		net.MessageDone()
	}
}
