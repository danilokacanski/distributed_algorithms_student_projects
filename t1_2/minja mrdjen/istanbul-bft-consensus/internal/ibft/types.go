package ibft

const (
	NoPreparedRound = 0
	NoPreparedValue = ""
)

type MessageType string

const (
	PrePrepare          MessageType = "PRE-PREPARE"
	Prepare             MessageType = "PREPARE"
	Commit              MessageType = "COMMIT"
	RoundChange         MessageType = "ROUND-CHANGE"
	DecisionCertificate MessageType = "DECISION-CERTIFICATE"
)

type ByzantineMode string

const (
	ByzantineNone       ByzantineMode = "NONE"
	ByzantineSilent     ByzantineMode = "SILENT"
	ByzantineEquivocate ByzantineMode = "EQUIVOCATE"
	ByzantineBadVote    ByzantineMode = "BAD_VOTE"
)

type TimerState string

const (
	TimerStopped TimerState = "STOPPED"
	TimerRunning TimerState = "RUNNING"
	TimerExpired TimerState = "EXPIRED"
)

type Message struct {
	Type   MessageType
	From   int
	Lambda int
	Round  int
	Value  string

	// Used only in ROUND-CHANGE messages.
	PreparedRound int
	PreparedValue string

	// Placeholder for later steps, when we add message justification.
	Justification []Message
	Signature     string
}

type NodeState string

const (
	StateNew         NodeState = "NEW"
	StatePrePrepared NodeState = "PRE-PREPARED"
	StatePrepared    NodeState = "PREPARED"
	StateDecided     NodeState = "DECIDED"
)
