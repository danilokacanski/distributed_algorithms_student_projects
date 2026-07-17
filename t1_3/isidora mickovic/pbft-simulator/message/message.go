package message

type MessageType int

const (
	Request MessageType = iota
	PrePrepare
	Prepare
	Commit
	ViewChange
	NewView
	Reply
	ViewChangeAck
	FetchViewChange
)

func (m MessageType) String() string {
	switch m {
	case Request:
		return "REQUEST"
	case PrePrepare:
		return "PRE-PREPARE"
	case Prepare:
		return "PREPARE"
	case Commit:
		return "COMMIT"
	case ViewChange:
		return "VIEW-CHANGE"
	case ViewChangeAck:
		return "VIEW-CHANGE-ACK"
	case NewView:
		return "NEW-VIEW"
	case Reply:
		return "REPLY"
	case FetchViewChange:
		return "FETCH-VIEW-CHANGE"
	default:
		return "UNKNOWN"
	}
}

type PQEntry struct {
	Sequence int
	Digest   string
	View     int
}

type XEntry struct {
	Sequence int
	Digest   string
	Payload  string
}

type VCEntry struct {
	ReplicaID int
	Digest    string
}

type Message struct {
	Type      MessageType
	View      int    
	Sequence  int    
	Digest    string 
	SenderID  int    
	Payload   string
	Timestamp int

	LowWaterMark int 
	PSet         []PQEntry
	QSet         []PQEntry

	AckTarget int    
	AckDigest string 

	VSet []VCEntry 
	XSet []XEntry  
}