//Paket PBFT implementira pojednostavljen ali ispravan simulator
//Practical Byzantine Fault Tolerance (PBFT) algoritma opisan u:

//Miguel Castro, Barbara Liskov — "Practical Byzantine Fault Tolerance
//	and Proactive Recovery", ACM TOCS, Vol. 20, No. 4, November 2002.

//Implementiran je NORMALAN SLUCAJ trofazanog protokola (pre-prepare, prepare,
//commit, sekcija 4.3) i POJEDNOSTAVLJEN view-change protokol (sekcija 4.5)
// Ne postoji garbage collection (4.4), state transfer (6.2) i proactive
//recovery (sekcija 5).

// koristi se concurrent runtime, process, link and failures apstrakcije sa vezbi
package pbft

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/danilokacanski/da/week03_04_parallel/process"
)

// ============================================================================
// TIPOVI PORUKA
// ============================================================================

// PBFTType je process.Message.Type koriscen za svaku PBFT protokol poruku
const PBFTType = "PBFT"

// Tipovi poruka podrzani u PBFTMessage.Kind
const (
	KindClientRequest = "REQUEST"     // client -> replicas
	KindPrePrepare    = "PRE-PREPARE" // primary -> backups
	KindPrepare       = "PREPARE"     // replica -> all replicas
	KindCommit        = "COMMIT"      // replica -> all replicas
	KindReply         = "REPLY"       // replica -> client
	KindViewChange    = "VIEW-CHANGE" // replica -> all replicas
	KindNewView       = "NEW-VIEW"    // new primary -> all replicas
)

// ============================================================================
// TIPOVI DOMENA
// ============================================================================

//Operation je state-machine operacija koju klijent zahteva.

type Operation string

//ClientRequest je zahtev uvezan unutar PRE-PREPARE(sekcija 4.2)

type ClientRequest struct {
	ClientID  process.ProcessID
	Op        Operation
	Timestamp int64 // logicki timestamp, striktno se uvecava po klijentu
}

//Digest vraca kratak, deterministicki otisak zahteva, i oznacava
// kriptografsku poruku digest D(m). Dovoljno je dobra da detektuje
// equivocation (dva razlicita zahteva trazena za isti view/sekvencni
// broj) unutar simulatora.

func (r ClientRequest) Digest() string {
	raw := fmt.Sprintf("%s|%s|%d", r.ClientID, r.Op, r.Timestamp)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])[:12]
}

func (r ClientRequest) String() string {
	return fmt.Sprintf("%s(client=%s,ts=%d)", r.Op, r.ClientID, r.Timestamp)
}

// ============================================================================
// PBFT MESSAGE PAYLOAD
// ============================================================================

// PBFTMessage je strukturiran payload nosen unutar svakog process.Message
// ciji Type == PBFTType. Zajedno sa Kind enkodira svaki tip poruke u
// normalnom slucaju (sekcija 4.3) i view-change (ssekcija 4.5) protokole.
type PBFTMessage struct {
	Kind string

	//normalan slucaj (sekcija 4.3)
	View    int               //trenutan/prihvacen view broj
	Seq     int               //sekvencni broj dodeljen od primary-a
	Digest  string            //digest klijentovog zahteva
	Request ClientRequest     //ceo zahtev(u REQUEST/PRE-PREPARE)
	From    process.ProcessID //logicki posiljalac

	// polja za odgovore (sekcija 4.2)
	Result string // rezultat vracen klijentu

	// view-change polja (sekcija 4.5, uprosceno)
	NewView       int                 //zahtevan/aktiviran view
	LastExecuted  int                 //najvisi sekvencni broj izvrsen od strane posiljaoca
	PendingReq    *ClientRequest      //zahtev na koji posiljalac i dalje ceka, ako ga ima
	ViewChangeSet []process.ProcessID //(u NEW-VIEW) posiljaoci ciji 'VIEW-CHANGE' su izbrojani
}

func (m PBFTMessage) String() string {
	switch m.Kind {
	case KindClientRequest:
		return fmt.Sprintf("REQUEST(%s)", m.Request)
	case KindPrePrepare:
		return fmt.Sprintf("PRE-PREPARE(v=%d,n=%d,d=%s,req=%s)", m.View, m.Seq, m.Digest, m.Request)
	case KindPrepare:
		return fmt.Sprintf("PREPARE(v=%d,n=%d,d=%s,from=%s)", m.View, m.Seq, m.Digest, m.From)
	case KindCommit:
		return fmt.Sprintf("COMMIT(v=%d,n=%d,d=%s,from=%s)", m.View, m.Seq, m.Digest, m.From)
	case KindReply:
		return fmt.Sprintf("REPLY(v=%d,from=%s,result=%s)", m.View, m.From, m.Result)
	case KindViewChange:
		return fmt.Sprintf("VIEW-CHANGE(newView=%d,from=%s,lastExec=%d)", m.NewView, m.From, m.LastExecuted)
	case KindNewView:
		return fmt.Sprintf("NEW-VIEW(v=%d,from=%s,proofs=%v)", m.NewView, m.From, m.ViewChangeSet)
	default:
		return fmt.Sprintf("UNKNOWN(%s)", m.Kind)
	}
}
