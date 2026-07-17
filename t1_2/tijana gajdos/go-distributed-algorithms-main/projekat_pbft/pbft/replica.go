package pbft

import (
	"context"
	"fmt"
	"time"

	"github.com/danilokacanski/da/week03_04_parallel/process"
)

// ============================================================================
// REPLIKA LOG UNOS
// ============================================================================

// logEntry prati sve sto replika zna o jednom sekvencnom broju:
// da li ima pre-prepared/prepared/commited, i ko je glasao

type logEntry struct {
	view    int
	seq     int
	digest  string
	request ClientRequest

	prePrepared   bool
	prepares      map[process.ProcessID]bool
	commits       map[process.ProcessID]bool
	preparedFired bool // COMMIT already broadcast for this entry?
	committed     bool
	executed      bool
}

func newLogEntry(view, seq int, digest string) *logEntry {
	return &logEntry{
		view: view, seq: seq, digest: digest,
		prepares: make(map[process.ProcessID]bool),
		commits:  make(map[process.ProcessID]bool),
	}
}

// ============================================================================
// REPLICA NODE
// ============================================================================

//ReplicaNode je jedna PBFT replika. Implementira process.Process.

//Pojednostavljenje za samo-glasanje: umesto da samo rezerve salju PREPARE
// i primary samo salje PRE-PREPARE (kao u radu), ovde SVAKA replika ukljucujuci
// primary-a odmah snima svoj PREPARE glas momenta kada prihvati/stvori zahtev,
// primary dodatno broadcast-uje eksplicitnu PREPARE poruku uz PRE-PREPARE.
// Ovo odrzava 'prepared' podatke simetricne kroz sve replike a i dalje zahtevajuci
// pun 2f+1 kvorum (sekcija 4.3) pre nego sto se COMMIT salje / ne menja safety dogovor

type ReplicaNode struct {
	id     process.ProcessID
	allIDs []process.ProcessID //stabilno, identican red na svakoj replici
	n      int
	f      int

	recorder *Recorder

	//Equivocate cini ovu repliku da se ponasa kao bizantijski primary:
	// kad god je ona primary, salje dve razlicite PRE-PREPARE poruke
	// (drugaciji digest) za klijentov zahtev dvoma razdvojenim grupama
	// replika umesto jedne konzistentne PRE-PREPARE.

	Equivocate bool

	//ViewChangeTimeout je koliko dugo replika ceka poslati zahtev da
	// uradi napredak pre nego sto posumnja na primary i zapocne view change (sekcija 4.5)

	ViewChangeTimeout time.Duration
}

func NewReplicaNode(id process.ProcessID, allIDs []process.ProcessID, recorder *Recorder, timeout time.Duration) *ReplicaNode {
	return &ReplicaNode{
		id: id, allIDs: allIDs, n: len(allIDs), f: MaxFaulty(len(allIDs)),
		recorder: recorder, ViewChangeTimeout: timeout,
	}
}

func (r *ReplicaNode) ID() process.ProcessID { return r.id }

//runState drzi SVE promenljive po-izvrsavanju stanja za jednu repliku
// Time sto je struct, omogucava message handlers da budu u razlicitim
// fajlovima i dalje deleci stanje direktno - nisu potrebni lockovi jer
// jedino ova gorutina ikada dodiruje runState vrednost

type runState struct {
	node *ReplicaNode
	send func(process.Message)

	view   int
	active bool //false dok se izvrsava view change

	nextSeq      int //sledeci sekvencni broj koji OVA replika zadaje kao primary
	lastExecuted int
	logs         map[int]*logEntry

	pendingRequest *ClientRequest //zahtev koji trenutno cekamo da bude commited

	viewChangeVotes map[int]map[process.ProcessID]bool // newView -> distinct senders

	timer *time.Timer // view-change tajmer sumnje
}

func (rs *runState) resetTimer(d time.Duration) {
	if !rs.timer.Stop() {
		select {
		case <-rs.timer.C:
		default:
		}
	}
	rs.timer.Reset(d)
}

func (rs *runState) primary() process.ProcessID {
	return PrimaryFor(rs.view, rs.node.allIDs)
}

func (rs *runState) isPrimary() bool {
	return rs.primary() == rs.node.id
}

// Run implementira process.Process.
func (r *ReplicaNode) Run(ctx context.Context, inbox <-chan process.Message, send func(process.Message)) {
	timer := time.NewTimer(r.ViewChangeTimeout)
	if !timer.Stop() {
		<-timer.C
	}
	// Tajmer zaustavljen: pokrece se jedino kada imamo zatrazen
	// zahtev na koji cekamo (handleClientRequest / handlePrePrepare)

	rs := &runState{
		node:            r,
		send:            send,
		view:            0,
		active:          true,
		nextSeq:         1,
		lastExecuted:    0,
		logs:            make(map[int]*logEntry),
		viewChangeVotes: make(map[int]map[process.ProcessID]bool),
		timer:           timer,
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-rs.timer.C:
			rs.onTimeout()

		case msg, ok := <-inbox:
			if !ok {
				return
			}
			cm, ok := msg.Data.(PBFTMessage)
			if !ok {
				continue
			}
			switch cm.Kind {
			case KindClientRequest:
				rs.handleClientRequest(cm)
			case KindPrePrepare:
				rs.handlePrePrepare(cm)
			case KindPrepare:
				rs.handlePrepare(cm)
			case KindCommit:
				rs.handleCommit(cm)
			case KindViewChange:
				rs.handleViewChange(cm)
			case KindNewView:
				rs.handleNewView(cm)
			}
		}
	}
}

// ============================================================================
// NORMALAN SLUCAJ (Section 4.3)
// ============================================================================

// handleClientRequest procesuira dolazni klijentski REQUEST.
// Ako je ova replika aktivan primary trenutnog view-a, zadaje
// sledeci sekvencni broj i pocinje trofazni protokol

func (rs *runState) handleClientRequest(cm PBFTMessage) {
	req := cm.Request
	fmt.Printf("  [%s] Received REQUEST %s (view=%d, primary=%s)\n", rs.node.id, req, rs.view, rs.primary())

	// Ovaj pojednostavljeni simulator prati jedan zahtev
	// sto je dovoljno da se demonstrira pun protokol od pocetka do kraja
	// ukljucujuci view promenu. Koristi se i za pamcenje onoga sto treba
	// da se ponovo predlozi nakon view change (u viewchange.go)

	if rs.pendingRequest == nil {
		rs.pendingRequest = &req
	}

	if !rs.active {
		return // view change se desava, novi primary ce ponovo predloziti
	}

	if !rs.isPrimary() {
		// Ispravna replika koja nije primary samo ceka: posumnjace
		// na primary-a pri timeout-u ako ga ne prati PRE-PREPARE

		rs.resetTimer(rs.node.ViewChangeTimeout)
		return
	}

	// Ja sam sad primary: dodeljujem sekvencni broj i red zahteva
	seq := rs.nextSeq
	rs.nextSeq++
	digest := req.Digest()

	if rs.node.Equivocate {
		rs.sendEquivocatingPrePrepare(seq, req)
	} else {
		entry := newLogEntry(rs.view, seq, digest)
		entry.request = req
		entry.prePrepared = true
		entry.prepares[rs.node.id] = true // primary-ev sopstveni PREPARE glas

		rs.logs[seq] = entry

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

// handlePrePrepare procesuira PRE-PREPARE primary-a trenutnog view-a
func (rs *runState) handlePrePrepare(cm PBFTMessage) {
	if !rs.active || cm.View != rs.view {
		return
	}
	if cm.From != rs.primary() {
		return // ignorisi PRE-PREPARE od bilo koga sem trenutnog primary-a
	}

	entry, ok := rs.logs[cm.Seq]

	if ok && entry.view != cm.View && !entry.committed {
		ok = false //zastareo ulaz starijeg view-a
	}

	switch {
	case ok && entry.prePrepared && entry.digest != cm.Digest:
		// Drugi, konfliktujuci PRE-PREPARE za isti (view,seq): primary je
		// equivocating(okolisajuci). Sekcija 4.3: Ne prihvata se PRE-PREPARE
		// za isti view i sekvencnim brojem koji sadrzi drugaciji digest
		//  Cuva se prvi, a ovaj ignorise

		fmt.Printf("  [%s] *** Detected conflicting PRE-PREPARE for seq=%d (have %s, got %s) — ignoring, primary is equivocating! ***\n",
			rs.node.id, cm.Seq, entry.digest, cm.Digest)
		return
	case ok && !entry.prePrepared && entry.digest != "" && entry.digest != cm.Digest:
		// Ako sam do sad imala samo PREPARE glasove skupljene za drugaciji digest
		// (moguca dostava van reda): odbaci ih i prihvati digest koji je zapravo
		// nosen u primary-evom PRE-PREPARE

		entry.prepares = make(map[process.ProcessID]bool)
		entry.digest = cm.Digest
	case !ok:
		entry = newLogEntry(cm.View, cm.Seq, cm.Digest)
		rs.logs[cm.Seq] = entry
	}

	entry.digest = cm.Digest
	entry.request = cm.Request
	entry.prePrepared = true
	entry.prepares[rs.node.id] = true // PREPARE glas ove replike

	fmt.Printf("  [%s] Accepted PRE-PREPARE(v=%d,n=%d,d=%s) from primary %s\n",
		rs.node.id, cm.View, cm.Seq, cm.Digest, cm.From)

	others := except(rs.node.allIDs, rs.node.id)
	Broadcast(rs.send, rs.node.id, others, PBFTMessage{
		Kind: KindPrepare, View: rs.view, Seq: cm.Seq, Digest: cm.Digest, From: rs.node.id,
	})

	rs.resetTimer(rs.node.ViewChangeTimeout)
	rs.maybeCommit(cm.Seq)
}

// handlePrepare procesuira PREPARE glas od druge replike
func (rs *runState) handlePrepare(cm PBFTMessage) {
	if !rs.active || cm.View != rs.view {
		return
	}
	entry, ok := rs.logs[cm.Seq]

	if ok && entry.view != cm.View && !entry.committed {
		ok = false //zastareo ulaz starijeg view-a
	}

	if !ok {
		entry = newLogEntry(cm.View, cm.Seq, cm.Digest)
		rs.logs[cm.Seq] = entry
	}
	if entry.digest == "" {
		entry.digest = cm.Digest
	}
	if entry.digest != cm.Digest {
		return //glas za drugaciji digest od onog koji smo prihvatili - ignorisi
	}
	entry.prepares[cm.From] = true

	rs.resetTimer(rs.node.ViewChangeTimeout)
	rs.maybeCommit(cm.Seq)
}

//maybeCommit implementira 'prepared' iz Sekcije 4.3: PRE-PREPARE
// plus set 2f+1 velicine poklapajucih PREPARE glasova (lokalni
// samo-glas se racuna kao jedan od tih 2f+1). Kada se ispuni, COMMIT
// se broadcast-uje tacno jednom

func (rs *runState) maybeCommit(seq int) {
	entry := rs.logs[seq]
	if entry == nil || entry.preparedFired || !entry.prePrepared {
		return
	}
	if len(entry.prepares) < QuorumSize(rs.node.n) {
		return
	}
	entry.preparedFired = true
	entry.commits[rs.node.id] = true //samo-glas, isto kao za PREPARE

	fmt.Printf("  [%s] PREPARED seq=%d (d=%s) with %d/%d votes — broadcasting COMMIT\n",
		rs.node.id, seq, entry.digest, len(entry.prepares), rs.node.n)

	others := except(rs.node.allIDs, rs.node.id)
	Broadcast(rs.send, rs.node.id, others, PBFTMessage{
		Kind: KindCommit, View: rs.view, Seq: seq, Digest: entry.digest, From: rs.node.id,
	})

	rs.maybeExecute(seq)
}

// handleCommitprocesuira COMMIT glas od druge replike
func (rs *runState) handleCommit(cm PBFTMessage) {
	if !rs.active || cm.View != rs.view {
		return
	}
	entry, ok := rs.logs[cm.Seq]

	if ok && entry.view != cm.View && !entry.committed {
		ok = false // zastareo ulaz starijeg view-a
	}

	if !ok {
		entry = newLogEntry(cm.View, cm.Seq, cm.Digest)
		rs.logs[cm.Seq] = entry
	}
	if entry.digest == "" {
		entry.digest = cm.Digest
	}
	if entry.digest != cm.Digest {
		return
	}
	entry.commits[cm.From] = true

	rs.resetTimer(rs.node.ViewChangeTimeout)
	rs.maybeExecute(cm.Seq)
}

//maybeExecute implementira 'committed' svojstvo (2f+1 istih COMMIT
// poruka, Sekcija 4.3) i izvrsava zahteve iskljucivo u sekvencnom redu
// potrebno za replikaciju state machine bezbednost (Svaka replika i
// izvrsava operaciju zatrazenu od klijenta kada je m commited i
// replika je izvrsila sve zahteve za nizim sekvencnim brojem)

func (rs *runState) maybeExecute(seq int) {
	entry := rs.logs[seq]
	//Replika izvrsava samo zahteve koje je lokalno pre-prepared
	// (mora da zna sadrzaj zahteva) i samo jednom
	if entry == nil || entry.committed || !entry.prePrepared {
		return
	}
	if len(entry.commits) < QuorumSize(rs.node.n) {
		return
	}
	entry.committed = true

	for {
		next := rs.lastExecuted + 1
		e, ok := rs.logs[next]
		if !ok || !e.committed || e.executed {
			break
		}
		e.executed = true
		rs.lastExecuted = next

		result := fmt.Sprintf("%s#%d", e.request.Op, next)
		fmt.Printf("  [%s] *** EXECUTED seq=%d op=%s -> result=%s ***\n", rs.node.id, next, e.request.Op, result)

		rs.node.recorder.RecordExecution(rs.node.id, ExecutionEntry{Seq: next, Digest: e.digest, Op: e.request.Op})

		rs.send(process.NewMessage(rs.node.id, e.request.ClientID, PBFTType, PBFTMessage{
			Kind: KindReply, View: rs.view, From: rs.node.id, Result: result,
		}))

		if rs.pendingRequest != nil && rs.pendingRequest.Digest() == e.digest {
			rs.pendingRequest = nil //nema vise na sta da se ceka
		}
	}
}
