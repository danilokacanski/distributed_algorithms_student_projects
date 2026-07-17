package pbft

import (
	"context"
	"fmt"
	"time"

	"github.com/danilokacanski/da/week03_04_parallel/process"
)

//ClientNode simulira PBFT klijenta (Sekcija 4.2): salje zahtev i ceka na
// slab sertifikat od f+1 istih odgovora pre prihvatanja rezultata

// Pojednostavljenje: ovaj klijent salje zahtev direktno svim replikama unapred
// (umesto jedino primary-u, broadcast-ujuci svima samo na timeout, kao u radu).
// Ovo primer cini kracim dok i dalje primenjuje trofazni protokol i f+1 odgovora
// klijentsko kvorum pravilo - ne oslabljuje garanciju bezbednosti na koju se klijent oslanja

type ClientNode struct {
	id       process.ProcessID
	allIDs   []process.ProcessID // ID replika kojima treba da se posalje zahtev
	request  ClientRequest
	recorder *Recorder
	needed   int //f+1 istih odgovora potrebno (WeakQuorumSize)
}

func NewClientNode(id process.ProcessID, replicaIDs []process.ProcessID, request ClientRequest, recorder *Recorder) *ClientNode {
	return &ClientNode{
		id: id, allIDs: replicaIDs, request: request, recorder: recorder,
		needed: WeakQuorumSize(len(replicaIDs)),
	}
}

func (c *ClientNode) ID() process.ProcessID { return c.id }

func (c *ClientNode) Run(ctx context.Context, inbox <-chan process.Message, send func(process.Message)) {
	fmt.Printf("  [%s] Sending %s to all %d replicas\n", c.id, c.request, len(c.allIDs))
	Broadcast(send, c.id, c.allIDs, PBFTMessage{Kind: KindClientRequest, Request: c.request, From: c.id})

	counts := make(map[string]int)
	done := false

	//Periodicna retrasmisija: ako je primary spor/pao/bizantijski,
	// nastavi ponovno slanje zahteva svim replikama tako da bi
	// eventualno novi primary isto naucio ovo (kopira klijentsku
	// retransmisiju-pri-timeoutu ponasanje, sekcija 4.2)

	retransmit := time.NewTicker(300 * time.Millisecond)
	defer retransmit.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-retransmit.C:
			if !done {
				Broadcast(send, c.id, c.allIDs, PBFTMessage{Kind: KindClientRequest, Request: c.request, From: c.id})
			}

		case msg, ok := <-inbox:
			if !ok {
				return
			}
			cm, ok := msg.Data.(PBFTMessage)
			if !ok || cm.Kind != KindReply {
				continue
			}
			c.recorder.RecordClientReply(c.id, cm)
			counts[cm.Result]++
			fmt.Printf("  [%s] Received REPLY from %s: result=%q (%d/%d matching)\n",
				c.id, msg.From, cm.Result, counts[cm.Result], c.needed)

			if !done && counts[cm.Result] >= c.needed {
				done = true
				fmt.Printf("  [%s] *** ACCEPTED result %q — collected f+1=%d matching replies ***\n",
					c.id, cm.Result, c.needed)
			}
		}
	}
}
