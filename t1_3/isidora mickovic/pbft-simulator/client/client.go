package client

import (
	"fmt"
	"time"

	"pbft-simulator/message"
)

type Client struct {
	ID        int
	Peers     []chan message.Message
	Inbox     chan message.Message
	F         int
	lastReqTS int
}

func NewClient(id int, peers []chan message.Message, f int) *Client {
	return &Client{
		ID:    id,
		Peers: peers,
		Inbox: make(chan message.Message, 100),
		F:     f,
	}
}

func (c *Client) broadcast(msg message.Message) {
	for _, peerCh := range c.Peers {
		select {
		case peerCh <- msg:
		default:
			fmt.Printf("[Client %d] upozorenje: kanal ka replici je pun, poruka odbačena\n", c.ID)
		}
	}
}

func (c *Client) SendRequest(operation string, timeout time.Duration, maxRetries int) {
	c.lastReqTS++
	ts := c.lastReqTS

	req := message.Message{
		Type:      message.Request,
		SenderID:  c.ID,
		Payload:   operation,
		Digest:    operation,
		Timestamp: ts,
	}

	replies := make(map[int]message.Message) 

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt == 0 {
			fmt.Printf("[Client %d] šalje REQUEST svim replikama: %q (timestamp %d)\n", c.ID, operation, ts)
		} else {
			fmt.Printf("[Client %d] RETRANSMITUJE REQUEST (pokušaj %d): %q\n", c.ID, attempt, operation)
		}
		c.broadcast(req)

		deadline := time.After(timeout)
	waitLoop:
		for {
			select {
			case msg := <-c.Inbox:
				if msg.Type != message.Reply || msg.Timestamp != ts {
					continue
				}
				replies[msg.SenderID] = msg

				counts := make(map[string]int)
				for _, r := range replies {
					counts[r.Payload]++
				}
				for result, cnt := range counts {
					if cnt >= c.F+1 {
						fmt.Printf("[Client %d] dobio f+1 poklapajućih REPLY (%q) - zahtev prihvaćen\n", c.ID, result)
						return
					}
				}
			case <-deadline:
				break waitLoop
			}
		}
	}

	fmt.Printf("[Client %d] nije dobio f+1 poklapajućih odgovora nakon %d pokušaja - odustaje\n", c.ID, maxRetries)
}