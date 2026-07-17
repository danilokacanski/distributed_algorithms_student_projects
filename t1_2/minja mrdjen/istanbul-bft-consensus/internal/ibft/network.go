package ibft

import (
	"fmt"
	"sync"
)

type NetworkEnvelope struct {
	To  int
	Msg Message
}

type Network struct {
	Nodes        map[int]*Node
	NodeOrder    []int
	DelayedNodes map[int]bool

	queue []NetworkEnvelope

	mu sync.Mutex
	wg sync.WaitGroup
}

func NewNetwork() *Network {
	return &Network{
		Nodes:        make(map[int]*Node),
		NodeOrder:    []int{},
		DelayedNodes: make(map[int]bool),
		queue:        []NetworkEnvelope{},
	}
}

func (net *Network) AddNode(node *Node) {
	net.mu.Lock()
	defer net.mu.Unlock()

	net.Nodes[node.ID] = node
	net.NodeOrder = append(net.NodeOrder, node.ID)
}

func (net *Network) SetNodeDelayed(id int, delayed bool) {
	net.mu.Lock()
	net.DelayedNodes[id] = delayed
	net.mu.Unlock()

	if delayed {
		fmt.Printf("[MREZA] Node %d je privremeno odlozen: poruke ka njemu se ne dostavljaju\n", id)
	} else {
		fmt.Printf("[MREZA] Node %d vise nije odlozen: ponovo prima poruke\n", id)
	}
}

func (net *Network) Send(to int, msg Message) {
	net.mu.Lock()
	defer net.mu.Unlock()

	net.queue = append(net.queue, NetworkEnvelope{
		To:  to,
		Msg: msg,
	})
}

func (net *Network) BestEffortBroadcast(msg Message) {
	net.mu.Lock()
	nodeOrder := append([]int(nil), net.NodeOrder...)
	net.mu.Unlock()

	for _, id := range nodeOrder {
		net.Send(id, msg)
	}
}

func (net *Network) Broadcast(msg Message) {
	net.BestEffortBroadcast(msg)
}

func (net *Network) Run() {
	for {
		net.mu.Lock()

		if len(net.queue) == 0 {
			net.mu.Unlock()
			return
		}

		batch := net.queue
		net.queue = []NetworkEnvelope{}

		net.mu.Unlock()

		for _, envelope := range batch {
			net.deliver(envelope)
		}

		net.wg.Wait()
	}
}

func (net *Network) deliver(envelope NetworkEnvelope) {
	net.mu.Lock()

	node, exists := net.Nodes[envelope.To]
	delayed := net.DelayedNodes[envelope.To]

	net.mu.Unlock()

	msg := envelope.Msg

	if !exists {
		return
	}

	if delayed {
		fmt.Printf("\n[MREZA] %s od Node %d ka Node %d NIJE DOSTAVLJENA jer Node %d kasni | lambda=%d | round=%d | value=%s\n",
			msg.Type, msg.From, envelope.To, envelope.To, msg.Lambda, msg.Round, msg.Value)
		return
	}

	fmt.Printf("\n[MREZA] %s od Node %d ka Node %d | lambda=%d | round=%d | value=%s\n",
		msg.Type, msg.From, envelope.To, msg.Lambda, msg.Round, msg.Value)

	net.wg.Add(1)
	node.Inbox <- msg
}

func (net *Network) MessageDone() {
	net.wg.Done()
}
