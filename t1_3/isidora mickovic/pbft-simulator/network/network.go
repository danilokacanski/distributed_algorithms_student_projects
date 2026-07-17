package network

import (
	"pbft-simulator/message"
	"pbft-simulator/node"
)

type Network struct {
	Nodes []*node.Node
	Channels []chan message.Message
}

func NewNetwork(totalNodes, f int, faults map[int]node.FaultType) *Network {
	channels := make([]chan message.Message, totalNodes)
	for i := range channels {
		channels[i] = make(chan message.Message, 100)
	}

	nodes := make([]*node.Node, totalNodes)
	for i := 0; i < totalNodes; i++ {
		fault := faults[i] 
		n := node.NewNode(i, totalNodes, f, fault)
		n.Inbox = channels[i]
		nodes[i] = n
	}

	// svaki čvor dobija listu kanala svih ostalih (uključujući svoj, radi lakšeg indeksiranja po ID-ju)
	for _, n := range nodes {
		n.Peers = channels
	}

	return &Network{Nodes: nodes, Channels: channels}
}

func (net *Network) Start() {
	for _, n := range net.Nodes {
		go n.Run()
	}
}

func (net *Network) Stop() {
	for _, n := range net.Nodes {
		select {
		case <-n.StopSignal():
			
		default:
			n.Stop()
		}
	}
}

func (net *Network) RegisterClient(clientID int, ch chan message.Message) {
	for _, n := range net.Nodes {
		n.RegisterClientChannel(clientID, ch)
	}
}

