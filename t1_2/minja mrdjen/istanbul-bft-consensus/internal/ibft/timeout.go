package ibft

import "fmt"

func TimeoutForRound(round int) int {
	if round < 1 {
		return 1
	}

	return 1 << (round - 1)
}

func (n *Node) StartTimer() {
	if n.Decided {
		return
	}

	n.TimerState = TimerRunning

	fmt.Printf("Node %d pokrece timer za round %d: t(r)=%d\n",
		n.ID, n.CurrentRound, TimeoutForRound(n.CurrentRound))
}

func (n *Node) StopTimer() {
	n.TimerState = TimerStopped

	fmt.Printf("Node %d zaustavlja timer\n", n.ID)
}

func (n *Node) ExpireTimer(net *Network) {
	n.OnTimeout(net)
}

func (n *Node) OnTimeout(net *Network) {
	if n.ByzantineMode != ByzantineNone {
		fmt.Printf("Node %d je vizantijski (%s), timeout se ignorise\n",
			n.ID, n.ByzantineMode)
		return
	}

	if n.Decided {
		return
	}

	oldRound := n.CurrentRound
	nextRound := oldRound + 1

	n.TimerState = TimerExpired

	fmt.Printf("TIMER istekao na Node %d u round %d. Prelazi se u round %d\n",
		n.ID, oldRound, nextRound)

	n.enterRound(nextRound)
	n.broadcastRoundChange(nextRound, net)
}
