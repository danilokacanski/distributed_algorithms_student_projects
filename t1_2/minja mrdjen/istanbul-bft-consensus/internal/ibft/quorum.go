package ibft

func MaxFaultyNodes(n int) int {
	return (n - 1) / 3
}

func QuorumSize(n int) int {
	f := MaxFaultyNodes(n)
	return ((n + f) / 2) + 1
}

func RoundChangeSetSize(n int) int {
	return MaxFaultyNodes(n) + 1
}
