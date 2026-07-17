package ibft

func Leader(lambda int, round int, validators []int) int {
	if len(validators) == 0 {
		return -1
	}

	index := (lambda + round - 2) % len(validators)

	if index < 0 {
		index += len(validators)
	}

	return validators[index]
}

func LeaderForRound(round int, validators []int) int {
	return Leader(1, round, validators)
}
