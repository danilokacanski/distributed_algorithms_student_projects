package ibft

type HighestPreparedResult struct {
	PreparedRound int
	PreparedValue string
	Found         bool
}

func HighestPrepared(roundChangeMessages map[int]Message) HighestPreparedResult {
	result := HighestPreparedResult{
		PreparedRound: NoPreparedRound,
		PreparedValue: NoPreparedValue,
		Found:         false,
	}

	for _, msg := range roundChangeMessages {
		if msg.PreparedRound == NoPreparedRound || msg.PreparedValue == NoPreparedValue {
			continue
		}

		if !result.Found || msg.PreparedRound > result.PreparedRound {
			result.PreparedRound = msg.PreparedRound
			result.PreparedValue = msg.PreparedValue
			result.Found = true
		}
	}

	return result
}

func JustifyRoundChange(qrc map[int]Message, lambda int, round int, validatorCount int) bool {
	if len(qrc) < QuorumSize(validatorCount) {
		return false
	}

	allWithoutPreparedValue := true

	for _, msg := range qrc {
		if !IsValidMessage(msg, validatorsFromCount(validatorCount)) {
			return false
		}

		if msg.Type != RoundChange {
			return false
		}

		if msg.Lambda != lambda || msg.Round != round {
			return false
		}

		if msg.PreparedRound != NoPreparedRound && msg.PreparedRound >= msg.Round {
			return false
		}

		hasPreparedRound := msg.PreparedRound != NoPreparedRound
		hasPreparedValue := msg.PreparedValue != NoPreparedValue

		if hasPreparedRound != hasPreparedValue {
			return false
		}

		if hasPreparedRound && hasPreparedValue {
			allWithoutPreparedValue = false
		}
	}

	if allWithoutPreparedValue {
		return true
	}

	highestPrepared := HighestPrepared(qrc)
	if !highestPrepared.Found {
		return false
	}

	return hasPrepareQuorumForHighestPrepared(
		qrc,
		lambda,
		highestPrepared.PreparedRound,
		highestPrepared.PreparedValue,
		validatorCount,
	)
}

func hasPrepareQuorumForHighestPrepared(
	qrc map[int]Message,
	lambda int,
	preparedRound int,
	preparedValue string,
	validatorCount int,
) bool {
	distinctPrepareSenders := make(map[int]bool)

	for _, roundChangeMsg := range qrc {
		for _, prepareMsg := range roundChangeMsg.Justification {
			if !IsValidMessage(prepareMsg, validatorsFromCount(validatorCount)) {
				continue
			}

			if prepareMsg.Type != Prepare {
				continue
			}

			if prepareMsg.Lambda != lambda {
				continue
			}

			if prepareMsg.Round != preparedRound {
				continue
			}

			if prepareMsg.Value != preparedValue {
				continue
			}

			distinctPrepareSenders[prepareMsg.From] = true
		}
	}

	return len(distinctPrepareSenders) >= QuorumSize(validatorCount)
}

func (n *Node) proposalValueForRound(round int, fallbackValue string) (string, HighestPreparedResult, bool) {
	if fallbackValue == "" {
		fallbackValue = n.InputValue
	}

	if round == 1 {
		return fallbackValue, HighestPreparedResult{}, true
	}

	qrc := n.RoundChangeMessages[round]

	if len(qrc) < QuorumSize(len(n.Validators)) {
		return fallbackValue, HighestPreparedResult{}, false
	}

	if !JustifyRoundChange(qrc, n.Lambda, round, len(n.Validators)) {
		return fallbackValue, HighestPreparedResult{}, false
	}

	highestPrepared := HighestPrepared(qrc)

	if highestPrepared.Found {
		return highestPrepared.PreparedValue, highestPrepared, true
	}

	return fallbackValue, highestPrepared, true
}

func roundChangeMessagesToSlice(qrc map[int]Message) []Message {
	messages := make([]Message, 0, len(qrc))

	for _, msg := range qrc {
		messages = append(messages, msg)
	}

	return messages
}

func roundChangeMessagesFromJustification(justification []Message) map[int]Message {
	qrc := make(map[int]Message)

	for _, msg := range justification {
		if msg.Type != RoundChange {
			continue
		}

		qrc[msg.From] = msg
	}

	return qrc
}

func JustifyPrePrepare(msg Message, validatorCount int) bool {
	if msg.Type != PrePrepare {
		return false
	}

	if msg.Round == 1 {
		return true
	}

	qrc := roundChangeMessagesFromJustification(msg.Justification)

	if len(qrc) < QuorumSize(validatorCount) {
		return false
	}

	if !JustifyRoundChange(qrc, msg.Lambda, msg.Round, validatorCount) {
		return false
	}

	highestPrepared := HighestPrepared(qrc)

	if highestPrepared.Found {
		return msg.Value == highestPrepared.PreparedValue
	}

	return true
}
