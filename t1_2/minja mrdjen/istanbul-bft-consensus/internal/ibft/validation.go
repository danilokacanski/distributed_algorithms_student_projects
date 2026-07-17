package ibft

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
)

var (
	publicKeysMu sync.RWMutex
	publicKeys   = make(map[int]ed25519.PublicKey)
)

func RegisterPublicKey(nodeID int, publicKey ed25519.PublicKey) {
	publicKeysMu.Lock()
	defer publicKeysMu.Unlock()

	publicKeys[nodeID] = publicKey
}

func PublicKeyFor(nodeID int) (ed25519.PublicKey, bool) {
	publicKeysMu.RLock()
	defer publicKeysMu.RUnlock()

	publicKey, exists := publicKeys[nodeID]
	return publicKey, exists
}

func Beta(value string) bool {
	if value == "" {
		return false
	}

	return !strings.Contains(value, "INVALID")
}

func IsValidator(id int, validators []int) bool {
	for _, validatorID := range validators {
		if validatorID == id {
			return true
		}
	}

	return false
}

func messagePayload(msg Message) []byte {
	payload := fmt.Sprintf(
		"%s|from=%d|lambda=%d|round=%d|value=%s|pr=%d|pv=%s",
		msg.Type,
		msg.From,
		msg.Lambda,
		msg.Round,
		msg.Value,
		msg.PreparedRound,
		msg.PreparedValue,
	)

	for _, justificationMsg := range msg.Justification {
		payload += fmt.Sprintf(
			"|j:%s:%d:%d:%d:%s:%d:%s:%s",
			justificationMsg.Type,
			justificationMsg.From,
			justificationMsg.Lambda,
			justificationMsg.Round,
			justificationMsg.Value,
			justificationMsg.PreparedRound,
			justificationMsg.PreparedValue,
			justificationMsg.Signature,
		)
	}

	return []byte(payload)
}

func SignMessage(msg Message, privateKey ed25519.PrivateKey) string {
	signature := ed25519.Sign(privateKey, messagePayload(msg))
	return base64.StdEncoding.EncodeToString(signature)
}

func VerifySignature(msg Message) bool {
	publicKey, exists := PublicKeyFor(msg.From)
	if !exists {
		return false
	}

	signature, err := base64.StdEncoding.DecodeString(msg.Signature)
	if err != nil {
		return false
	}

	return ed25519.Verify(publicKey, messagePayload(msg), signature)
}

func IsValidMessage(msg Message, validators []int) bool {
	if !IsValidator(msg.From, validators) {
		return false
	}

	if !VerifySignature(msg) {
		return false
	}

	if msg.Lambda <= 0 {
		return false
	}

	if msg.Round < 1 {
		return false
	}

	switch msg.Type {
	case PrePrepare, Prepare, Commit:
		return Beta(msg.Value)

	case RoundChange:
		if msg.PreparedRound != NoPreparedRound && msg.PreparedRound >= msg.Round {
			return false
		}

		hasPreparedRound := msg.PreparedRound != NoPreparedRound
		hasPreparedValue := msg.PreparedValue != NoPreparedValue

		if hasPreparedRound != hasPreparedValue {
			return false
		}

		if hasPreparedValue && !Beta(msg.PreparedValue) {
			return false
		}

		return true

	case DecisionCertificate:
		if msg.Value == "" {
			return false
		}

		return Beta(msg.Value)

	default:
		return false
	}
}

func validatorsFromCount(n int) []int {
	validators := make([]int, n)

	for i := 0; i < n; i++ {
		validators[i] = i
	}

	return validators
}
