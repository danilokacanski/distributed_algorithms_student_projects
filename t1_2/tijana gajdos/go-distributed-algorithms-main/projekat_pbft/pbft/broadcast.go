package pbft

import "github.com/danilokacanski/da/week03_04_parallel/process"

//Broadcast salje payload svakom ID u 'to'. Imitira multicast iz rada
// za PRE-PREPARE/PREPARE/COMMIT/VIEW-CHANGE/NEW-VIEW

func Broadcast(send func(process.Message), from process.ProcessID, to []process.ProcessID, payload PBFTMessage) {
	for _, id := range to {
		send(process.NewMessage(from, id, PBFTType, payload))
	}
}

//EquivocatingBroadcast se koristi samo kod bizantijskog primary-a
// da bi pokazao PBFT-ovu izdrzljivost: salje payloadA 'groupA' destinacijama,
// i payloadB GroupB destinacijama. npr. dve razlicite PRE-PREPARE poruke
// istog (view, seq) sa razlicitim digestom. PBFT-ov 'prepared certificate'
// garantuje da ovo nikad nece izazvati dva razlicita zahteva za commit
// na istom sekvencnom broju.

func EquivocatingBroadcast(
	send func(process.Message),
	from process.ProcessID,
	groupA []process.ProcessID, payloadA PBFTMessage,
	groupB []process.ProcessID, payloadB PBFTMessage,
) {
	for _, id := range groupA {
		send(process.NewMessage(from, id, PBFTType, payloadA))
	}
	for _, id := range groupB {
		send(process.NewMessage(from, id, PBFTType, payloadB))
	}
}
