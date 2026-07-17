package pbft

import "github.com/danilokacanski/da/week03_04_parallel/process"

//except vraca allIDs bez id - koristi se kada replika mora da posalje
// 'svim DRUGIM replikama'
func except(allIDs []process.ProcessID, id process.ProcessID) []process.ProcessID {
	out := make([]process.ProcessID, 0, len(allIDs)-1)
	for _, x := range allIDs {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}
