package pbft

import (
	"fmt"

	"github.com/danilokacanski/da/week03_04_parallel/process"
)

// CheckResult drzi ishod jedne provere property-a
type CheckResult struct {
	Property string
	Passed   bool
	Details  string
}

func (c CheckResult) String() string {
	status := "PASS"
	if !c.Passed {
		status = "FAIL"
	}
	return fmt.Sprintf("[%s] %s: %s", status, c.Property, c.Details)
}

// CheckTotalOrderAgreement potvrdjuje da nijedne dve replike nisu izvrsile
// drugaciji digest ya isti sekvencni broj - safety property PBFTa: oblik
// linearizabilnosti (appendix b u radu). Sve korektne replike izvrsavaju
// istu sekvencnu zahteva, cak i u prisustvu bizantijskog primary-a
func CheckTotalOrderAgreement(s Snapshot) CheckResult {
	perSeq := make(map[int]string)
	for replica, log := range s.Executions {
		for _, e := range log {
			if existing, ok := perSeq[e.Seq]; ok {
				if existing != e.Digest {
					return CheckResult{"Agreement (total order)", false,
						fmt.Sprintf("replica %s executed digest %s at seq %d, but another replica executed %s",
							replica, e.Digest, e.Seq, existing)}
				}
			} else {
				perSeq[e.Seq] = e.Digest
			}
		}
	}
	return CheckResult{"Agreement (total order)", true,
		fmt.Sprintf("all replicas agree on the executed digest for every sequence number (%d checked)", len(perSeq))}
}

// CheckNoDoubleExecutions verifikuje da replika nije ne izvrsava isti sekvencni broj
// dva puta (tacno jedno izvrsavanje, sekcija 4.2)
func CheckNoDoubleExecution(s Snapshot) CheckResult {
	for replica, log := range s.Executions {
		seen := make(map[int]bool)
		for _, e := range log {
			if seen[e.Seq] {
				return CheckResult{"No double execution", false,
					fmt.Sprintf("replica %s executed sequence %d more than once", replica, e.Seq)}
			}
			seen[e.Seq] = true
		}
	}
	return CheckResult{"No double execution", true, "every replica executed each sequence number at most once"}
}

//CheckClientQuorum verifikuje da je klijent sakupio barem f+1 istih odgovora
// (sekcija 4.2: klijent ceka na slab sertifikat sa f+1 odgovora, pre nego sto
// prihvati rezultat r)

func CheckClientQuorum(s Snapshot, client process.ProcessID, n int) CheckResult {
	replies := s.ClientReplies[client]
	need := WeakQuorumSize(n)

	counts := make(map[string]int)
	for _, r := range replies {
		counts[r.Result]++
	}
	for result, c := range counts {
		if c >= need {
			return CheckResult{"Client quorum", true,
				fmt.Sprintf("client %s collected %d/%d matching replies (>= f+1=%d) for result %q",
					client, c, len(replies), need, result)}
		}
	}
	return CheckResult{"Client quorum", false,
		fmt.Sprintf("client %s never collected f+1=%d matching replies (got %d total, by-result=%v)",
			client, need, len(replies), counts)}
}

//CheckViewProgress verifikuje da, kad god je barem jedna replika trebala da inicijalizuje
// view-change, sistem eventualno dostize visi view - liveness deo
// view-change protokola (sekcija 4.5.1)

func CheckViewProgress(s Snapshot, allIDs []process.ProcessID) CheckResult {
	anyViewChange := false
	for _, vc := range s.ViewChanges {
		if vc > 0 {
			anyViewChange = true
			break
		}
	}
	if !anyViewChange {
		return CheckResult{"View progress", true, "no view change was needed in this scenario"}
	}
	maxView := 0
	for _, v := range s.FinalView {
		if v > maxView {
			maxView = v
		}
	}
	if maxView == 0 {
		return CheckResult{"View progress", false, "a view change was initiated but no replica ever advanced past view 0"}
	}
	return CheckResult{"View progress", true,
		fmt.Sprintf("system advanced to view %d after a view change was triggered", maxView)}
}

//RunAllChecks izvrsava svaku relevantnu proveru i ispisuje rezultate.
// n je totalan broj replika, klijent je proveravan za svoj kvorum (prosledi "" za preskakanje)

func RunAllChecks(s Snapshot, allIDs []process.ProcessID, n int, client process.ProcessID) {
	fmt.Println("\n--- PBFT Property Checks ---")
	results := []CheckResult{
		CheckTotalOrderAgreement(s),
		CheckNoDoubleExecution(s),
		CheckViewProgress(s, allIDs),
	}
	if client != "" {
		results = append(results, CheckClientQuorum(s, client, n))
	}
	allPassed := true
	for _, r := range results {
		fmt.Println(" ", r)
		if !r.Passed {
			allPassed = false
		}
	}
	if allPassed {
		fmt.Println("\n  All properties: PASS")
	} else {
		fmt.Println("\n  Some properties: FAIL")
	}
	fmt.Println("-----------------------------")
}
