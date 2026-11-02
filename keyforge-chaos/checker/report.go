package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

// The overall verdict: not linearizable if any key is, else unknown if the checker gave up on any key.
const (
	linearizable    = "LINEARIZABLE"
	notLinearizable = "NOT LINEARIZABLE"
	unknown         = "UNKNOWN"
)

type report struct {
	History        string            `json:"history"`
	Started        string            `json:"started"`
	Metadata       map[string]string `json:"metadata"`
	Verdict        string            `json:"verdict"`
	Operations     map[string]counts `json:"operations"`
	Faults         []fault           `json:"faults"`
	Keys           int               `json:"keys"`
	Checked        int               `json:"checked_operations"`
	Ok             int               `json:"ok_operations"`
	Indeterminate  int               `json:"indeterminate_writes"`
	DroppedFailed  int               `json:"dropped_failed"`
	DroppedReads   int               `json:"dropped_indeterminate_reads"`
	DroppedSets    int               `json:"dropped_unobserved_sets"`
	CheckSeconds   float64           `json:"check_seconds"`
	TimeoutSeconds float64           `json:"timeout_seconds_per_key"`
	Problems       []keyResult       `json:"problem_keys"`
	Visualization  string            `json:"visualization,omitempty"`
}

type counts struct {
	Ok   int `json:"ok"`
	Fail int `json:"fail"`
	Info int `json:"info"`
}

func verdictOf(results []keyResult) (string, []keyResult) {
	verdict := linearizable
	var problems []keyResult
	for _, r := range results {
		switch r.Result {
		case porcupine.Illegal:
			verdict = notLinearizable
			problems = append(problems, r)
		case porcupine.Unknown:
			if verdict == linearizable {
				verdict = unknown
			}
			problems = append(problems, r)
		}
	}
	// Keys without a linearization first.
	sort.SliceStable(problems, func(i, j int) bool {
		return problems[i].Result == porcupine.Illegal && problems[j].Result != porcupine.Illegal
	})
	return verdict, problems
}

func operationCounts(h *history) map[string]counts {
	result := map[string]counts{}
	for _, op := range h.Ops {
		c := result[op.F]
		switch op.Type {
		case "ok":
			c.Ok++
		case "fail":
			c.Fail++
		default:
			c.Info++
		}
		result[op.F] = c
	}
	return result
}

func (r *report) text() string {
	var b strings.Builder
	b.WriteString("KeyForge linearizability check (Porcupine)\n")
	fmt.Fprintf(&b, "history:       %s, started %s\n", r.History, r.Started)
	var functions []string
	for _, f := range []string{"get", "set", "incr"} {
		c := r.Operations[f]
		functions = append(functions, fmt.Sprintf("%s %d ok / %d fail / %d info", f, c.Ok, c.Fail, c.Info))
	}
	fmt.Fprintf(&b, "operations:    %s\n", strings.Join(functions, "; "))
	byKind := map[string]int{}
	var kinds []string
	for _, f := range r.Faults {
		if byKind[f.Kind] == 0 {
			kinds = append(kinds, f.Kind)
		}
		byKind[f.Kind]++
	}
	var faultSummary []string
	for _, kind := range kinds {
		faultSummary = append(faultSummary, fmt.Sprintf("%s x%d", kind, byKind[kind]))
	}
	if len(faultSummary) == 0 {
		faultSummary = append(faultSummary, "none")
	}
	fmt.Fprintf(&b, "nemesis:       %d faults: %s\n", len(r.Faults), strings.Join(faultSummary, ", "))
	fmt.Fprintf(&b, "checked:       %d keys, %d operations: %d completed, %d indeterminate writes that may or may not "+
		"have taken effect\n", r.Keys, r.Checked, r.Ok, r.Indeterminate)
	fmt.Fprintf(&b, "left out:      %d failed operations (no effect), %d indeterminate reads (no constraint) and %d "+
		"indeterminate sets whose value nobody read (no observable effect)\n", r.DroppedFailed, r.DroppedReads,
		r.DroppedSets)
	fmt.Fprintf(&b, "checker time:  %.1f s (at most %.0f s per key)\n\n", r.CheckSeconds, r.TimeoutSeconds)
	switch r.Verdict {
	case linearizable:
		b.WriteString("RESULT: LINEARIZABLE\n")
	case notLinearizable:
		illegal := 0
		for _, p := range r.Problems {
			if p.Result == porcupine.Illegal {
				illegal++
			}
		}
		fmt.Fprintf(&b, "RESULT: NOT LINEARIZABLE (%d of %d keys have no linearization)\n", illegal, r.Keys)
	default:
		fmt.Fprintf(&b, "RESULT: UNKNOWN (the checker ran out of time on %d keys; no violation found)\n", len(r.Problems))
	}
	for _, p := range r.Problems {
		fmt.Fprintf(&b, "  %s (%d operations): %s", p.Key, p.Operations, strings.ToLower(string(p.Result)))
		if p.Explanation != "" {
			fmt.Fprintf(&b, ": %s", p.Explanation)
		}
		b.WriteString("\n")
	}
	if r.Visualization != "" {
		fmt.Fprintf(&b, "visualization: %s (open it in a browser)\n", r.Visualization)
	}
	return b.String()
}

func (r *report) write(textPath, jsonPath string) error {
	if err := os.WriteFile(textPath, []byte(r.text()), 0o644); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(jsonPath, append(encoded, '\n'), 0o644)
}
