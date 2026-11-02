package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// conversion is a history in Porcupine's terms, one operation list per key.
type conversion struct {
	PerKey map[string][]porcupine.Operation
	Keys   []string // sorted
	// What went in: completed operations, and indeterminate writes that may or may not have taken effect.
	Ok, IndeterminateWrites int
	// What was left out: failed operations never took effect, an indeterminate read constrains nothing, and an
	// indeterminate set whose value nobody observed changes nothing that was observed (see unobserved).
	DroppedFailed, DroppedIndeterminateReads, DroppedUnobservedSets int
}

// toPorcupine turns operations into Porcupine operations. An indeterminate write may have taken effect at any time
// after its invocation, even after every other operation, so it returns after the end of the history and accepts any
// result: Porcupine may then place it anywhere from its invocation on, including after everything else, which is
// the same as it never taking effect.
//
// Two reductions keep keys with many indeterminate writes checkable without changing any verdict: unobserved
// indeterminate sets are left out (unobservedSets), and indeterminate incrs must take effect in invocation order
// (kvState.IndeterminateIncrs).
func toPorcupine(h *history) conversion {
	return convert(h, unobservedSets(h), true)
}

// convert does the work of toPorcupine, leaving out the operations marked in skip and numbering indeterminate incrs
// if orderIncrs is set.
func convert(h *history, skip map[int]bool, orderIncrs bool) conversion {
	c := conversion{PerKey: map[string][]porcupine.Operation{}}
	clients := map[string]map[int]int{} // key -> process -> dense client id, for the visualization's rows
	indeterminateIncrs := map[string]int{}
	// Operations are in invocation order, so numbering them as they come numbers them by invocation.
	for i, op := range h.Ops {
		if op.Type == "fail" {
			c.DroppedFailed++
			continue
		}
		if op.Type == "info" && op.F == "get" {
			c.DroppedIndeterminateReads++
			continue
		}
		if skip[i] {
			c.DroppedUnobservedSets++
			continue
		}
		in := kvInput{Key: op.Key, Op: op.F}
		if op.F == "set" {
			in.Value = *op.Value
		}
		if orderIncrs && op.F == "incr" && op.Type == "info" {
			indeterminateIncrs[op.Key]++
			in.Order = indeterminateIncrs[op.Key]
		}
		var out kvOutput
		returned := op.Return
		if op.Type == "info" {
			out.Unknown = true
			returned = h.End + 1
			c.IndeterminateWrites++
		} else {
			c.Ok++
			if op.Value != nil && op.F != "set" {
				out.Exists = true
				out.Value = *op.Value
			}
		}
		ids, seen := clients[op.Key]
		if !seen {
			ids = map[int]int{}
			clients[op.Key] = ids
		}
		id, seen := ids[op.Process]
		if !seen {
			id = len(ids)
			ids[op.Process] = id
		}
		c.PerKey[op.Key] = append(c.PerKey[op.Key], porcupine.Operation{ClientId: id, Input: in, Call: op.Call,
			Output: out, Return: returned, Metadata: opMetadata{Process: op.Process, Node: op.Node, Type: op.Type,
				Error: op.Error}})
	}
	for key := range c.PerKey {
		c.Keys = append(c.Keys, key)
	}
	sort.Strings(c.Keys)
	return c
}

// unobservedSets finds the indeterminate sets that can be left out: those whose value no operation ever observed.
//
// Every indeterminate operation stays open until the end of its key's history, and each one multiplies the orders
// Porcupine has to try, so dropping the ones that cannot matter keeps keys with many of them checkable. It is sound:
// once set(v) takes effect, the key holds v plus the increments applied since, until the next set. So every get or
// incr that completes before the next set observes a value in [v, v+n], where n counts the incrs that may have taken
// effect (ok or indeterminate). If no completed get or incr observed a value in that range, then in any linearization
// set(v) is followed only by indeterminate incrs until the next set or the end of the history. Removing it changes
// nothing any operation observed, and adding it at the very end of a linearization without it is always possible.
// The history with it is therefore linearizable exactly when the history without it is.
func unobservedSets(h *history) map[int]bool {
	type keyFacts struct {
		increments int     // incrs that may have taken effect
		observed   []int64 // values returned by completed gets and incrs
	}
	facts := map[string]*keyFacts{}
	for _, op := range h.Ops {
		f := facts[op.Key]
		if f == nil {
			f = &keyFacts{}
			facts[op.Key] = f
		}
		if op.F == "incr" && op.Type != "fail" {
			f.increments++
		}
		if op.Type == "ok" && op.F != "set" && op.Value != nil {
			f.observed = append(f.observed, *op.Value)
		}
	}
	for _, f := range facts {
		sort.Slice(f.observed, func(i, j int) bool { return f.observed[i] < f.observed[j] })
	}
	unobserved := map[int]bool{}
	for i, op := range h.Ops {
		if op.F != "set" || op.Type != "info" {
			continue
		}
		f := facts[op.Key]
		low, high := *op.Value, *op.Value+int64(f.increments)
		first := sort.Search(len(f.observed), func(j int) bool { return f.observed[j] >= low })
		if first == len(f.observed) || f.observed[first] > high {
			unobserved[i] = true
		}
	}
	return unobserved
}

type keyResult struct {
	Key         string                `json:"key"`
	Operations  int                   `json:"operations"`
	Result      porcupine.CheckResult `json:"result"`
	Seconds     float64               `json:"seconds"`
	Explanation string                `json:"explanation,omitempty"`
}

// checkKeys checks every key on its own, on several goroutines; each key gets at most timeout.
func checkKeys(c conversion, timeout time.Duration, workers int) []keyResult {
	results := make([]keyResult, len(c.Keys))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < max(1, workers); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				ops := c.PerKey[c.Keys[i]]
				started := time.Now()
				result, info := porcupine.CheckOperationsVerbose(kvModel, ops, timeout)
				results[i] = keyResult{Key: c.Keys[i], Operations: len(ops), Result: result,
					Seconds: time.Since(started).Seconds()}
				if result == porcupine.Illegal {
					results[i].Explanation = explain(ops, info)
				}
			}
		}()
	}
	for i := range c.Keys {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return results
}

// explain describes where a key's history stops being linearizable: the longest prefix Porcupine could linearize, the
// state it leaves, and the operations that could have come next, none of which fits.
func explain(ops []porcupine.Operation, info porcupine.LinearizationInfo) string {
	partials := info.PartialLinearizations()
	if len(partials) == 0 || len(partials[0]) == 0 {
		return ""
	}
	longest := partials[0][0]
	for _, candidate := range partials[0] {
		if len(candidate) > len(longest) {
			longest = candidate
		}
	}
	state := kvModel.Init()
	placed := map[int]bool{}
	for _, id := range longest {
		_, state = step(state, ops[id].Input, ops[id].Output)
		placed[id] = true
	}
	// An operation can come next if it was invoked before every remaining operation returned.
	var remaining []int
	firstReturn := int64(-1)
	for id, op := range ops {
		if !placed[id] {
			remaining = append(remaining, id)
			if firstReturn < 0 || op.Return < firstReturn {
				firstReturn = op.Return
			}
		}
	}
	var next []string
	for _, id := range remaining {
		if ops[id].Call <= firstReturn {
			next = append(next, describeTimed(ops[id]))
		}
	}
	if len(next) > 4 {
		next = append(next[:4], fmt.Sprintf("and %d more", len(next)-4))
	}
	return fmt.Sprintf("the longest linearizable prefix has %d of %d operations and leaves the key at %s; no operation "+
		"that could come next fits: %s", len(longest), len(ops), describeState(state), strings.Join(next, "; "))
}

func describeTimed(op porcupine.Operation) string {
	description := describeOperation(op.Input, op.Output)
	if m, ok := op.Metadata.(opMetadata); ok {
		description += fmt.Sprintf(" [process %d via %s", m.Process, m.Node)
		if m.Type == "info" {
			description += ", indeterminate"
		}
		description += "]"
	}
	return fmt.Sprintf("%s at %.3f s", description, float64(op.Call)/1e9)
}

// visualize writes Porcupine's interactive visualization of the given keys, annotated with the nemesis's faults.
func visualize(path string, c conversion, keys []string, faults []fault, timeout time.Duration) error {
	var ops []porcupine.Operation
	for _, key := range keys {
		ops = append(ops, c.PerKey[key]...)
	}
	_, info := porcupine.CheckOperationsVerbose(kvModel, ops, timeout)
	info.AddAnnotations(annotations(faults))
	return porcupine.VisualizePath(kvModel, info, path)
}

var faultColors = map[string]string{
	"partition": "#fde2e2",
	"kill":      "#e2e8fd",
	"pause":     "#fdf3d2",
	"latency":   "#e2f5e8",
	"loss":      "#efe2fd",
	"wipe":      "#d9d9d9",
}

func annotations(faults []fault) []porcupine.Annotation {
	result := make([]porcupine.Annotation, 0, len(faults))
	for _, f := range faults {
		result = append(result, porcupine.Annotation{Tag: "nemesis", Start: f.Start, End: f.End,
			Description: f.Kind, Details: f.Description, BackgroundColor: faultColors[f.Kind]})
	}
	return result
}
