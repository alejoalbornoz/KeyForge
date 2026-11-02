package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

// The sequential specification of one key under GET, SET and INCR, as Porcupine models it. The history is partitioned
// by key (operations on different keys are independent), so a state is one key's value.

type kvInput struct {
	Key   string
	Op    string // get, set or incr
	Value int64  // set: the value written
	// Order numbers a key's indeterminate incrs by invocation (1, 2, ...); 0 for every other operation, or when the
	// order is not enforced.
	Order int
}

type kvOutput struct {
	Exists  bool  // get: whether the key existed
	Value   int64 // get: the value read; incr: the new value
	Unknown bool  // the outcome is unknown (an indeterminate write): any result is accepted
}

type kvState struct {
	Exists bool
	Value  int64
	// IndeterminateIncrs counts the numbered indeterminate incrs already linearized: the next one must be number
	// IndeterminateIncrs+1.
	//
	// Indeterminate incrs are interchangeable. Each adds one and accepts any result, and none ever returns, so no
	// operation has to follow one. Two of them can swap places in any linearization: the earlier-invoked one can take
	// the later one's place, and the later one can move further back. So whenever a linearization exists, one exists
	// that applies them in invocation order, and enforcing that order loses nothing. It turns the 2^k subsets of k open
	// increments Porcupine would otherwise explore into k+1 prefixes.
	IndeterminateIncrs int
}

// opMetadata is shown in the visualization's tooltips.
type opMetadata struct {
	Process int
	Node    string
	Type    string
	Error   string
}

func step(state, input, output interface{}) (bool, interface{}) {
	s := state.(kvState)
	in := input.(kvInput)
	out := output.(kvOutput)
	switch in.Op {
	case "get":
		if out.Unknown {
			return true, s
		}
		return out.Exists == s.Exists && (!s.Exists || out.Value == s.Value), s
	case "set":
		return true, kvState{Exists: true, Value: in.Value, IndeterminateIncrs: s.IndeterminateIncrs}
	case "incr":
		// INCR of a missing key starts from 0; the workload only ever writes integers.
		next := kvState{Exists: true, Value: s.Value + 1, IndeterminateIncrs: s.IndeterminateIncrs}
		if !s.Exists {
			next.Value = 1
		}
		if in.Order > 0 {
			if in.Order != s.IndeterminateIncrs+1 {
				return false, s
			}
			next.IndeterminateIncrs++
		}
		return out.Unknown || out.Value == next.Value, next
	}
	panic("unknown operation " + in.Op)
}

func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := map[string][]porcupine.Operation{}
	for _, op := range history {
		key := op.Input.(kvInput).Key
		byKey[key] = append(byKey[key], op)
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	partitions := make([][]porcupine.Operation, 0, len(keys))
	for _, key := range keys {
		partitions = append(partitions, byKey[key])
	}
	return partitions
}

func describeState(state interface{}) string {
	s := state.(kvState)
	if !s.Exists {
		return "nil"
	}
	return fmt.Sprint(s.Value)
}

func describeOperation(input, output interface{}) string {
	in := input.(kvInput)
	out := output.(kvOutput)
	result := "?"
	switch {
	case out.Unknown:
	case in.Op == "set":
		result = "OK"
	case in.Op == "get" && !out.Exists:
		result = "nil"
	default:
		result = fmt.Sprint(out.Value)
	}
	switch in.Op {
	case "set":
		return fmt.Sprintf("set(%s, %d) -> %s", in.Key, in.Value, result)
	default:
		return fmt.Sprintf("%s(%s) -> %s", in.Op, in.Key, result)
	}
}

func describeMetadata(metadata interface{}) string {
	m, ok := metadata.(opMetadata)
	if !ok {
		return ""
	}
	parts := []string{fmt.Sprintf("process %d via %s: %s", m.Process, m.Node, m.Type)}
	if m.Error != "" {
		parts = append(parts, m.Error)
	}
	return strings.Join(parts, "; ")
}

var kvModel = porcupine.Model{
	Partition: partitionByKey,
	Init:      func() interface{} { return kvState{} },
	Step:      step,
	Equal:     func(a, b interface{}) bool { return a.(kvState) == b.(kvState) },
	Hash: func(state interface{}) uint64 {
		s := state.(kvState)
		hash := uint64(s.IndeterminateIncrs) * 0x9E3779B97F4A7C15
		if s.Exists {
			hash ^= uint64(s.Value)*2 + 1
		}
		return hash
	},
	DescribeOperation:         describeOperation,
	DescribeState:             describeState,
	DescribeOperationMetadata: describeMetadata,
}
