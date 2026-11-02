package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

// historyFormat is the format the Java runner writes (io.keyforge.chaos.History).
const historyFormat = "keyforge-history/1"

// An operation is a client operation rebuilt from its invocation and completion.
type operation struct {
	Process int
	F       string // get, set or incr
	Key     string
	Value   *int64 // set: the value written; get: the value read (nil: no such key); incr: the new value
	Type    string // ok, fail or info; info also for an invocation that never completed
	Call    int64  // nanoseconds since the history began
	Return  int64  // -1 if the invocation never completed
	Node    string
	Error   string
}

// A fault is a nemesis action, from the invocation of its start to the completion of its stop: the longest window in
// which it may have been in effect.
type fault struct {
	Kind        string
	Description string
	Start       int64
	End         int64
}

type history struct {
	Started  string
	Metadata map[string]string
	Ops      []operation
	Faults   []fault
	End      int64 // time of the last event
}

type rawHistory struct {
	Format   string            `json:"format"`
	Started  string            `json:"started"`
	TimeUnit string            `json:"time_unit"`
	Metadata map[string]string `json:"metadata"`
	Events   []rawEvent        `json:"events"`
}

type rawEvent struct {
	Index   int64           `json:"index"`
	Time    int64           `json:"time"`
	Type    string          `json:"type"`
	Process json.RawMessage `json:"process"`
	F       string          `json:"f"`
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
	Node    string          `json:"node"`
	Error   string          `json:"error"`
	Fault   string          `json:"fault"`
}

func readHistory(path string) (*history, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parseHistory(file)
}

// parseHistory pairs every invocation with the next completion of the same process. Failed operations are kept here
// (the report counts them); the checker drops them.
func parseHistory(r io.Reader) (*history, error) {
	var raw rawHistory
	decoder := json.NewDecoder(r)
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("not a history: %w", err)
	}
	if raw.Format != historyFormat {
		return nil, fmt.Errorf("unsupported history format %q, expected %q", raw.Format, historyFormat)
	}
	if raw.TimeUnit != "ns" {
		return nil, fmt.Errorf("unsupported time unit %q, expected ns", raw.TimeUnit)
	}
	h := &history{Started: raw.Started, Metadata: raw.Metadata}
	pending := map[int]int{} // process -> index in h.Ops of its open invocation
	var open *fault
	for _, e := range raw.Events {
		if e.Time > h.End {
			h.End = e.Time
		}
		if bytes.Equal(bytes.TrimSpace(e.Process), []byte(`"nemesis"`)) {
			if err := nemesisEvent(h, &open, e); err != nil {
				return nil, fmt.Errorf("event %d: %w", e.Index, err)
			}
			continue
		}
		process, err := strconv.Atoi(string(e.Process))
		if err != nil {
			return nil, fmt.Errorf("event %d: process %s is neither a number nor \"nemesis\"", e.Index, e.Process)
		}
		value, err := parseValue(e.Value)
		if err != nil {
			return nil, fmt.Errorf("event %d: %w", e.Index, err)
		}
		switch e.Type {
		case "invoke":
			if _, busy := pending[process]; busy {
				return nil, fmt.Errorf("event %d: process %d invoked an operation while another was in flight", e.Index, process)
			}
			if e.F != "get" && e.F != "set" && e.F != "incr" {
				return nil, fmt.Errorf("event %d: unknown operation %q", e.Index, e.F)
			}
			if e.F == "set" && value == nil {
				return nil, fmt.Errorf("event %d: set without a value", e.Index)
			}
			pending[process] = len(h.Ops)
			h.Ops = append(h.Ops, operation{Process: process, F: e.F, Key: e.Key, Value: value, Type: "info",
				Call: e.Time, Return: -1, Node: e.Node})
		case "ok", "fail", "info":
			i, busy := pending[process]
			if !busy {
				return nil, fmt.Errorf("event %d: completion of process %d, which has no operation in flight", e.Index, process)
			}
			op := &h.Ops[i]
			if op.F != e.F || op.Key != e.Key {
				return nil, fmt.Errorf("event %d: completion %s %s does not match invocation %s %s", e.Index, e.F, e.Key, op.F, op.Key)
			}
			delete(pending, process)
			op.Type = e.Type
			op.Return = e.Time
			op.Error = e.Error
			if e.F != "set" {
				op.Value = value
			}
			if e.Type == "ok" && e.F == "incr" && value == nil {
				return nil, fmt.Errorf("event %d: incr completed without a value", e.Index)
			}
		default:
			return nil, fmt.Errorf("event %d: unknown event type %q", e.Index, e.Type)
		}
	}
	if open != nil {
		open.End = h.End
		h.Faults = append(h.Faults, *open)
	}
	return h, nil
}

func nemesisEvent(h *history, open **fault, e rawEvent) error {
	description := ""
	if len(e.Value) > 0 && string(e.Value) != "null" {
		if err := json.Unmarshal(e.Value, &description); err != nil {
			return fmt.Errorf("nemesis value is not a string: %w", err)
		}
	}
	switch {
	case e.F == "start" && e.Type == "invoke":
		if *open != nil {
			(*open).End = e.Time
			h.Faults = append(h.Faults, **open)
		}
		*open = &fault{Kind: e.Fault, Description: description, Start: e.Time, End: -1}
	case e.F == "start" && e.Type == "info":
		if *open != nil {
			(*open).Description = description
		}
	case e.F == "stop" && e.Type == "info":
		if *open != nil {
			(*open).End = e.Time
			h.Faults = append(h.Faults, **open)
			*open = nil
		}
	case e.F == "stop" && e.Type == "invoke":
	default:
		return fmt.Errorf("unknown nemesis event %s %s", e.Type, e.F)
	}
	return nil
}

func parseValue(raw json.RawMessage) (*int64, error) {
	text := string(bytes.TrimSpace(raw))
	if text == "" || text == "null" {
		return nil, nil
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("value %s is not an integer", text)
	}
	return &value, nil
}
