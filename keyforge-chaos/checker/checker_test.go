package main

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// historyBuilder writes histories in the runner's JSON format. Times are in milliseconds for readability.
type historyBuilder struct {
	events []map[string]interface{}
}

// op adds an invocation at call and, unless typ is "open", a completion at ret. value is the value written by a set,
// or the result of a get or incr (nil for a missing key).
func (b *historyBuilder) op(process int, f, key string, call, ret int64, typ string, value interface{}) *historyBuilder {
	invoke := map[string]interface{}{"time": call * 1e6, "type": "invoke", "process": process, "f": f, "key": key,
		"value": nil, "node": "kf1"}
	if f == "set" {
		invoke["value"] = value
	}
	b.events = append(b.events, invoke)
	if typ != "open" {
		done := map[string]interface{}{"time": ret * 1e6, "type": typ, "process": process, "f": f, "key": key,
			"value": value, "node": "kf1"}
		if typ != "ok" {
			done["error"] = "something went wrong"
		}
		b.events = append(b.events, done)
	}
	return b
}

func (b *historyBuilder) nemesis(start, stop int64, kind, description string) *historyBuilder {
	for _, e := range []struct {
		t      int64
		typ, f string
	}{{start, "invoke", "start"}, {start + 1, "info", "start"}, {stop, "invoke", "stop"}, {stop + 1, "info", "stop"}} {
		b.events = append(b.events, map[string]interface{}{"time": e.t * 1e6, "type": e.typ, "process": "nemesis",
			"f": e.f, "fault": kind, "value": description})
	}
	return b
}

func (b *historyBuilder) json(t *testing.T) []byte {
	t.Helper()
	sort.SliceStable(b.events, func(i, j int) bool {
		return b.events[i]["time"].(int64) < b.events[j]["time"].(int64)
	})
	for i, e := range b.events {
		e["index"] = i
	}
	encoded, err := json.Marshal(map[string]interface{}{"format": historyFormat, "started": "2026-10-29T00:00:00Z",
		"time_unit": "ns", "metadata": map[string]string{"seed": "1"}, "events": b.events})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func (b *historyBuilder) parse(t *testing.T) *history {
	t.Helper()
	h, err := parseHistory(bytes.NewReader(b.json(t)))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (b *historyBuilder) check(t *testing.T) porcupine.CheckResult {
	t.Helper()
	results := checkKeys(toPorcupine(b.parse(t)), 10*time.Second, 2)
	verdict, _ := verdictOf(results)
	switch verdict {
	case linearizable:
		return porcupine.Ok
	case notLinearizable:
		return porcupine.Illegal
	default:
		return porcupine.Unknown
	}
}

func expect(t *testing.T, b *historyBuilder, want porcupine.CheckResult) {
	t.Helper()
	if got := b.check(t); got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}

func TestModelSteps(t *testing.T) {
	missing := kvState{}
	five := kvState{Exists: true, Value: 5}
	cases := []struct {
		name  string
		state kvState
		in    kvInput
		out   kvOutput
		ok    bool
		next  kvState
	}{
		{"get of a missing key reads nil", missing, kvInput{Op: "get"}, kvOutput{}, true, missing},
		{"get must not invent a value", missing, kvInput{Op: "get"}, kvOutput{Exists: true, Value: 5}, false, missing},
		{"get reads the value", five, kvInput{Op: "get"}, kvOutput{Exists: true, Value: 5}, true, five},
		{"get must not miss a value", five, kvInput{Op: "get"}, kvOutput{}, false, five},
		{"get must read the current value", five, kvInput{Op: "get"}, kvOutput{Exists: true, Value: 4}, false, five},
		{"set overwrites", five, kvInput{Op: "set", Value: 9}, kvOutput{}, true, kvState{Exists: true, Value: 9}},
		{"incr of a missing key gives 1", missing, kvInput{Op: "incr"}, kvOutput{Value: 1}, true, kvState{Exists: true, Value: 1}},
		{"incr adds one", five, kvInput{Op: "incr"}, kvOutput{Value: 6}, true, kvState{Exists: true, Value: 6}},
		{"incr must add exactly one", five, kvInput{Op: "incr"}, kvOutput{Value: 7}, false, kvState{Exists: true, Value: 6}},
		{"an indeterminate incr accepts anything", five, kvInput{Op: "incr"}, kvOutput{Unknown: true}, true, kvState{Exists: true, Value: 6}},
	}
	for _, c := range cases {
		ok, next := step(c.state, c.in, c.out)
		if ok != c.ok || (ok && next.(kvState) != c.next) {
			t.Errorf("%s: got (%v, %v), want (%v, %v)", c.name, ok, next, c.ok, c.next)
		}
	}
}

func TestParsePairsInvocationsWithCompletions(t *testing.T) {
	h := (&historyBuilder{}).
		op(0, "set", "k", 0, 10, "ok", 1000).
		op(1, "get", "k", 5, 15, "ok", 1000).
		op(2, "incr", "k", 6, 30, "info", nil).
		op(3, "incr", "k", 20, 0, "open", nil).
		nemesis(2, 25, "partition", "halves {kf1,kf2} | {kf3,kf4,kf5}").
		parse(t)

	if len(h.Ops) != 4 {
		t.Fatalf("expected 4 operations, got %d", len(h.Ops))
	}
	set, get, info, open := h.Ops[0], h.Ops[1], h.Ops[2], h.Ops[3]
	if set.F != "set" || *set.Value != 1000 || set.Type != "ok" || set.Call != 0 || set.Return != 10e6 {
		t.Errorf("set: %+v", set)
	}
	if get.Type != "ok" || *get.Value != 1000 {
		t.Errorf("get: %+v", get)
	}
	if info.Type != "info" || info.Error != "something went wrong" {
		t.Errorf("info: %+v", info)
	}
	if open.Type != "info" || open.Return != -1 {
		t.Errorf("an invocation without completion must be indeterminate: %+v", open)
	}
	if len(h.Faults) != 1 || h.Faults[0].Kind != "partition" || h.Faults[0].Start != 2e6 || h.Faults[0].End != 26e6 ||
		!strings.Contains(h.Faults[0].Description, "halves") {
		t.Errorf("faults: %+v", h.Faults)
	}
	if h.End != 30e6 {
		t.Errorf("end: %d", h.End)
	}
}

func TestParseRejectsMalformedHistories(t *testing.T) {
	for name, text := range map[string]string{
		"wrong format":    `{"format":"other","time_unit":"ns","events":[]}`,
		"orphan":          `{"format":"keyforge-history/1","time_unit":"ns","events":[{"index":0,"time":1,"type":"ok","process":1,"f":"get","key":"k","value":null}]}`,
		"mismatched":      `{"format":"keyforge-history/1","time_unit":"ns","events":[{"index":0,"time":1,"type":"invoke","process":1,"f":"get","key":"k"},{"index":1,"time":2,"type":"ok","process":1,"f":"get","key":"other","value":null}]}`,
		"two in flight":   `{"format":"keyforge-history/1","time_unit":"ns","events":[{"index":0,"time":1,"type":"invoke","process":1,"f":"get","key":"k"},{"index":1,"time":2,"type":"invoke","process":1,"f":"get","key":"k"}]}`,
		"not an integer":  `{"format":"keyforge-history/1","time_unit":"ns","events":[{"index":0,"time":1,"type":"invoke","process":1,"f":"set","key":"k","value":"x"}]}`,
		"unknown op":      `{"format":"keyforge-history/1","time_unit":"ns","events":[{"index":0,"time":1,"type":"invoke","process":1,"f":"append","key":"k"}]}`,
		"set, no value":   `{"format":"keyforge-history/1","time_unit":"ns","events":[{"index":0,"time":1,"type":"invoke","process":1,"f":"set","key":"k","value":null}]}`,
		"incr, no result": `{"format":"keyforge-history/1","time_unit":"ns","events":[{"index":0,"time":1,"type":"invoke","process":1,"f":"incr","key":"k"},{"index":1,"time":2,"type":"ok","process":1,"f":"incr","key":"k","value":null}]}`,
	} {
		if _, err := parseHistory(strings.NewReader(text)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestASequentialHistoryIsLinearizable(t *testing.T) {
	expect(t, (&historyBuilder{}).
		op(0, "get", "k", 0, 1, "ok", nil).
		op(0, "set", "k", 2, 3, "ok", 1000).
		op(1, "incr", "k", 4, 5, "ok", 1001).
		op(1, "get", "k", 6, 7, "ok", 1001), porcupine.Ok)
}

func TestConcurrentOperationsMayTakeEffectInEitherOrder(t *testing.T) {
	// The read overlaps the write, so it may see the old or the new value.
	for _, read := range []interface{}{nil, 1000} {
		expect(t, (&historyBuilder{}).
			op(0, "set", "k", 0, 10, "ok", 1000).
			op(1, "get", "k", 5, 15, "ok", read), porcupine.Ok)
	}
}

func TestAStaleReadIsCaught(t *testing.T) {
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 10, "ok", 1000).
		op(1, "set", "k", 11, 20, "ok", 2000).
		op(2, "get", "k", 30, 40, "ok", 1000), porcupine.Illegal)
}

func TestALostWriteIsCaughtByALaterRead(t *testing.T) {
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 10, "ok", 1000).
		op(1, "get", "k", 100, 110, "ok", nil), porcupine.Illegal)
}

func TestAnIncrementAppliedTwiceIsCaught(t *testing.T) {
	expect(t, (&historyBuilder{}).
		op(0, "incr", "k", 0, 10, "ok", 1).
		op(1, "incr", "k", 20, 30, "ok", 3), porcupine.Illegal)
}

func TestAnIndeterminateIncrementMayExplainAJump(t *testing.T) {
	expect(t, (&historyBuilder{}).
		op(0, "incr", "k", 0, 10, "info", nil).
		op(1, "incr", "k", 20, 30, "ok", 2), porcupine.Ok)
	expect(t, (&historyBuilder{}).
		op(0, "incr", "k", 0, 0, "open", nil).
		op(1, "incr", "k", 20, 30, "ok", 2), porcupine.Ok)
}

func TestAnIndeterminateWriteMayOrMayNotTakeEffect(t *testing.T) {
	for _, read := range []interface{}{nil, 1000} {
		expect(t, (&historyBuilder{}).
			op(0, "set", "k", 0, 10, "info", 1000).
			op(1, "get", "k", 20, 30, "ok", read), porcupine.Ok)
	}
	// But once seen, it cannot be unseen.
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 10, "info", 1000).
		op(1, "get", "k", 20, 30, "ok", 1000).
		op(1, "get", "k", 40, 50, "ok", nil), porcupine.Illegal)
}

func TestAFailedWriteMustNeverBeSeen(t *testing.T) {
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 10, "fail", 1000).
		op(1, "get", "k", 20, 30, "ok", nil), porcupine.Ok)
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 10, "fail", 1000).
		op(1, "get", "k", 20, 30, "ok", 1000), porcupine.Illegal)
}

func TestKeysAreCheckedIndependently(t *testing.T) {
	results := checkKeys(toPorcupine((&historyBuilder{}).
		op(0, "set", "a", 0, 10, "ok", 1000).
		op(1, "get", "b", 20, 30, "ok", nil).
		op(2, "get", "a", 20, 30, "ok", nil).
		parse(t)), 10*time.Second, 2)
	verdict, problems := verdictOf(results)
	if verdict != notLinearizable || len(problems) != 1 || problems[0].Key != "a" {
		t.Fatalf("expected only key a to fail, got %s %+v", verdict, problems)
	}
	if !strings.Contains(problems[0].Explanation, "leaves the key at 1000") ||
		!strings.Contains(problems[0].Explanation, "get(a) -> nil") {
		t.Errorf("explanation: %s", problems[0].Explanation)
	}
}

func writeHistory(t *testing.T, b *historyBuilder) (string, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "history.json")
	if err := os.WriteFile(path, b.json(t), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, path
}

func TestAViolationProducesAReportAndAVisualization(t *testing.T) {
	dir, path := writeHistory(t, (&historyBuilder{}).
		op(0, "incr", "k", 0, 10, "ok", 1).
		op(1, "incr", "k", 20, 30, "ok", 3).
		op(2, "get", "other", 0, 10, "ok", nil).
		nemesis(5, 25, "kill", "leader [kf1]"))
	var stdout, stderr bytes.Buffer

	if code := run([]string{"-history", path}, &stdout, &stderr); code != 1 {
		t.Fatalf("expected exit 1, got %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "RESULT: NOT LINEARIZABLE (1 of 2 keys have no linearization)") {
		t.Errorf("report: %s", stdout.String())
	}
	if text, _ := os.ReadFile(filepath.Join(dir, "report.txt")); !bytes.Equal(text, stdout.Bytes()) {
		t.Errorf("report.txt differs from the printed report")
	}
	var r report
	encoded, _ := os.ReadFile(filepath.Join(dir, "report.json"))
	if err := json.Unmarshal(encoded, &r); err != nil || r.Verdict != notLinearizable || r.Problems[0].Key != "k" {
		t.Errorf("report.json: %v %+v", err, r)
	}
	html, err := os.ReadFile(filepath.Join(dir, "linearizability.html"))
	// The operations are embedded as JSON, the way json.Marshal encodes them.
	described, _ := json.Marshal("incr(k) -> 3")
	if err != nil || !bytes.Contains(html, described) || bytes.Contains(html, []byte("get(other)")) {
		t.Errorf("expected a visualization of key k only: %v", err)
	}
	if !bytes.Contains(html, []byte("leader [kf1]")) {
		t.Errorf("expected the nemesis's fault on the timeline")
	}
}

func TestALinearizableHistoryProducesNoVisualization(t *testing.T) {
	dir, path := writeHistory(t, (&historyBuilder{}).op(0, "set", "k", 0, 10, "ok", 1000))
	var stdout, stderr bytes.Buffer

	if code := run([]string{"-history", path, "-out", dir}, &stdout, &stderr); code != 0 {
		t.Fatalf("expected exit 0, got %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "RESULT: LINEARIZABLE") {
		t.Errorf("report: %s", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "linearizability.html")); !os.IsNotExist(err) {
		t.Errorf("expected no visualization")
	}
}

func TestUnreadableHistoriesAreReported(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-history", filepath.Join(t.TempDir(), "missing.json")}, &stdout, &stderr); code != 3 {
		t.Fatalf("expected exit 3, got %d", code)
	}
	if code := run(nil, &stdout, &stderr); code != 3 {
		t.Fatalf("expected exit 3 without -history, got %d", code)
	}
}

// testdata/sample-history.json was written by the Java runner (a short run with process kills against the Docker
// cluster): the checker must read the runner's format as written.
func TestAHistoryWrittenByTheRunnerIsReadAndChecked(t *testing.T) {
	h, err := readHistory(filepath.Join("testdata", "sample-history.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Ops) < 100 || len(h.Faults) == 0 || h.Metadata["nemesis"] == "" {
		t.Fatalf("expected operations, faults and metadata: %d ops, %d faults, %v", len(h.Ops), len(h.Faults), h.Metadata)
	}
	verdict, problems := verdictOf(checkKeys(toPorcupine(h), 30*time.Second, 2))
	if verdict != linearizable {
		t.Fatalf("expected the sample to be linearizable, got %s: %+v", verdict, problems)
	}
}

func TestAnIndeterminateSetNobodyObservedIsLeftOut(t *testing.T) {
	c := toPorcupine((&historyBuilder{}).
		op(0, "set", "k", 0, 10, "ok", 1000).
		op(1, "set", "k", 5, 15, "info", 2000).
		op(2, "incr", "k", 20, 30, "ok", 1001).
		op(3, "set", "k", 40, 50, "info", 3000).
		op(3, "get", "k", 60, 70, "ok", 3000).
		parse(t))

	if c.DroppedUnobservedSets != 1 || c.IndeterminateWrites != 1 || len(c.PerKey["k"]) != 4 {
		t.Fatalf("expected only set(2000) to be left out: %+v", c)
	}
}

func TestLeavingOutUnobservedSetsNeverHidesAViolation(t *testing.T) {
	// The read of nil after an acknowledged set is wrong whatever the indeterminate set did.
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 10, "ok", 1000).
		op(1, "set", "k", 5, 15, "info", 2000).
		op(2, "get", "k", 20, 30, "ok", nil), porcupine.Illegal)
	// A value derived from the indeterminate set keeps it in, and explains the read.
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 10, "ok", 1000).
		op(1, "set", "k", 5, 15, "info", 2000).
		op(2, "incr", "k", 20, 30, "info", nil).
		op(3, "get", "k", 40, 50, "ok", 2001), porcupine.Ok)
}

// The checker's reductions (leaving out unobserved indeterminate sets, ordering indeterminate incrs) must not change
// any verdict: random small histories of one key, with overlapping operations and random outcomes, are checked with
// and without them.
func TestTheReductionsPreserveEveryVerdict(t *testing.T) {
	const trials = 3000
	random := rand.New(rand.NewSource(1))
	illegal, dropped, ordered := 0, 0, 0
	for trial := 0; trial < trials; trial++ {
		b := &historyBuilder{}
		values := []interface{}{nil}
		processes := 2 + random.Intn(5)
		for process := 0; process < processes; process++ {
			at := int64(random.Intn(20))
			operations := 1 + random.Intn(4)
			for n := 0; n < operations; n++ {
				call, ret := at, at+1+int64(random.Intn(30))
				at = ret + 1 + int64(random.Intn(10))
				typ := []string{"ok", "ok", "info", "info", "fail"}[random.Intn(5)]
				switch random.Intn(3) {
				case 0:
					value := int64(1000 * (1 + random.Intn(3)))
					values = append(values, value, value+1, value+2)
					b.op(process*10+n, "set", "k", call, ret, typ, value)
				case 1:
					var result interface{}
					if typ == "ok" {
						result = values[random.Intn(len(values))]
					}
					b.op(process*10+n, "get", "k", call, ret, typ, result)
				default:
					var result interface{}
					if typ == "ok" {
						result = int64(1 + random.Intn(3))
						if candidate, isValue := values[random.Intn(len(values))].(int64); isValue {
							result = candidate
						}
					}
					b.op(process*10+n, "incr", "k", call, ret, typ, result)
				}
			}
		}
		h := b.parse(t)
		reduced := toPorcupine(h)
		full := convert(h, nil, false)
		reducedVerdict, _ := verdictOf(checkKeys(reduced, 10*time.Second, 1))
		fullVerdict, _ := verdictOf(checkKeys(full, 10*time.Second, 1))
		if reducedVerdict != fullVerdict {
			t.Errorf("trial %d: %s with the reductions, %s without: %s", trial, reducedVerdict, fullVerdict,
				b.json(t))
		}
		if fullVerdict == notLinearizable {
			illegal++
		}
		dropped += reduced.DroppedUnobservedSets
		incrs := 0
		for _, op := range reduced.PerKey["k"] {
			if op.Input.(kvInput).Order > 0 {
				incrs++
			}
		}
		if incrs >= 2 {
			ordered++
		}
	}
	// The trials must exercise both verdicts and both reductions.
	if illegal < trials/10 || illegal > trials*9/10 || dropped == 0 || ordered < trials/10 {
		t.Fatalf("degenerate trials: %d of %d not linearizable, %d sets left out, %d with two indeterminate incrs or "+
			"more", illegal, trials, dropped, ordered)
	}
}

func TestManyIndeterminateIncrementsStayCheckable(t *testing.T) {
	// 40 open increments: without ordering them, Porcupine would face 2^40 subsets.
	b := (&historyBuilder{}).op(0, "set", "k", 0, 1, "ok", 1000)
	for i := 0; i < 40; i++ {
		b.op(100+i, "incr", "k", int64(10+i), int64(11+i), "info", nil)
	}
	b.op(1, "get", "k", 100, 101, "ok", 1017).op(2, "incr", "k", 102, 103, "ok", 1025).op(3, "get", "k", 104, 105, "ok", 1025)
	started := time.Now()
	expect(t, b, porcupine.Ok)
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Errorf("took %s", elapsed)
	}
	// But they cannot explain more increments than there were, nor a value going down.
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 1, "ok", 1000).
		op(1, "incr", "k", 2, 3, "info", nil).
		op(2, "incr", "k", 4, 5, "info", nil).
		op(3, "incr", "k", 10, 11, "ok", 1004), porcupine.Illegal)
	expect(t, (&historyBuilder{}).
		op(0, "set", "k", 0, 1, "ok", 1000).
		op(1, "incr", "k", 2, 3, "info", nil).
		op(2, "get", "k", 10, 11, "ok", 1001).
		op(3, "get", "k", 12, 13, "ok", 1000), porcupine.Illegal)
}
