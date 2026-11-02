// Command checker verifies a KeyForge test history for linearizability with Porcupine.
//
// It reads the JSON history the Java runner writes (keyforge-chaos, io.keyforge.chaos.History), checks every key's
// operations against a sequential model of GET, SET and INCR, and writes report.txt and report.json. When a history
// is not linearizable (or the checker runs out of time) it also writes linearizability.html, Porcupine's interactive
// visualization of the keys concerned, with the nemesis's faults on the timeline.
//
// Exit status: 0 linearizable, 1 not linearizable, 2 unknown (timed out), 3 the history could not be checked.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("checker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	historyPath := flags.String("history", "", "the history to check (JSON written by the KeyForge chaos runner)")
	outDir := flags.String("out", "", "where report.txt, report.json and linearizability.html go (default: the history's directory)")
	timeout := flags.Duration("timeout", time.Minute, "time limit per key; a key that runs out of time is unknown")
	workers := flags.Int("workers", runtime.NumCPU(), "keys checked at once")
	visualizeAll := flags.Bool("visualize-all", false, "write the visualization even when the history is linearizable")
	if err := flags.Parse(args); err != nil {
		return 3
	}
	if *historyPath == "" {
		fmt.Fprintln(stderr, "checker: -history is required")
		flags.Usage()
		return 3
	}
	if *outDir == "" {
		*outDir = filepath.Dir(*historyPath)
	}

	h, err := readHistory(*historyPath)
	if err != nil {
		fmt.Fprintf(stderr, "checker: %s: %v\n", *historyPath, err)
		return 3
	}
	started := time.Now()
	c := toPorcupine(h)
	results := checkKeys(c, *timeout, *workers)
	verdict, problems := verdictOf(results)
	r := &report{
		History:        filepath.Base(*historyPath),
		Started:        h.Started,
		Metadata:       h.Metadata,
		Verdict:        verdict,
		Operations:     operationCounts(h),
		Faults:         h.Faults,
		Keys:           len(c.Keys),
		Checked:        c.Ok + c.IndeterminateWrites,
		Ok:             c.Ok,
		Indeterminate:  c.IndeterminateWrites,
		DroppedFailed:  c.DroppedFailed,
		DroppedReads:   c.DroppedIndeterminateReads,
		DroppedSets:    c.DroppedUnobservedSets,
		TimeoutSeconds: timeout.Seconds(),
		Problems:       problems,
	}
	if verdict != linearizable || *visualizeAll {
		keys := c.Keys
		if verdict != linearizable {
			keys = nil
			for _, p := range problems {
				keys = append(keys, p.Key)
			}
		}
		const name = "linearizability.html"
		if err := visualize(filepath.Join(*outDir, name), c, keys, h.Faults, *timeout); err != nil {
			fmt.Fprintf(stderr, "checker: could not write the visualization: %v\n", err)
		} else {
			r.Visualization = name
		}
	}
	r.CheckSeconds = time.Since(started).Seconds()
	if err := r.write(filepath.Join(*outDir, "report.txt"), filepath.Join(*outDir, "report.json")); err != nil {
		fmt.Fprintf(stderr, "checker: could not write the report: %v\n", err)
		return 3
	}
	fmt.Fprint(stdout, r.text())
	switch verdict {
	case linearizable:
		return 0
	case notLinearizable:
		return 1
	default:
		return 2
	}
}
