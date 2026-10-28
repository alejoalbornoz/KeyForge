package io.keyforge.raft.sim;

import java.util.Comparator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

/** Runs many seeds in parallel; each run is single-threaded and deterministic, so parallelism changes nothing. */
final class SeedRunner {

    /** Outcome of a batch: the failures (lowest seed first) and totals over the passing runs. */
    record Batch(List<SimulationFailure> failures, long runs, long steps, long leaders, long writes, long reads,
                 long snapshotsInstalled, long crashes, LongSummaryStatistics applied) {

        Optional<SimulationFailure> firstFailure() {
            return failures.stream().min(Comparator.comparingLong(SimulationFailure::seed));
        }

        String summary() {
            return runs + " seeds, " + steps + " steps, " + leaders + " leaders elected, " + writes
                    + " writes acknowledged, " + reads + " linearizable reads, " + snapshotsInstalled
                    + " snapshots installed, " + crashes + " crashes, " + applied.getAverage()
                    + " entries applied per run on average, " + failures.size() + " failures"
                    + failures.stream().sorted(Comparator.comparingLong(SimulationFailure::seed))
                    .map(failure -> "\n  " + failure.getMessage().lines().findFirst().orElse(""))
                    .reduce("", String::concat);
        }
    }

    private SeedRunner() {
    }

    /** {@code -Dkeyforge.sim.seed=N} runs only seed N; otherwise seeds {@code first .. first + count - 1}. */
    static Batch run(long first, long count) {
        String single = System.getProperty("keyforge.sim.seed");
        LongStream seeds = single != null
                ? LongStream.of(Long.parseLong(single))
                : LongStream.range(first, first + count);
        AtomicLong steps = new AtomicLong();
        AtomicLong leaders = new AtomicLong();
        AtomicLong writes = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        AtomicLong installs = new AtomicLong();
        AtomicLong crashes = new AtomicLong();
        AtomicLong runs = new AtomicLong();
        LongSummaryStatistics applied = new LongSummaryStatistics();
        List<SimulationFailure> failures = seeds.parallel().mapToObj(seed -> {
            try {
                SimulationResult result = Simulation.run(seed);
                runs.incrementAndGet();
                steps.addAndGet(result.steps());
                leaders.addAndGet(result.leadersElected());
                writes.addAndGet(result.writesAcknowledged());
                reads.addAndGet(result.readsServed());
                installs.addAndGet(result.snapshotsInstalled());
                crashes.addAndGet(result.crashes());
                synchronized (applied) {
                    applied.accept(result.entriesApplied());
                }
                return null;
            } catch (SimulationFailure failure) {
                runs.incrementAndGet();
                return failure;
            }
        }).filter(failure -> failure != null).toList();
        return new Batch(failures, runs.get(), steps.get(), leaders.get(), writes.get(), reads.get(), installs.get(),
                crashes.get(), applied);
    }
}
