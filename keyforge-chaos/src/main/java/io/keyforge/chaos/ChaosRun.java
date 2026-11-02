package io.keyforge.chaos;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One linearizability test: the workload runs for the configured duration while the nemesis injects faults; then the
 * faults are healed, the cluster is given time to recover, every key is read once more, and the history and a summary
 * are written to the output directory. Checking the history is the Porcupine checker's job.
 */
final class ChaosRun {

    private static final Logger log = LoggerFactory.getLogger(ChaosRun.class);

    /** How long the cluster may take to answer and elect a leader before the run starts. */
    private static final Duration STARTUP_PATIENCE = Duration.ofSeconds(90);

    private final ChaosOptions options;
    private final ClusterControl control;
    private final ClusterProbe probe;

    /** @param control the nemesis's hands; not used when no fault kind is enabled */
    ChaosRun(ChaosOptions options, ClusterControl control, ClusterProbe probe) {
        this.options = options;
        this.control = control;
        this.probe = probe;
    }

    /**
     * What a run produced.
     *
     * @param history  every event
     * @param summary  the text written to {@code run.txt}
     * @param recovered whether every node served again after the faults were healed
     */
    record Result(History history, String summary, boolean recovered) {
    }

    Result run() throws IOException, InterruptedException {
        Files.createDirectories(options.output());
        List<String> names = List.copyOf(options.nodes().keySet());
        if (!options.nemesis().isEmpty()) {
            // Start clean even if an earlier run was interrupted.
            control.healEverything(names);
        }
        log.info("Waiting for every node to answer and a leader to be elected");
        if (!probe.awaitServing(STARTUP_PATIENCE)) {
            throw new IllegalStateException("the cluster is not serving: " + probe.statuses(names));
        }

        History history = new History();
        RunStats stats = new RunStats();
        Workload workload = new Workload(options, history, stats);
        SplittableRandom random = new SplittableRandom(options.seed() ^ 0x5DEECE66DL);
        Nemesis nemesis = null;
        if (!options.nemesis().isEmpty()) {
            nemesis = new Nemesis(control, probe, new FaultPlanner(names, options.nemesis(), random.split()), history,
                    names, options.faultDuration(), options.calmDuration(), random.split());
        }

        log.info("Running {} clients at {} ops/s for {} s, nemesis: {}", options.clients(), options.rate(),
                options.duration().toSeconds(), FaultKind.describe(options.nemesis()));
        long started = System.nanoTime();
        if (nemesis != null) {
            nemesis.start();
        }
        try {
            workload.run(options.duration());
        } finally {
            if (nemesis != null) {
                nemesis.stop();
            }
        }
        double workloadSeconds = (System.nanoTime() - started) / 1e9;

        long healed = System.nanoTime();
        boolean recovered = probe.awaitServing(options.recovery());
        double recoverySeconds = (System.nanoTime() - healed) / 1e9;
        if (recovered) {
            log.info("Every node serves again {} s after healing; reading every key", format(recoverySeconds));
        } else {
            log.warn("The cluster did not recover within {} s: {}", options.recovery().toSeconds(),
                    probe.statuses(names));
        }
        workload.finalReads();

        Map<String, String> metadata = options.describe();
        history.writeJson(options.output().resolve("history.json"), metadata);
        String summary = summary(history, stats, workload, nemesis, workloadSeconds, recovered, recoverySeconds);
        Files.writeString(options.output().resolve("run.txt"), summary, StandardCharsets.UTF_8);
        return new Result(history, summary, recovered);
    }

    private String summary(History history, RunStats stats, Workload workload, Nemesis nemesis,
                           double workloadSeconds, boolean recovered, double recoverySeconds) {
        List<HistoryEvent> events = history.events();
        long completions = events.stream()
                .filter(event -> event instanceof HistoryEvent.Op op && op.type() != EventType.INVOKE).count();
        StringBuilder text = new StringBuilder();
        text.append("KeyForge linearizability test run\n");
        text.append("started:     ").append(history.started()).append('\n');
        text.append("options:     ");
        List<String> pairs = new ArrayList<>();
        options.describe().forEach((name, value) -> pairs.add(name + "=" + value));
        text.append(String.join(" ", pairs)).append('\n');
        text.append(String.format(Locale.ROOT, "workload:    %d operations over %d keys in %s s, including the final"
                + " reads%n", completions, workload.keys().size(), format(workloadSeconds)));
        text.append('\n').append(stats.render()).append('\n');
        text.append("nemesis:     ").append(nemesis == null ? "none" : nemesis.faults() + " faults").append('\n');
        for (HistoryEvent event : events) {
            if (event instanceof HistoryEvent.Nemesis action && action.type() == EventType.INFO) {
                text.append(String.format(Locale.ROOT, "  %8.1f s  %-5s %-9s %s%n", action.timeNanos() / 1e9,
                        action.function(), action.fault().label(), action.description()));
            }
        }
        text.append("recovery:    ").append(recovered
                ? "every node served again " + format(recoverySeconds) + " s after the faults were healed"
                : "the cluster did NOT recover within " + options.recovery().toSeconds() + " s").append('\n');
        text.append("history:     history.json (").append(events.size()).append(" events)\n");
        return text.toString();
    }

    private static String format(double seconds) {
        return String.format(Locale.ROOT, "%.1f", seconds);
    }
}
