package io.keyforge.server.metrics;

import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.keyforge.server.command.CommandObserver;
import io.keyforge.server.command.CommandSpec;
import io.keyforge.server.command.CommandTable;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Times every command with a Micrometer {@link Timer} per command name, published as a percentile histogram so that
 * Prometheus can compute p99 across nodes ({@code histogram_quantile}), and counts error replies.
 *
 * <p>Meters are created once, up front, for every command in the table: recording is a map lookup by identity and
 * the timer's lock-free update, with no allocation.
 *
 * <ul>
 *   <li>{@code keyforge.commands} (timer, tag {@code command}): dispatch to reply on this node. On a follower that
 *       includes forwarding to the leader; on the leader, the wait for the Raft commit or the ReadIndex round.</li>
 *   <li>{@code keyforge.command.errors} (counter, tag {@code command}): replies that were errors.</li>
 * </ul>
 */
public final class CommandMetrics implements CommandObserver {

    /** Below this a histogram bucket is not worth having: a local in-memory command takes about a microsecond. */
    static final Duration MIN_EXPECTED = Duration.ofNanos(10_000);
    /** The longest a command waits by default ({@code raft.request-timeout-ms}), with room to spare. */
    static final Duration MAX_EXPECTED = Duration.ofSeconds(10);

    private final Map<CommandSpec, Meters> meters = new IdentityHashMap<>();

    public CommandMetrics(CommandTable commands, MeterRegistry registry) {
        for (CommandSpec spec : commands.commands()) {
            Timer timer = Timer.builder("keyforge.commands")
                    .description("Time from dispatching a command to its reply on this node")
                    .tag("command", spec.name())
                    .publishPercentileHistogram()
                    .minimumExpectedValue(MIN_EXPECTED)
                    .maximumExpectedValue(MAX_EXPECTED)
                    .register(registry);
            Counter errors = Counter.builder("keyforge.command.errors")
                    .description("Commands whose reply was an error")
                    .tag("command", spec.name())
                    .register(registry);
            meters.put(spec, new Meters(timer, errors));
        }
    }

    @Override
    public void executed(CommandSpec spec, long nanos, boolean error) {
        Meters forSpec = meters.get(spec);
        if (forSpec == null) {
            return; // a command added to the table after this was built
        }
        forSpec.timer().record(nanos, TimeUnit.NANOSECONDS);
        if (error) {
            forSpec.errors().increment();
        }
    }

    private record Meters(Timer timer, Counter errors) {
    }
}
