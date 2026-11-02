package io.keyforge.chaos;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SynchronizedHistogram;

/**
 * Counts of completed operations by function and outcome, and the latency of successful ones. Thread-safe. The
 * history holds the same facts; these are for the run summary a person reads.
 */
final class RunStats {

    private static final long MAX_LATENCY_MICROS = TimeUnit.MINUTES.toMicros(10);

    private final Map<OpFunction, long[]> counts = new EnumMap<>(OpFunction.class);
    private final Map<OpFunction, SynchronizedHistogram> latencies = new EnumMap<>(OpFunction.class);

    RunStats() {
        for (OpFunction function : OpFunction.values()) {
            counts.put(function, new long[EventType.values().length]);
            latencies.put(function, new SynchronizedHistogram(MAX_LATENCY_MICROS, 3));
        }
    }

    /** Counts one completed operation; {@code ok} ones also record their latency. */
    void record(OpFunction function, EventType outcome, long latencyNanos) {
        long[] row = counts.get(function);
        synchronized (row) {
            row[outcome.ordinal()]++;
        }
        if (outcome == EventType.OK) {
            latencies.get(function).recordValue(Math.min(MAX_LATENCY_MICROS,
                    Math.max(0, TimeUnit.NANOSECONDS.toMicros(latencyNanos))));
        }
    }

    long count(OpFunction function, EventType outcome) {
        long[] row = counts.get(function);
        synchronized (row) {
            return row[outcome.ordinal()];
        }
    }

    /** A table of outcomes and latencies per function. */
    String render() {
        StringBuilder text = new StringBuilder();
        text.append(String.format(Locale.ROOT, "%-6s %9s %9s %9s   %-44s%n", "op", "ok", "fail", "info",
                "ok latency (ms): p50 / p99 / max"));
        long[] totals = new long[EventType.values().length];
        for (OpFunction function : OpFunction.values()) {
            Histogram histogram = latencies.get(function).copy();
            long ok = count(function, EventType.OK);
            long fail = count(function, EventType.FAIL);
            long info = count(function, EventType.INFO);
            totals[EventType.OK.ordinal()] += ok;
            totals[EventType.FAIL.ordinal()] += fail;
            totals[EventType.INFO.ordinal()] += info;
            String latency = histogram.getTotalCount() == 0 ? "-" : String.format(Locale.ROOT,
                    "%.1f / %.1f / %.1f", histogram.getValueAtPercentile(50) / 1000.0,
                    histogram.getValueAtPercentile(99) / 1000.0, histogram.getMaxValue() / 1000.0);
            text.append(String.format(Locale.ROOT, "%-6s %9d %9d %9d   %s%n", function.label(), ok, fail, info,
                    latency));
        }
        text.append(String.format(Locale.ROOT, "%-6s %9d %9d %9d%n", "total", totals[EventType.OK.ordinal()],
                totals[EventType.FAIL.ordinal()], totals[EventType.INFO.ordinal()]));
        return text.toString();
    }
}
