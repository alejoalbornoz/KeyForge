package io.keyforge.bench.report;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * One {@code redis-benchmark} result: a target, a pipeline depth and a command, as {@code compare-with-redis.sh}
 * writes them to {@code redis-benchmark.csv}. Latencies are redis-benchmark's own, in milliseconds; they come from a
 * closed loop and so suffer from coordinated omission.
 *
 * @param target                 what was measured, for example {@code redis} or {@code keyforge-lsm}
 * @param pipeline               requests each client sends before reading replies ({@code -P})
 * @param clients                parallel connections ({@code -c})
 * @param requests               requests sent ({@code -n})
 * @param test                   the command, as redis-benchmark names it ({@code SET}, {@code GET}, {@code INCR})
 * @param rps                    requests per second
 * @param serverCpuUsPerRequest  server CPU time per request in microseconds, over every server container of the target
 */
record RedisBenchmarkRow(
        String target,
        int pipeline,
        int clients,
        long requests,
        String test,
        double rps,
        double avgMs,
        double minMs,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        double maxMs,
        double serverCpuUsPerRequest) {

    static final String CSV_HEADER = "target,pipeline,clients,requests,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,"
            + "max_ms,server_cpu_us_per_request";

    /** Reads every row of a {@code redis-benchmark.csv}; the first line must be {@link #CSV_HEADER}. */
    static List<RedisBenchmarkRow> read(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.isEmpty() || !lines.getFirst().strip().equals(CSV_HEADER)) {
            throw new IOException(file + " does not start with the header " + CSV_HEADER);
        }
        return lines.stream().skip(1).map(String::strip).filter(line -> !line.isEmpty())
                .map(RedisBenchmarkRow::parse).toList();
    }

    static RedisBenchmarkRow parse(String line) {
        String[] fields = line.split(",", -1);
        if (fields.length != 13) {
            throw new IllegalArgumentException("Expected 13 fields, got " + fields.length + ": " + line);
        }
        return new RedisBenchmarkRow(
                fields[0], Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Long.parseLong(fields[3]),
                fields[4].toUpperCase(Locale.ROOT), number(fields[5]), number(fields[6]), number(fields[7]),
                number(fields[8]), number(fields[9]), number(fields[10]), number(fields[11]), number(fields[12]));
    }

    private static double number(String field) {
        return field.isBlank() ? Double.NaN : Double.parseDouble(field);
    }
}
