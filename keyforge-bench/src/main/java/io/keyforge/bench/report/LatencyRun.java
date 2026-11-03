package io.keyforge.bench.report;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One open-loop run of the load generator, read back from the CSV it appends to ({@code latency.csv}). Latencies are
 * in microseconds and corrected for coordinated omission: each is measured from the request's intended send time.
 *
 * @param target          the run's label
 * @param connections     connections requested (which also name the run's files)
 * @param targetRate      offered requests per second
 * @param achievedRate    replies per second
 * @param errors          error replies
 * @param serverCpuCores  CPU cores the server process used, or -1 if it was not sampled
 */
record LatencyRun(
        String target,
        int connections,
        int targetRate,
        double achievedRate,
        long errors,
        double p50Us,
        double p99Us,
        double p999Us,
        double p9999Us,
        double maxUs,
        double serverCpuCores) {

    private static final Pattern MEASURED_SECOND =
            Pattern.compile("\\] measure\\s+\\d+s\\s+\\d+ req/s\\s+p50\\s+\\d+ us\\s+p99\\s+(\\d+) us");

    /** The name {@code LoadGenerator} gives this run's HdrHistogram percentile file. */
    String histogramFileName() {
        return String.format(Locale.ROOT, "%s-%dc-%drps.hgrm", target, connections, targetRate);
    }

    /** The name {@code compare-with-redis.sh} gives this run's console output. */
    String logFileName() {
        return String.format(Locale.ROOT, "%s-%dc-%drps.log", target, connections, targetRate);
    }

    /**
     * The median of the per-second p99s (in µs) the load generator printed while measuring: the tail of a typical
     * second, which an isolated stall cannot move. NaN if the output has no measured seconds.
     */
    static double medianSecondP99(List<String> output) {
        List<Double> p99s = new ArrayList<>();
        for (String line : output) {
            Matcher matcher = MEASURED_SECOND.matcher(line);
            if (matcher.find()) {
                p99s.add(Double.parseDouble(matcher.group(1)));
            }
        }
        if (p99s.isEmpty()) {
            return Double.NaN;
        }
        p99s.sort(null);
        int middle = p99s.size() / 2;
        return p99s.size() % 2 == 1 ? p99s.get(middle) : (p99s.get(middle - 1) + p99s.get(middle)) / 2;
    }

    /** Reads the open-loop rows of a load-generator CSV, finding columns by their header names. */
    static List<LatencyRun> read(Path file) throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            return List.of();
        }
        List<String> header = Arrays.asList(lines.getFirst().strip().split(","));
        List<LatencyRun> runs = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            if (line.isBlank()) {
                continue;
            }
            String[] fields = line.strip().split(",", -1);
            Columns columns = new Columns(header, fields);
            if (!columns.text("mode").equals("open")) {
                continue;
            }
            runs.add(new LatencyRun(
                    columns.text("label"),
                    (int) columns.number("connections"),
                    (int) columns.number("target_rps"),
                    columns.number("achieved_rps"),
                    (long) columns.number("errors"),
                    columns.number("p50_us"),
                    columns.number("p99_us"),
                    columns.number("p999_us"),
                    columns.number("p9999_us"),
                    columns.number("max_us"),
                    columns.number("server_cpu_cores")));
        }
        return runs;
    }

    private record Columns(List<String> header, String[] fields) {

        String text(String name) {
            int index = header.indexOf(name);
            if (index < 0 || index >= fields.length) {
                throw new IllegalArgumentException("No column '" + name + "' in " + header);
            }
            return fields[index];
        }

        double number(String name) {
            return Double.parseDouble(text(name));
        }
    }
}
