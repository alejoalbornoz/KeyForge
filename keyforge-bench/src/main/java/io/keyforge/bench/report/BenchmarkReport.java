package io.keyforge.bench.report;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Turns the output of {@code keyforge-bench/scripts/compare-with-redis.sh} into charts and tables: it reads
 * {@code redis-benchmark.csv}, {@code latency.csv} and the {@code .hgrm} files of a results directory and writes
 * {@code charts/throughput-pipeline-<n>.svg}, {@code charts/latency-<rate>rps.svg} and {@code charts/summary.md}.
 *
 * <pre>
 * java -cp keyforge-bench-all.jar io.keyforge.bench.report.BenchmarkReport docs/benchmarks/results/.../redis-comparison
 * </pre>
 */
public final class BenchmarkReport {

    /** Known targets in their fixed order, which also fixes each one's series color across every chart. */
    private static final Map<String, String> KNOWN_TARGETS = knownTargets();
    private static final List<String> COMMAND_ORDER = List.of("SET", "GET", "INCR");

    private BenchmarkReport() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: BenchmarkReport <results directory>");
            System.exit(2);
            return;
        }
        for (Path file : write(Path.of(args[0]))) {
            System.out.println("Wrote " + file);
        }
    }

    /** Writes the charts and the summary of the results in {@code directory}; returns the files written. */
    static List<Path> write(Path directory) throws IOException {
        Path benchmarkCsv = directory.resolve("redis-benchmark.csv");
        Path latencyCsv = directory.resolve("latency.csv");
        List<RedisBenchmarkRow> rows = Files.exists(benchmarkCsv) ? RedisBenchmarkRow.read(benchmarkCsv) : List.of();
        List<LatencyRun> runs = Files.exists(latencyCsv) ? LatencyRun.read(latencyCsv) : List.of();
        if (rows.isEmpty() && runs.isEmpty()) {
            throw new IOException("No results in " + directory + " (redis-benchmark.csv, latency.csv)");
        }
        Targets targets = new Targets();
        rows.forEach(row -> targets.add(row.target()));
        runs.forEach(run -> targets.add(run.target()));

        Path charts = directory.resolve("charts");
        Files.createDirectories(charts);
        List<Path> written = new ArrayList<>();
        for (int pipeline : new TreeSet<>(rows.stream().map(RedisBenchmarkRow::pipeline).toList())) {
            BarChart chart = throughputChart(rows, pipeline, targets);
            written.add(writeString(charts.resolve("throughput-pipeline-" + pipeline + ".svg"), chart.toSvg()));
        }
        for (int rate : new TreeSet<>(runs.stream().map(LatencyRun::targetRate).toList())) {
            List<PercentileChart.Line> lines = new ArrayList<>();
            for (String target : targets.names()) {
                LatencyRun run = last(runs, target, rate);
                Path histogram = run == null ? null : directory.resolve(run.histogramFileName());
                if (histogram != null && Files.exists(histogram)) {
                    lines.add(new PercentileChart.Line(targets.displayName(target), targets.slot(target),
                            PercentileDistribution.read(histogram).scaled(0.001)));
                }
            }
            if (!lines.isEmpty()) {
                PercentileChart chart = new PercentileChart(
                        String.format(Locale.ROOT, "Open-loop latency at %,d requests/s", rate),
                        connectionsNote(runs, rate) + "80% GET, 20% SET, corrected for coordinated omission "
                                + "(lower is better)",
                        lines);
                written.add(writeString(charts.resolve("latency-" + rate + "rps.svg"), chart.toSvg()));
            }
        }
        Map<LatencyRun, Double> medianSecondP99 = new LinkedHashMap<>();
        for (LatencyRun run : runs) {
            Path output = directory.resolve(run.logFileName());
            if (Files.exists(output)) {
                // Latin-1 accepts any bytes: the output can hold jcmd's error text when the server is not a JVM.
                List<String> lines = Files.readAllLines(output, StandardCharsets.ISO_8859_1);
                medianSecondP99.put(run, LatencyRun.medianSecondP99(lines));
            }
        }
        written.add(writeString(charts.resolve("summary.md"), summary(rows, runs, targets, medianSecondP99)));
        return written;
    }

    static BarChart throughputChart(List<RedisBenchmarkRow> rows, int pipeline, Targets targets) {
        List<RedisBenchmarkRow> selected = rows.stream().filter(row -> row.pipeline() == pipeline).toList();
        List<String> categories = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (String target : targets.names()) {
            if (selected.stream().anyMatch(row -> row.target().equals(target))) {
                names.add(target);
                categories.add(targets.displayName(target));
            }
        }
        List<BarChart.Series> series = new ArrayList<>();
        for (String test : commands(selected)) {
            List<Double> values = new ArrayList<>();
            for (String target : names) {
                RedisBenchmarkRow row = last(selected, target, test);
                values.add(row == null ? Double.NaN : row.rps());
            }
            series.add(new BarChart.Series(test, values));
        }
        int clients = selected.isEmpty() ? 0 : selected.getFirst().clients();
        String title = pipeline == 1
                ? "redis-benchmark throughput, no pipelining"
                : "redis-benchmark throughput, pipeline of " + pipeline;
        return new BarChart(title,
                String.format(Locale.ROOT, "Requests per second, %d clients (higher is better)", clients),
                "req/s", categories, series);
    }

    /** @param medianSecondP99 per open-loop run, the median of its per-second p99s in µs, where its output was kept */
    static String summary(List<RedisBenchmarkRow> rows, List<LatencyRun> runs, Targets targets,
                          Map<LatencyRun, Double> medianSecondP99) {
        StringBuilder md = new StringBuilder();
        if (!rows.isEmpty()) {
            List<String> commands = commands(rows);
            md.append("### redis-benchmark: throughput\n\n");
            md.append("Requests per second (closed loop). Server CPU is the time all of the target's server "
                    + "containers spent per request.\n\n");
            md.append("| Target | Pipeline |");
            commands.forEach(command -> md.append(' ').append(command).append(" |"));
            md.append(" Server CPU per request (").append(String.join(" / ", commands)).append(") |\n");
            md.append("| --- | ---: |").append(" ---: |".repeat(commands.size())).append(" ---: |\n");
            for (int pipeline : new TreeSet<>(rows.stream().map(RedisBenchmarkRow::pipeline).toList())) {
                for (String target : targets.names()) {
                    if (rows.stream().noneMatch(r -> r.target().equals(target) && r.pipeline() == pipeline)) {
                        continue;
                    }
                    md.append("| ").append(targets.displayName(target)).append(" | ").append(pipeline).append(" |");
                    List<String> cpu = new ArrayList<>();
                    for (String command : commands) {
                        RedisBenchmarkRow row = last(rows, target, pipeline, command);
                        md.append(' ').append(row == null ? "–" : String.format(Locale.ROOT, "%,.0f", row.rps()))
                                .append(" |");
                        cpu.add(row == null ? "–" : String.format(Locale.ROOT, "%.1f", row.serverCpuUsPerRequest()));
                    }
                    md.append(' ').append(String.join(" / ", cpu)).append(" µs |\n");
                }
            }
            md.append("\n### redis-benchmark: closed-loop latency\n\n");
            md.append("redis-benchmark's own p50 / p99 in ms. A closed loop waits for each reply before sending the "
                    + "next request, so these hide stalls (coordinated omission); see the open-loop table.\n\n");
            md.append("| Target | Pipeline |");
            commands.forEach(command -> md.append(' ').append(command).append(" p50 / p99 |"));
            md.append('\n').append("| --- | ---: |").append(" ---: |".repeat(commands.size())).append('\n');
            for (int pipeline : new TreeSet<>(rows.stream().map(RedisBenchmarkRow::pipeline).toList())) {
                for (String target : targets.names()) {
                    if (rows.stream().noneMatch(r -> r.target().equals(target) && r.pipeline() == pipeline)) {
                        continue;
                    }
                    md.append("| ").append(targets.displayName(target)).append(" | ").append(pipeline).append(" |");
                    for (String command : commands) {
                        RedisBenchmarkRow row = last(rows, target, pipeline, command);
                        md.append(' ').append(row == null ? "–"
                                : String.format(Locale.ROOT, "%.3f / %.3f", row.p50Ms(), row.p99Ms())).append(" |");
                    }
                    md.append('\n');
                }
            }
        }
        if (!runs.isEmpty()) {
            if (!md.isEmpty()) {
                md.append('\n');
            }
            md.append("### Load generator: open-loop latency\n\n");
            md.append("Latency in ms from each request's intended send time (no coordinated omission), "
                    + "80% GET / 20% SET. *Median 1-s p99* is the median of the per-second p99s: the tail of a "
                    + "typical second, which an isolated stall cannot move.\n\n");
            md.append("| Target | Offered (req/s) | Delivered (req/s) | p50 | p99 | p99.9 | p99.99 | max "
                    + "| Median 1-s p99 | Server CPU (cores) | Errors |\n");
            md.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
            for (int rate : new TreeSet<>(runs.stream().map(LatencyRun::targetRate).toList())) {
                for (String target : targets.names()) {
                    LatencyRun run = last(runs, target, rate);
                    if (run == null) {
                        continue;
                    }
                    double secondP99 = medianSecondP99.getOrDefault(run, Double.NaN);
                    md.append(String.format(Locale.ROOT,
                            "| %s | %,d | %,.0f | %s | %s | %s | %s | %s | %s | %s | %d |%n",
                            targets.displayName(target), rate, run.achievedRate(), ms(run.p50Us()), ms(run.p99Us()),
                            ms(run.p999Us()), ms(run.p9999Us()), ms(run.maxUs()),
                            Double.isNaN(secondP99) ? "–" : ms(secondP99),
                            run.serverCpuCores() < 0 ? "–" : String.format(Locale.ROOT, "%.2f", run.serverCpuCores()),
                            run.errors()));
                }
            }
        }
        return md.toString();
    }

    /** Microseconds as milliseconds with three significant digits. */
    static String ms(double micros) {
        double millis = micros / 1000;
        if (millis >= 100) {
            return String.format(Locale.ROOT, "%,.0f", millis);
        }
        if (millis >= 10) {
            return String.format(Locale.ROOT, "%.1f", millis);
        }
        if (millis >= 1) {
            return String.format(Locale.ROOT, "%.2f", millis);
        }
        return String.format(Locale.ROOT, "%.3f", millis);
    }

    private static String connectionsNote(List<LatencyRun> runs, int rate) {
        Set<Integer> connections = new TreeSet<>();
        runs.stream().filter(run -> run.targetRate() == rate).forEach(run -> connections.add(run.connections()));
        return connections.size() == 1 ? connections.iterator().next() + " connections, " : "";
    }

    private static List<String> commands(List<RedisBenchmarkRow> rows) {
        Set<String> commands = new LinkedHashSet<>();
        COMMAND_ORDER.stream().filter(c -> rows.stream().anyMatch(row -> row.test().equals(c))).forEach(commands::add);
        rows.forEach(row -> commands.add(row.test()));
        return List.copyOf(commands);
    }

    private static RedisBenchmarkRow last(List<RedisBenchmarkRow> rows, String target, String test) {
        RedisBenchmarkRow found = null;
        for (RedisBenchmarkRow row : rows) {
            if (row.target().equals(target) && row.test().equals(test)) {
                found = row;
            }
        }
        return found;
    }

    private static RedisBenchmarkRow last(List<RedisBenchmarkRow> rows, String target, int pipeline, String test) {
        return last(rows.stream().filter(row -> row.pipeline() == pipeline).toList(), target, test);
    }

    private static LatencyRun last(List<LatencyRun> runs, String target, int rate) {
        LatencyRun found = null;
        for (LatencyRun run : runs) {
            if (run.target().equals(target) && run.targetRate() == rate) {
                found = run;
            }
        }
        return found;
    }

    private static Path writeString(Path file, String content) throws IOException {
        Files.writeString(file, content, StandardCharsets.UTF_8);
        return file;
    }

    private static Map<String, String> knownTargets() {
        Map<String, String> targets = new LinkedHashMap<>();
        targets.put("redis", "Redis");
        targets.put("redis-aof", "Redis, AOF everysec");
        targets.put("keyforge-memory", "KeyForge, memory");
        targets.put("keyforge-lsm", "KeyForge, LSM");
        targets.put("keyforge-lsm-vt", "KeyForge, LSM, virtual threads");
        targets.put("cluster-leader", "KeyForge 3 nodes, leader");
        targets.put("cluster-follower", "KeyForge 3 nodes, follower");
        return targets;
    }

    /** The targets of a report: known ones in their fixed order first, then others in order of appearance. */
    static final class Targets {
        private final Set<String> seen = new LinkedHashSet<>();

        void add(String target) {
            seen.add(target);
        }

        List<String> names() {
            List<String> names = new ArrayList<>();
            KNOWN_TARGETS.keySet().stream().filter(seen::contains).forEach(names::add);
            seen.stream().filter(target -> !KNOWN_TARGETS.containsKey(target)).forEach(names::add);
            return names;
        }

        String displayName(String target) {
            return KNOWN_TARGETS.getOrDefault(target, target);
        }

        /** A known target keeps its color in every report; others take the slots after them. */
        int slot(String target) {
            List<String> known = new ArrayList<>(KNOWN_TARGETS.keySet());
            int index = known.indexOf(target);
            if (index < 0) {
                List<String> others = seen.stream().filter(t -> !KNOWN_TARGETS.containsKey(t)).toList();
                index = known.size() + others.indexOf(target);
            }
            return Svg.slot(index);
        }
    }
}
