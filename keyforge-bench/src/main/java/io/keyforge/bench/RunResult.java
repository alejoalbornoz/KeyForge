package io.keyforge.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

/**
 * The outcome of one run. Latencies are in microseconds, memory in MiB; server figures are -1 when no server pid was
 * given.
 *
 * @param label             the run's label, usually the server type
 * @param transport         the load generator's Netty transport
 * @param connections       connections requested
 * @param connected         connections established
 * @param connectSeconds    time to establish them
 * @param openLoop          whether requests followed a fixed schedule (latency corrected for coordinated omission)
 * @param targetRate        offered requests per second in open loop, 0 in closed loop
 * @param achievedRate      replies per second during the measurement
 * @param requests          replies during the measurement
 * @param errors            error replies during the measurement
 * @param disconnects       connections lost while traffic was running
 * @param durationSeconds   length of the measurement
 * @param p50               median latency
 * @param p90               90th percentile latency
 * @param p99               99th percentile latency
 * @param p999              99.9th percentile latency
 * @param p9999             99.99th percentile latency
 * @param max               highest latency
 * @param mean              mean latency
 * @param timerLagP99       99th percentile of how late request timers fired (the load generator's own delay)
 * @param timerLagMax       highest timer lag
 * @param serverRssBaseMb   server RSS before the load generator connected
 * @param serverRssIdleMb   server RSS with every connection open and idle
 * @param serverRssPeakMb   highest server RSS sampled during the measurement
 * @param serverHeapBaseMb  server live heap after a full GC, before the load generator connected
 * @param serverHeapIdleMb  server live heap after a full GC, with every connection open and idle
 * @param serverThreadsIdle server OS threads with every connection open and idle
 * @param serverThreadsPeak most server OS threads sampled during the measurement
 * @param serverCpuCores    average CPU cores the server used during the measurement
 * @param clientCpuCores    average CPU cores the load generator used during the measurement, -1 if unknown
 */
public record RunResult(
        String label,
        String transport,
        int connections,
        int connected,
        double connectSeconds,
        boolean openLoop,
        int targetRate,
        double achievedRate,
        long requests,
        long errors,
        long disconnects,
        double durationSeconds,
        double p50,
        double p90,
        double p99,
        double p999,
        double p9999,
        double max,
        double mean,
        double timerLagP99,
        double timerLagMax,
        double serverRssBaseMb,
        double serverRssIdleMb,
        double serverRssPeakMb,
        double serverHeapBaseMb,
        double serverHeapIdleMb,
        int serverThreadsIdle,
        int serverThreadsPeak,
        double serverCpuCores,
        double clientCpuCores) {

    static final String CSV_HEADER = "label,transport,connections,connected,connect_s,mode,target_rps,achieved_rps,"
            + "requests,errors,disconnects,duration_s,p50_us,p90_us,p99_us,p999_us,p9999_us,max_us,mean_us,"
            + "timer_lag_p99_us,timer_lag_max_us,server_rss_base_mb,server_rss_idle_mb,server_rss_peak_mb,"
            + "server_heap_base_mb,server_heap_idle_mb,server_threads_idle,server_threads_peak,server_cpu_cores,"
            + "client_cpu_cores";

    String toCsv() {
        return String.join(",",
                label, transport, Integer.toString(connections), Integer.toString(connected), f1(connectSeconds),
                openLoop ? "open" : "closed", Integer.toString(targetRate), f1(achievedRate),
                Long.toString(requests), Long.toString(errors), Long.toString(disconnects), f1(durationSeconds),
                f1(p50), f1(p90), f1(p99), f1(p999), f1(p9999), f1(max), f1(mean), f1(timerLagP99), f1(timerLagMax),
                f1(serverRssBaseMb), f1(serverRssIdleMb), f1(serverRssPeakMb), f1(serverHeapBaseMb),
                f1(serverHeapIdleMb), Integer.toString(serverThreadsIdle),
                Integer.toString(serverThreadsPeak), f2(serverCpuCores), f2(clientCpuCores));
    }

    /** Appends this result to a CSV file, writing the header first if the file is new or empty. */
    void appendCsv(Path file) throws IOException {
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        boolean needsHeader = !Files.exists(file) || Files.size(file) == 0;
        String text = (needsHeader ? CSV_HEADER + "\n" : "") + toCsv() + "\n";
        Files.writeString(file, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** A human-readable summary. */
    String summary() {
        StringBuilder text = new StringBuilder();
        text.append(String.format(Locale.ROOT, "%s: %d/%d connections (%.1f s to connect), %s loop%s, %s transport%n",
                label, connected, connections, connectSeconds, openLoop ? "open" : "closed",
                openLoop ? " at " + targetRate + " req/s" : "", transport));
        text.append(String.format(Locale.ROOT, "  throughput  %.0f req/s over %.1f s (%d replies, %d errors, "
                + "%d disconnects)%n", achievedRate, durationSeconds, requests, errors, disconnects));
        text.append(String.format(Locale.ROOT, "  latency us  p50 %.0f  p90 %.0f  p99 %.0f  p99.9 %.0f  p99.99 %.0f  "
                + "max %.0f  mean %.0f%s%n", p50, p90, p99, p999, p9999, max, mean,
                openLoop ? "" : "  (closed loop: not corrected for coordinated omission)"));
        if (openLoop) {
            text.append(String.format(Locale.ROOT, "  timer lag   p99 %.0f us  max %.0f us%n",
                    timerLagP99, timerLagMax));
        }
        if (serverRssBaseMb >= 0) {
            text.append(String.format(Locale.ROOT, "  server      RSS %.0f MiB before connect, %.0f MiB idle, %.0f MiB "
                    + "peak; live heap %.1f MiB before connect, %.1f MiB idle; threads %d idle, %d peak; %.2f cores%n",
                    serverRssBaseMb, serverRssIdleMb, serverRssPeakMb, serverHeapBaseMb, serverHeapIdleMb,
                    serverThreadsIdle, serverThreadsPeak, serverCpuCores));
        }
        if (clientCpuCores >= 0) {
            text.append(String.format(Locale.ROOT, "  client      %.2f cores%n", clientCpuCores));
        }
        return text.toString();
    }

    private static String f1(double value) {
        return String.format(Locale.ROOT, "%.1f", value);
    }

    private static String f2(double value) {
        return String.format(Locale.ROOT, "%.2f", value);
    }
}
