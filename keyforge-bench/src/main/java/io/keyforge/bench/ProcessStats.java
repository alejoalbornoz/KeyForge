package io.keyforge.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads a Linux process's resident memory, thread count and CPU time from {@code /proc/<pid>/status} and
 * {@code /proc/<pid>/stat}. The load generator samples the server this way when both run in containers that share a
 * PID namespace ({@code docker run --pid container:<server>}).
 */
final class ProcessStats {

    /** {@code USER_HZ}: the unit of CPU times in {@code /proc/<pid>/stat}. 100 on every mainstream Linux platform. */
    static final long CLOCK_TICKS_PER_SECOND = 100;

    private final Path directory;

    private ProcessStats(Path directory) {
        this.directory = directory;
    }

    /** Stats of process {@code pid}, as seen from this process's PID namespace. */
    static ProcessStats of(int pid) {
        return new ProcessStats(Path.of("/proc", Integer.toString(pid)));
    }

    /** Stats of the calling JVM itself. */
    static ProcessStats self() {
        return new ProcessStats(Path.of("/proc", "self"));
    }

    /** Whether {@code /proc} has the process; false on non-Linux systems. */
    boolean available() {
        return Files.isReadable(directory.resolve("status")) && Files.isReadable(directory.resolve("stat"));
    }

    Sample sample() throws IOException {
        return parse(Files.readString(directory.resolve("status")), Files.readString(directory.resolve("stat")));
    }

    /**
     * One reading.
     *
     * @param rssBytes     resident set size now ({@code VmRSS})
     * @param peakRssBytes highest resident set size since the process started ({@code VmHWM})
     * @param threads      operating-system threads ({@code Threads})
     * @param cpuTicks     user plus system CPU time, in {@link #CLOCK_TICKS_PER_SECOND} units
     */
    record Sample(long rssBytes, long peakRssBytes, int threads, long cpuTicks) {

        /** Average CPU cores used between {@code earlier} and this sample, {@code elapsedNanos} apart. */
        double coresSince(Sample earlier, long elapsedNanos) {
            double cpuSeconds = (double) (cpuTicks - earlier.cpuTicks) / CLOCK_TICKS_PER_SECOND;
            return cpuSeconds / (elapsedNanos / 1e9);
        }
    }

    static Sample parse(String status, String stat) {
        long rss = -1;
        long peakRss = -1;
        int threads = -1;
        for (String line : status.split("\n")) {
            if (line.startsWith("VmRSS:")) {
                rss = kilobytes(line);
            } else if (line.startsWith("VmHWM:")) {
                peakRss = kilobytes(line);
            } else if (line.startsWith("Threads:")) {
                threads = Integer.parseInt(line.substring("Threads:".length()).strip());
            }
        }
        if (rss < 0 || peakRss < 0 || threads < 0) {
            throw new IllegalArgumentException("status lacks VmRSS, VmHWM or Threads");
        }
        // The command name, field 2, is in parentheses and may contain spaces; fields after it are space-separated.
        String[] fields = stat.substring(stat.lastIndexOf(')') + 2).strip().split(" ");
        long userTicks = Long.parseLong(fields[11]);   // field 14, utime
        long systemTicks = Long.parseLong(fields[12]); // field 15, stime
        return new Sample(rss, peakRss, threads, userTicks + systemTicks);
    }

    private static long kilobytes(String line) {
        String value = line.substring(line.indexOf(':') + 1).strip();
        if (!value.endsWith(" kB")) {
            throw new IllegalArgumentException("Unexpected unit in '" + line + "'");
        }
        return Long.parseLong(value.substring(0, value.length() - 3).strip()) * 1024;
    }
}
