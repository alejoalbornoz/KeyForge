package io.keyforge.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Measures a JVM's live heap with {@code jcmd}: a full GC ({@code GC.run}), then {@code GC.heap_info}. Unlike RSS,
 * this excludes garbage and memory the GC keeps committed, so the difference between two readings is what the objects
 * created in between really cost. Attaching works across containers that share a PID namespace.
 */
final class ServerHeap {

    private static final Pattern USED = Pattern.compile("used (\\d+)([KMG])");

    private ServerHeap() {
    }

    /** The live heap of {@code pid} after a full GC, in bytes. */
    static long liveBytesAfterFullGc(int pid) throws IOException, InterruptedException {
        jcmd(pid, "GC.run");
        return parseUsedBytes(jcmd(pid, "GC.heap_info"));
    }

    /** Parses the first {@code used <n>K|M|G} of {@code GC.heap_info} output; G1, Parallel and ZGC all print one. */
    static long parseUsedBytes(String heapInfo) {
        Matcher matcher = USED.matcher(heapInfo);
        if (!matcher.find()) {
            throw new IllegalArgumentException("No heap usage in: " + heapInfo);
        }
        long value = Long.parseLong(matcher.group(1));
        return switch (matcher.group(2)) {
            case "K" -> value * 1024;
            case "M" -> value * 1024 * 1024;
            default -> value * 1024 * 1024 * 1024;
        };
    }

    private static String jcmd(int pid, String command) throws IOException, InterruptedException {
        Path jcmd = Path.of(System.getProperty("java.home"), "bin", "jcmd");
        Process process = new ProcessBuilder(List.of(jcmd.toString(), Integer.toString(pid), command))
                .redirectErrorStream(true)
                .start();
        byte[] output = process.getInputStream().readAllBytes();
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IOException("jcmd " + pid + " " + command + " failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
        return new String(output, StandardCharsets.UTF_8);
    }
}
