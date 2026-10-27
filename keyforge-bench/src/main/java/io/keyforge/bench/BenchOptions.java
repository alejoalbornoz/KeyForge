package io.keyforge.bench;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Options of one load-generator run, parsed from {@code --name=value} arguments.
 *
 * @param host               server host
 * @param port               server port
 * @param connections        connections opened before traffic starts; each has at most one request in flight
 * @param rate               offered requests per second over all connections (open loop); 0 means closed loop, where
 *                           each connection sends its next request as soon as the previous reply arrives
 * @param warmup             traffic before measuring; its latencies are discarded
 * @param duration           measured traffic
 * @param settle             pause between the last connect and the idle memory sample
 * @param threads            client event-loop threads
 * @param transport          {@code auto} (epoll when available, else NIO), {@code nio} or {@code epoll}
 * @param connectConcurrency connection attempts in flight at once
 * @param keyspace           distinct keys
 * @param valueSize          bytes per value
 * @param getRatio           fraction of requests that are {@code GET}; the rest are {@code SET}
 * @param prepopulate        whether to {@code SET} every key before the run
 * @param label              names the run in the results, for example the server type
 * @param output             CSV file the result row is appended to, or {@code null}
 * @param serverPid          pid whose {@code /proc} entries are sampled for memory, threads and CPU; 0 for none
 */
public record BenchOptions(
        String host,
        int port,
        int connections,
        int rate,
        Duration warmup,
        Duration duration,
        Duration settle,
        int threads,
        String transport,
        int connectConcurrency,
        int keyspace,
        int valueSize,
        double getRatio,
        boolean prepopulate,
        String label,
        Path output,
        int serverPid) {

    private static final Map<String, String> DEFAULTS = defaults();

    public BenchOptions {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(transport, "transport");
        Objects.requireNonNull(label, "label");
        require(port > 0 && port <= 65535, "port", "must be between 1 and 65535");
        require(connections >= 1, "connections", "must be positive");
        require(rate >= 0, "rate", "must not be negative");
        require(!warmup.isNegative(), "warmup-seconds", "must not be negative");
        require(duration.isPositive(), "duration-seconds", "must be positive");
        require(!settle.isNegative(), "settle-seconds", "must not be negative");
        require(threads >= 1, "threads", "must be positive");
        require(transport.equals("auto") || transport.equals("nio") || transport.equals("epoll"),
                "transport", "must be auto, nio or epoll");
        require(connectConcurrency >= 1, "connect-concurrency", "must be positive");
        require(keyspace >= 1, "keyspace", "must be positive");
        require(valueSize >= 0, "value-size", "must not be negative");
        require(getRatio >= 0 && getRatio <= 1, "get-ratio", "must be between 0 and 1");
        require(serverPid >= 0, "server-pid", "must not be negative");
    }

    /** Whether the run is open loop: requests are sent on a fixed schedule, not as fast as replies come back. */
    public boolean openLoop() {
        return rate > 0;
    }

    /** The interval between a connection's intended send times in open loop. */
    public long intervalNanos() {
        return openLoop() ? Math.max(1, Math.round(1e9 * connections / rate)) : 0;
    }

    /**
     * Parses {@code --name=value} arguments; unknown names are rejected.
     *
     * @throws IllegalArgumentException for unknown options or invalid values
     */
    public static BenchOptions parse(String... args) {
        Map<String, String> values = new LinkedHashMap<>(DEFAULTS);
        for (String arg : args) {
            int equals = arg.indexOf('=');
            if (!arg.startsWith("--") || equals < 3) {
                throw new IllegalArgumentException("Unexpected argument '" + arg + "'; expected --<name>=<value>");
            }
            String name = arg.substring(2, equals);
            if (!values.containsKey(name)) {
                throw new IllegalArgumentException("Unknown option '--" + name + "'; known options: "
                        + DEFAULTS.keySet());
            }
            values.put(name, arg.substring(equals + 1));
        }
        String output = values.get("output");
        return new BenchOptions(
                values.get("host"),
                parseInt(values, "port"),
                parseInt(values, "connections"),
                parseInt(values, "rate"),
                Duration.ofSeconds(parseInt(values, "warmup-seconds")),
                Duration.ofSeconds(parseInt(values, "duration-seconds")),
                Duration.ofSeconds(parseInt(values, "settle-seconds")),
                parseInt(values, "threads"),
                values.get("transport").toLowerCase(Locale.ROOT),
                parseInt(values, "connect-concurrency"),
                parseInt(values, "keyspace"),
                parseInt(values, "value-size"),
                parseDouble(values, "get-ratio"),
                parseBoolean(values, "prepopulate"),
                values.get("label"),
                output.isEmpty() ? null : Path.of(output),
                parseInt(values, "server-pid"));
    }

    /** The options and their defaults, for {@code --help}. */
    public static Map<String, String> defaultValues() {
        return DEFAULTS;
    }

    private static Map<String, String> defaults() {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put("host", "127.0.0.1");
        defaults.put("port", "6379");
        defaults.put("connections", "100");
        defaults.put("rate", "0");
        defaults.put("warmup-seconds", "10");
        defaults.put("duration-seconds", "30");
        defaults.put("settle-seconds", "5");
        defaults.put("threads", Integer.toString(Runtime.getRuntime().availableProcessors()));
        defaults.put("transport", "auto");
        defaults.put("connect-concurrency", "256");
        defaults.put("keyspace", "100000");
        defaults.put("value-size", "64");
        defaults.put("get-ratio", "0.8");
        defaults.put("prepopulate", "true");
        defaults.put("label", "keyforge");
        defaults.put("output", "");
        defaults.put("server-pid", "0");
        return Collections.unmodifiableMap(defaults);
    }

    private static int parseInt(Map<String, String> values, String name) {
        String value = values.get(name).strip();
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Option '--" + name + "' must be an integer, got '" + value + "'");
        }
    }

    private static double parseDouble(Map<String, String> values, String name) {
        String value = values.get(name).strip();
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Option '--" + name + "' must be a number, got '" + value + "'");
        }
    }

    private static boolean parseBoolean(Map<String, String> values, String name) {
        String value = values.get(name).strip().toLowerCase(Locale.ROOT);
        return switch (value) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(
                    "Option '--" + name + "' must be true or false, got '" + value + "'");
        };
    }

    private static void require(boolean condition, String name, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Option '--" + name + "' " + message);
        }
    }
}
