package io.keyforge.chaos;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Options of one linearizability test, parsed from {@code --name=value} arguments.
 *
 * @param nodes          the cluster's nodes in order: each node's name (its Docker Compose service, such as
 *                       {@code kf1}) and the RESP address the runner reaches it at
 * @param project        the Docker Compose project the containers belong to
 * @param duration       how long the workload runs while the nemesis injects faults
 * @param clients        concurrent client processes; client {@code i} talks to node {@code i mod nodes}
 * @param rate           operations per second over all clients; each client paces itself
 * @param keys           keys in use at any time
 * @param opsPerKey      operations a key receives before it is retired and replaced with a fresh one (keeps each key's
 *                       history small enough to check)
 * @param getRatio       share of operations that are {@code GET}
 * @param incrRatio      share of operations that are {@code INCR}; the rest are {@code SET}
 * @param opTimeout      how long a client waits for a reply before recording the operation as indeterminate
 * @param connectTimeout how long a client waits for a connection; an operation that could not be sent failed
 * @param nemesis        the fault kinds to inject, empty for none
 * @param faultDuration  mean duration of a fault; each lasts between half and one and a half times this
 * @param calmDuration   mean pause between faults, drawn the same way
 * @param recovery       after the faults are healed, how long to wait for every node to serve before the final reads
 * @param seed           seeds every random choice: keys, operations, faults and their targets
 * @param output         directory the history ({@code history.json}) and the run summary ({@code run.txt}) go to
 */
public record ChaosOptions(
        Map<String, InetSocketAddress> nodes,
        String project,
        Duration duration,
        int clients,
        int rate,
        int keys,
        int opsPerKey,
        double getRatio,
        double incrRatio,
        Duration opTimeout,
        Duration connectTimeout,
        Set<FaultKind> nemesis,
        Duration faultDuration,
        Duration calmDuration,
        Duration recovery,
        long seed,
        Path output) {

    private static final Map<String, String> DEFAULTS = defaults();

    public ChaosOptions {
        nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
        nemesis = nemesis.isEmpty() ? Set.of() : Collections.unmodifiableSet(EnumSet.copyOf(nemesis));
        Objects.requireNonNull(project, "project");
        Objects.requireNonNull(output, "output");
        require(!nodes.isEmpty(), "nodes", "must name at least one node");
        require(duration.isPositive(), "duration-seconds", "must be positive");
        require(clients >= 1, "clients", "must be positive");
        require(rate >= 1, "rate", "must be positive");
        require(keys >= 1, "keys", "must be positive");
        require(opsPerKey >= 1, "ops-per-key", "must be positive");
        require(getRatio >= 0 && getRatio <= 1, "get-ratio", "must be between 0 and 1");
        require(incrRatio >= 0 && incrRatio <= 1, "incr-ratio", "must be between 0 and 1");
        require(getRatio + incrRatio <= 1, "incr-ratio", "plus --get-ratio must not exceed 1");
        require(opTimeout.isPositive(), "op-timeout-ms", "must be positive");
        require(connectTimeout.isPositive(), "connect-timeout-ms", "must be positive");
        require(faultDuration.isPositive(), "fault-seconds", "must be positive");
        require(calmDuration.isPositive(), "calm-seconds", "must be positive");
        require(!recovery.isNegative(), "recovery-seconds", "must not be negative");
    }

    /**
     * Parses {@code --name=value} arguments; unknown names are rejected.
     *
     * @throws IllegalArgumentException for unknown options or invalid values
     */
    public static ChaosOptions parse(String... args) {
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
        String seed = values.get("seed").strip();
        return new ChaosOptions(
                parseNodes(values.get("nodes")),
                values.get("project").strip(),
                Duration.ofSeconds(parseInt(values, "duration-seconds")),
                parseInt(values, "clients"),
                parseInt(values, "rate"),
                parseInt(values, "keys"),
                parseInt(values, "ops-per-key"),
                parseDouble(values, "get-ratio"),
                parseDouble(values, "incr-ratio"),
                Duration.ofMillis(parseInt(values, "op-timeout-ms")),
                Duration.ofMillis(parseInt(values, "connect-timeout-ms")),
                FaultKind.parseList(values.get("nemesis")),
                Duration.ofSeconds(parseInt(values, "fault-seconds")),
                Duration.ofSeconds(parseInt(values, "calm-seconds")),
                Duration.ofSeconds(parseInt(values, "recovery-seconds")),
                seed.isEmpty() ? System.nanoTime() ^ System.currentTimeMillis() : parseLong("seed", seed),
                Path.of(values.get("output")));
    }

    /** The options and their defaults, for {@code --help}. */
    public static Map<String, String> defaultValues() {
        return DEFAULTS;
    }

    /** The options as {@code name=value} pairs, recorded in the history and the run summary. */
    public Map<String, String> describe() {
        Map<String, String> description = new LinkedHashMap<>();
        StringBuilder nodeList = new StringBuilder();
        nodes.forEach((name, address) -> nodeList.append(nodeList.isEmpty() ? "" : ",").append(name).append('=')
                .append(address.getHostString()).append(':').append(address.getPort()));
        description.put("nodes", nodeList.toString());
        description.put("project", project);
        description.put("duration-seconds", Long.toString(duration.toSeconds()));
        description.put("clients", Integer.toString(clients));
        description.put("rate", Integer.toString(rate));
        description.put("keys", Integer.toString(keys));
        description.put("ops-per-key", Integer.toString(opsPerKey));
        description.put("get-ratio", Double.toString(getRatio));
        description.put("incr-ratio", Double.toString(incrRatio));
        description.put("op-timeout-ms", Long.toString(opTimeout.toMillis()));
        description.put("connect-timeout-ms", Long.toString(connectTimeout.toMillis()));
        description.put("nemesis", FaultKind.describe(nemesis));
        description.put("fault-seconds", Long.toString(faultDuration.toSeconds()));
        description.put("calm-seconds", Long.toString(calmDuration.toSeconds()));
        description.put("recovery-seconds", Long.toString(recovery.toSeconds()));
        description.put("seed", Long.toString(seed));
        description.put("output", output.toString());
        return description;
    }

    private static Map<String, String> defaults() {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put("nodes", "kf1=kf1:6379,kf2=kf2:6379,kf3=kf3:6379,kf4=kf4:6379,kf5=kf5:6379");
        defaults.put("project", "keyforge");
        defaults.put("duration-seconds", "300");
        defaults.put("clients", "15");
        defaults.put("rate", "150");
        defaults.put("keys", "5");
        defaults.put("ops-per-key", "300");
        defaults.put("get-ratio", "0.5");
        defaults.put("incr-ratio", "0.25");
        defaults.put("op-timeout-ms", "10000");
        defaults.put("connect-timeout-ms", "1000");
        defaults.put("nemesis", "partition");
        defaults.put("fault-seconds", "10");
        defaults.put("calm-seconds", "5");
        defaults.put("recovery-seconds", "60");
        defaults.put("seed", "");
        defaults.put("output", "history");
        return Collections.unmodifiableMap(defaults);
    }

    private static Map<String, InetSocketAddress> parseNodes(String value) {
        Map<String, InetSocketAddress> nodes = new LinkedHashMap<>();
        for (String entry : value.split(",")) {
            String trimmed = entry.strip();
            if (trimmed.isEmpty()) {
                continue;
            }
            int equals = trimmed.indexOf('=');
            int colon = trimmed.lastIndexOf(':');
            if (equals < 1 || colon < equals + 2 || colon == trimmed.length() - 1) {
                throw new IllegalArgumentException("Option '--nodes' needs name=host:port entries, got '" + trimmed
                        + "'");
            }
            String name = trimmed.substring(0, equals);
            int port = parseInt("nodes", trimmed.substring(colon + 1));
            require(port > 0 && port <= 65535, "nodes", "ports must be between 1 and 65535");
            if (nodes.put(name, InetSocketAddress.createUnresolved(trimmed.substring(equals + 1, colon), port))
                    != null) {
                throw new IllegalArgumentException("Option '--nodes' names " + name + " twice");
            }
        }
        return nodes;
    }

    private static int parseInt(Map<String, String> values, String name) {
        return parseInt(name, values.get(name));
    }

    private static int parseInt(String name, String value) {
        try {
            return Integer.parseInt(value.strip());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Option '--" + name + "' must be an integer, got '" + value.strip()
                    + "'");
        }
    }

    private static long parseLong(String name, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Option '--" + name + "' must be an integer, got '" + value + "'");
        }
    }

    private static double parseDouble(Map<String, String> values, String name) {
        String value = values.get(name).strip().toLowerCase(Locale.ROOT);
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Option '--" + name + "' must be a number, got '" + value + "'");
        }
    }

    private static void require(boolean condition, String name, String message) {
        if (!condition) {
            throw new IllegalArgumentException("Option '--" + name + "' " + message);
        }
    }
}
