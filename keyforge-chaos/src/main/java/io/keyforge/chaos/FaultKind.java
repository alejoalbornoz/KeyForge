package io.keyforge.chaos;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/** The kinds of fault the nemesis can inject (ADR 0016). */
public enum FaultKind {
    /** Network partitions: chosen nodes drop every packet from chosen peers (iptables). */
    PARTITION,
    /** Server processes killed with SIGKILL, restarted when the fault ends. */
    KILL,
    /** Server processes stopped with SIGSTOP, resumed with SIGCONT. */
    PAUSE,
    /** Delay, with jitter, on every inbound Raft connection of chosen nodes (Toxiproxy). */
    LATENCY,
    /** A random share of the packets chosen nodes receive from their peers is dropped (iptables). */
    LOSS,
    /**
     * Checker self-test, not a realistic fault: every node is killed and a majority loses its data, which Raft cannot
     * survive. Acknowledged writes disappear, so the checker must report the run as not linearizable.
     */
    WIPE;

    /** The faults {@code all} stands for: every real fault, not the {@link #WIPE} self-test. */
    public static final Set<FaultKind> ALL = EnumSet.of(PARTITION, KILL, PAUSE, LATENCY, LOSS);

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Parses a comma-separated list such as {@code partition,kill}; {@code all} and {@code none} are shorthands. */
    public static Set<FaultKind> parseList(String value) {
        Set<FaultKind> kinds = EnumSet.noneOf(FaultKind.class);
        for (String item : value.split(",")) {
            String name = item.strip().toLowerCase(Locale.ROOT);
            switch (name) {
                case "", "none" -> {
                }
                case "all" -> kinds.addAll(ALL);
                default -> {
                    try {
                        kinds.add(valueOf(name.toUpperCase(Locale.ROOT)));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Option '--nemesis' does not know the fault '" + name
                                + "'; known faults: all, none, " + describe(EnumSet.allOf(FaultKind.class)));
                    }
                }
            }
        }
        return kinds;
    }

    /** The inverse of {@link #parseList}: {@code none} or the labels joined with commas. */
    public static String describe(Set<FaultKind> kinds) {
        return kinds.isEmpty() ? "none"
                : kinds.stream().sorted().map(FaultKind::label).collect(Collectors.joining(","));
    }
}
