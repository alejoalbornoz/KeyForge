package io.keyforge.chaos;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** One fault the nemesis injects and later heals. */
sealed interface Fault {

    FaultKind kind();

    /** The fault's shape and targets, for the history and the logs. */
    String description();

    /**
     * Each node drops every packet from the peers listed for it.
     *
     * @param shape   {@code isolate-node}, {@code isolate-leader}, {@code halves}, {@code ring} or {@code bridge}
     * @param grudges for each node, the peers it drops (see {@link Partitions})
     * @param layout  who can reach whom, for people
     */
    record Partition(String shape, Map<String, Set<String>> grudges, String layout) implements Fault {

        public Partition {
            grudges = Map.copyOf(grudges);
        }

        @Override
        public FaultKind kind() {
            return FaultKind.PARTITION;
        }

        @Override
        public String description() {
            return shape + " " + layout;
        }
    }

    /** The nodes' server processes are killed with SIGKILL and restarted when the fault is healed. */
    record Kill(String target, List<String> nodes) implements Fault {

        public Kill {
            nodes = List.copyOf(nodes);
        }

        @Override
        public FaultKind kind() {
            return FaultKind.KILL;
        }

        @Override
        public String description() {
            return target + " " + nodes;
        }
    }

    /** The nodes' server processes are stopped with SIGSTOP and resumed with SIGCONT when the fault is healed. */
    record Pause(String target, List<String> nodes) implements Fault {

        public Pause {
            nodes = List.copyOf(nodes);
        }

        @Override
        public FaultKind kind() {
            return FaultKind.PAUSE;
        }

        @Override
        public String description() {
            return target + " " + nodes;
        }
    }

    /** Every inbound Raft connection of the nodes is delayed, in both directions, by {@code delay ± jitter} ms. */
    record Latency(String target, List<String> nodes, int delayMillis, int jitterMillis) implements Fault {

        public Latency {
            nodes = List.copyOf(nodes);
        }

        @Override
        public FaultKind kind() {
            return FaultKind.LATENCY;
        }

        @Override
        public String description() {
            return target + " " + nodes + " " + delayMillis + " +/- " + jitterMillis + " ms";
        }
    }

    /** The nodes drop {@code percent} % of the packets they receive from their peers, chosen at random. */
    record Loss(String target, List<String> nodes, int percent) implements Fault {

        public Loss {
            nodes = List.copyOf(nodes);
        }

        @Override
        public FaultKind kind() {
            return FaultKind.LOSS;
        }

        @Override
        public String description() {
            return target + " " + nodes + " " + percent + "%";
        }
    }

    /**
     * Checker self-test: every node is killed, {@code wiped} (a majority) lose their data and restart first and elect
     * a leader among themselves; then the others restart.
     */
    record Wipe(List<String> wiped) implements Fault {

        public Wipe {
            wiped = List.copyOf(wiped);
        }

        @Override
        public FaultKind kind() {
            return FaultKind.WIPE;
        }

        @Override
        public String description() {
            return "majority " + wiped + " loses its data";
        }
    }
}
