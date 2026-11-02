package io.keyforge.chaos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Network partitions as Jepsen calls them, "grudges": for each node, the peers whose packets it drops. Every shape
 * here is symmetric (a node drops a peer exactly when the peer drops it) and lists every node, with an empty set for
 * nodes that hear from everyone.
 */
final class Partitions {

    private Partitions() {
    }

    /** Splits the cluster into components that cannot reach each other; each node must be in exactly one. */
    static Map<String, Set<String>> components(List<? extends Collection<String>> components) {
        Map<String, Set<String>> grudges = new LinkedHashMap<>();
        for (Collection<String> component : components) {
            for (String node : component) {
                Set<String> others = new LinkedHashSet<>();
                for (Collection<String> other : components) {
                    if (other != component) {
                        others.addAll(other);
                    }
                }
                if (grudges.put(node, others) != null) {
                    throw new IllegalArgumentException(node + " is in two components");
                }
            }
        }
        return grudges;
    }

    /** Cuts {@code node} off from every other node. */
    static Map<String, Set<String>> isolate(String node, List<String> nodes) {
        List<String> rest = new ArrayList<>(nodes);
        if (!rest.remove(node)) {
            throw new IllegalArgumentException(node + " is not one of " + nodes);
        }
        return components(List.of(List.of(node), rest));
    }

    /** Splits {@code order} into a minority, its first {@code n / 2} nodes, and a majority, the rest. */
    static Map<String, Set<String>> halves(List<String> order) {
        int split = order.size() / 2;
        return components(List.of(order.subList(0, split), order.subList(split, order.size())));
    }

    /**
     * Places the nodes on a ring in the given order; each node reaches only itself and its two neighbours. With five
     * nodes every node sees a majority, but no two nodes see the same majority.
     */
    static Map<String, Set<String>> ring(List<String> order) {
        int n = order.size();
        Map<String, Set<String>> grudges = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            Set<String> dropped = new LinkedHashSet<>();
            for (int j = 0; j < n; j++) {
                int distance = Math.min(Math.floorMod(i - j, n), Math.floorMod(j - i, n));
                if (distance > 1) {
                    dropped.add(order.get(j));
                }
            }
            grudges.put(order.get(i), dropped);
        }
        return grudges;
    }

    /**
     * Two halves that cannot reach each other, joined by one bridge node that reaches both: the first
     * {@code (n - 1) / 2} nodes of {@code order}, the bridge, and the rest. Each half plus the bridge is a majority.
     */
    static Map<String, Set<String>> bridge(List<String> order) {
        int bridge = (order.size() - 1) / 2;
        List<String> left = order.subList(0, bridge);
        List<String> right = order.subList(bridge + 1, order.size());
        Map<String, Set<String>> grudges = new LinkedHashMap<>();
        for (String node : order) {
            grudges.put(node, new LinkedHashSet<>(left.contains(node) ? right : right.contains(node) ? left
                    : List.of()));
        }
        return grudges;
    }

    /** Renders components as {@code {a,b} | {c,d,e}}. */
    static String describeComponents(List<? extends Collection<String>> components) {
        return components.stream().map(component -> String.join(",", component))
                .collect(Collectors.joining("} | {", "{", "}"));
    }
}
