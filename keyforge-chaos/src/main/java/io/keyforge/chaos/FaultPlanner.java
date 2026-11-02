package io.keyforge.chaos;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.random.RandomGenerator;

/**
 * Draws the nemesis's next fault: a kind among those enabled, then a shape or targets and parameters, all from one
 * seeded random generator. Faults aimed at the leader take the leader the nemesis just observed, or a random node if
 * there is none.
 */
final class FaultPlanner {

    private static final List<String> PARTITION_SHAPES =
            List.of("isolate-node", "isolate-leader", "halves", "ring", "bridge");
    private static final List<String> PROCESS_TARGETS = List.of("one", "leader", "minority", "majority");
    private static final List<String> NETWORK_TARGETS = List.of("one", "minority", "all");

    private final List<String> nodes;
    private final List<FaultKind> kinds;
    private final RandomGenerator random;

    FaultPlanner(List<String> nodes, Set<FaultKind> kinds, RandomGenerator random) {
        if (kinds.isEmpty()) {
            throw new IllegalArgumentException("no fault kinds to choose from");
        }
        this.nodes = List.copyOf(nodes);
        this.kinds = kinds.stream().sorted().toList();
        this.random = random;
    }

    /** The next fault, given the current leader if one is known. */
    Fault next(Optional<String> leader) {
        return switch (pick(kinds)) {
            case PARTITION -> partition(leader);
            case KILL -> {
                String target = pick(PROCESS_TARGETS);
                yield new Fault.Kill(target, processTargets(target, leader));
            }
            case PAUSE -> {
                String target = pick(PROCESS_TARGETS);
                yield new Fault.Pause(target, processTargets(target, leader));
            }
            case LATENCY -> {
                String target = pick(NETWORK_TARGETS);
                int delay = 20 + random.nextInt(281);
                yield new Fault.Latency(target, networkTargets(target), delay, delay / 2);
            }
            case LOSS -> {
                String target = pick(NETWORK_TARGETS);
                yield new Fault.Loss(target, networkTargets(target), 5 + random.nextInt(21));
            }
            case WIPE -> new Fault.Wipe(shuffled().subList(0, majority()));
        };
    }

    private Fault.Partition partition(Optional<String> leader) {
        String shape = pick(PARTITION_SHAPES);
        List<String> order = shuffled();
        return switch (shape) {
            case "isolate-node", "isolate-leader" -> {
                String node = shape.equals("isolate-leader") ? leader.orElse(order.getFirst()) : order.getFirst();
                List<String> rest = new ArrayList<>(nodes);
                rest.remove(node);
                yield new Fault.Partition(shape, Partitions.isolate(node, nodes),
                        Partitions.describeComponents(List.of(List.of(node), rest)));
            }
            case "halves" -> {
                int split = order.size() / 2;
                List<List<String>> components = List.of(order.subList(0, split), order.subList(split, order.size()));
                yield new Fault.Partition(shape, Partitions.halves(order), Partitions.describeComponents(components));
            }
            case "ring" -> new Fault.Partition(shape, Partitions.ring(order),
                    String.join("-", order) + "-" + order.getFirst());
            case "bridge" -> {
                int bridge = (order.size() - 1) / 2;
                yield new Fault.Partition(shape, Partitions.bridge(order),
                        Partitions.describeComponents(List.of(order.subList(0, bridge), order.subList(bridge + 1,
                                order.size()))) + " bridged by " + order.get(bridge));
            }
            default -> throw new IllegalStateException(shape);
        };
    }

    private List<String> processTargets(String target, Optional<String> leader) {
        List<String> order = shuffled();
        return switch (target) {
            case "one" -> List.of(order.getFirst());
            case "leader" -> List.of(leader.orElse(order.getFirst()));
            case "minority" -> order.subList(0, Math.max(1, nodes.size() - majority()));
            case "majority" -> order.subList(0, majority());
            default -> throw new IllegalStateException(target);
        };
    }

    private List<String> networkTargets(String target) {
        List<String> order = shuffled();
        return switch (target) {
            case "one" -> List.of(order.getFirst());
            case "minority" -> order.subList(0, Math.max(1, nodes.size() - majority()));
            case "all" -> List.copyOf(nodes);
            default -> throw new IllegalStateException(target);
        };
    }

    private int majority() {
        return nodes.size() / 2 + 1;
    }

    private List<String> shuffled() {
        List<String> order = new ArrayList<>(nodes);
        for (int i = order.size() - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            order.set(i, order.set(j, order.get(i)));
        }
        return List.copyOf(order);
    }

    private <T> T pick(List<T> choices) {
        return choices.get(random.nextInt(choices.size()));
    }
}
