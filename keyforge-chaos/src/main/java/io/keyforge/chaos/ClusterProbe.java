package io.keyforge.chaos;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.keyforge.protocol.RespBulkString;
import io.keyforge.protocol.RespSimpleString;
import io.keyforge.protocol.RespValue;

/** Asks the nodes, over RESP, who leads and whether they serve ({@code INFO raft}). */
final class ClusterProbe implements Leadership {

    private static final long POLL_MILLIS = 100;

    /**
     * What one node said about itself.
     *
     * @param role {@code leader}, {@code follower}, {@code candidate}, or {@code down} if it did not answer
     * @param term its current term, -1 if it did not answer
     */
    record Status(String role, long term) {

        static final Status DOWN = new Status("down", -1);
    }

    private final Map<String, InetSocketAddress> nodes;
    private final Duration timeout;

    /** @param timeout how long to wait for each node's answer */
    ClusterProbe(Map<String, InetSocketAddress> nodes, Duration timeout) {
        this.nodes = Map.copyOf(nodes);
        this.timeout = timeout;
    }

    /** Asks every node at once for its role and term. */
    Map<String, Status> statuses(Collection<String> names) {
        Map<String, Status> statuses = new LinkedHashMap<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Map<String, Future<Status>> answers = new LinkedHashMap<>();
            for (String name : names) {
                answers.put(name, executor.submit(() -> status(nodes.get(name))));
            }
            for (Map.Entry<String, Future<Status>> answer : answers.entrySet()) {
                try {
                    statuses.put(answer.getKey(), answer.getValue().get());
                } catch (ExecutionException e) {
                    statuses.put(answer.getKey(), Status.DOWN);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    statuses.put(answer.getKey(), Status.DOWN);
                }
            }
        }
        return statuses;
    }

    /** The leader of the highest term among the nodes that answer, if any claims to lead. */
    @Override
    public Optional<String> leader() {
        return leader(nodes.keySet());
    }

    /** The leader of the highest term among {@code names}, if any of them claims to lead. */
    Optional<String> leader(Collection<String> names) {
        String leader = null;
        long term = -1;
        for (Map.Entry<String, Status> status : statuses(names).entrySet()) {
            if (status.getValue().role().equals("leader") && status.getValue().term() > term) {
                leader = status.getKey();
                term = status.getValue().term();
            }
        }
        return Optional.ofNullable(leader);
    }

    @Override
    public boolean awaitLeader(Collection<String> names, Duration patience) throws InterruptedException {
        long deadline = System.nanoTime() + patience.toNanos();
        while (leader(names).isEmpty()) {
            if (System.nanoTime() - deadline >= 0) {
                return false;
            }
            Thread.sleep(POLL_MILLIS);
        }
        return true;
    }

    /** Waits until every node answers and one of them leads. */
    boolean awaitServing(Duration patience) throws InterruptedException {
        long deadline = System.nanoTime() + patience.toNanos();
        while (true) {
            Map<String, Status> statuses = statuses(nodes.keySet());
            boolean allUp = statuses.values().stream().noneMatch(status -> status.equals(Status.DOWN));
            if (allUp && statuses.values().stream().anyMatch(status -> status.role().equals("leader"))) {
                return true;
            }
            if (System.nanoTime() - deadline >= 0) {
                return false;
            }
            Thread.sleep(POLL_MILLIS);
        }
    }

    private Status status(InetSocketAddress address) {
        try (RespConnection connection = RespConnection.open(address, timeout, timeout)) {
            RespValue reply = connection.call("INFO", "raft");
            String text = switch (reply) {
                case RespBulkString bulk -> new String(bulk.value(), StandardCharsets.UTF_8);
                case RespSimpleString simple -> simple.value();
                default -> "";
            };
            return parse(text);
        } catch (IOException e) {
            return Status.DOWN;
        }
    }

    /** Parses the {@code role} and {@code term} fields of an {@code INFO raft} reply. */
    static Status parse(String info) {
        String role = "unknown";
        long term = -1;
        for (String line : info.split("\r?\n")) {
            List<String> field = List.of(line.split(":", 2));
            if (field.size() == 2) {
                switch (field.getFirst()) {
                    case "role" -> role = field.get(1).strip();
                    case "term" -> term = Long.parseLong(field.get(1).strip());
                    default -> {
                    }
                }
            }
        }
        return new Status(role, term);
    }
}
