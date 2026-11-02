package io.keyforge.chaos;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concurrent clients running {@code GET}, {@code SET} and {@code INCR} on a few keys, recording every operation in a
 * {@link History}.
 *
 * <p>Each client is a Jepsen "process": a virtual thread with its own connection to one node (client {@code i} talks
 * to node {@code i mod nodes}), at most one operation in flight, paced to share {@code rate} operations per second
 * with the others. After an indeterminate completion the client reconnects if the connection may be out of step, and
 * continues as a new process: its old operation may still take effect at any later time.
 */
final class Workload {

    private static final Logger log = LoggerFactory.getLogger(Workload.class);

    /** Attempts per key in the final reads. */
    private static final int FINAL_READ_ATTEMPTS = 5;

    private final ChaosOptions options;
    private final History history;
    private final RunStats stats;
    private final List<Map.Entry<String, InetSocketAddress>> nodes;
    private final KeySchedule keys;
    private final SplittableRandom seeds;
    /** Values written by {@code SET}: multiples of 1,000, so values reached by increments rarely repeat them. */
    private final AtomicLong values = new AtomicLong();
    private int nextProcess;

    Workload(ChaosOptions options, History history, RunStats stats) {
        this.options = options;
        this.history = history;
        this.stats = stats;
        this.nodes = List.copyOf(options.nodes().entrySet());
        this.seeds = new SplittableRandom(options.seed());
        this.keys = new KeySchedule("lin:" + Long.toHexString(seeds.nextLong() & 0xFFFFFFL) + ":", options.keys(),
                options.opsPerKey());
        this.nextProcess = options.clients();
    }

    /** Runs the clients until {@code duration} has passed, then waits for their last operations to complete. */
    void run(Duration duration) throws InterruptedException {
        long deadline = System.nanoTime() + duration.toNanos();
        List<Thread> clients = new ArrayList<>();
        for (int slot = 0; slot < options.clients(); slot++) {
            int client = slot;
            SplittableRandom random = seeds.split();
            clients.add(Thread.ofVirtual().name("client-" + slot).start(() -> client(client, random, deadline)));
        }
        for (Thread client : clients) {
            client.join();
        }
    }

    /**
     * Reads every key the workload used, once the faults are healed: a lost write shows up even on keys nobody read
     * after it. Each key is read through the nodes in turn until a read succeeds.
     */
    void finalReads() throws InterruptedException {
        ConcurrentLinkedQueue<String> pending = new ConcurrentLinkedQueue<>(keys.issued());
        List<Thread> readers = new ArrayList<>();
        for (int slot = 0; slot < options.clients(); slot++) {
            int reader = slot;
            readers.add(Thread.ofVirtual().name("final-read-" + slot).start(() -> finalReader(reader, pending)));
        }
        for (Thread reader : readers) {
            reader.join();
        }
    }

    /** Every key the workload used. */
    List<String> keys() {
        return keys.issued();
    }

    private void client(int slot, SplittableRandom random, long deadline) {
        Map.Entry<String, InetSocketAddress> node = nodes.get(slot % nodes.size());
        long interval = Math.max(1, Math.round(1e9 * options.clients() / options.rate()));
        int process = slot;
        RespConnection connection = null;
        long next = System.nanoTime() + random.nextLong(interval);
        try {
            while (true) {
                long now = System.nanoTime();
                if (next - deadline >= 0) {
                    return;
                }
                if (next - now > 0) {
                    LockSupport.parkNanos(next - now);
                    continue;
                }
                OpFunction function = chooseFunction(random);
                String key = keys.next(random);
                Long value = function == OpFunction.SET ? values.incrementAndGet() * 1000 : null;
                Attempt attempt = execute(process, function, key, value, node, connection);
                connection = attempt.connection();
                if (attempt.completion().type() == EventType.INFO) {
                    process = newProcess();
                }
                // Next start: a uniformly drawn gap of mean `interval` after this one began, or now if late.
                next = Math.max(now + random.nextLong(2 * interval), System.nanoTime());
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void finalReader(int slot, ConcurrentLinkedQueue<String> pending) {
        int process = newProcess();
        String key;
        while ((key = pending.poll()) != null) {
            for (int attempt = 0; attempt < FINAL_READ_ATTEMPTS; attempt++) {
                Map.Entry<String, InetSocketAddress> node = nodes.get((slot + attempt) % nodes.size());
                Attempt result = execute(process, OpFunction.GET, key, null, node, null);
                if (result.connection() != null) {
                    result.connection().close();
                }
                if (result.completion().type() == EventType.OK) {
                    break;
                }
                if (result.completion().type() == EventType.INFO) {
                    process = newProcess();
                }
            }
        }
    }

    private record Attempt(Completion completion, RespConnection connection) {
    }

    /**
     * Runs one operation on {@code connection}, opening one if it is {@code null}, and records it. Returns the
     * connection to use next: {@code null} if it may be out of step with its replies or could not be opened.
     */
    private Attempt execute(int process, OpFunction function, String key, Long value,
                            Map.Entry<String, InetSocketAddress> node, RespConnection connection) {
        HistoryEvent.Op invocation = history.invoke(process, function, key, value, node.getKey());
        Completion completion;
        if (connection == null) {
            try {
                connection = RespConnection.open(node.getValue(), options.connectTimeout(), options.opTimeout());
            } catch (IOException e) {
                completion = Completion.notSent(e);
                record(invocation, completion);
                return new Attempt(completion, null);
            }
        }
        try {
            completion = Completion.of(function, switch (function) {
                case GET -> connection.call("GET", key);
                case SET -> connection.call("SET", key, Long.toString(value));
                case INCR -> connection.call("INCR", key);
            });
        } catch (IOException e) {
            completion = Completion.noReply(e, options.opTimeout());
            connection.close();
            connection = null;
        }
        record(invocation, completion);
        return new Attempt(completion, connection);
    }

    private void record(HistoryEvent.Op invocation, Completion completion) {
        HistoryEvent.Op done = history.complete(invocation, completion.type(), completion.value(),
                completion.error());
        stats.record(invocation.function(), completion.type(), done.timeNanos() - invocation.timeNanos());
        if (completion.type() != EventType.OK) {
            log.debug("{} {} via {} -> {}: {}", invocation.function().label(), invocation.key(), invocation.node(),
                    completion.type().label(), completion.error());
        }
    }

    private OpFunction chooseFunction(SplittableRandom random) {
        double draw = random.nextDouble();
        if (draw < options.getRatio()) {
            return OpFunction.GET;
        }
        return draw < options.getRatio() + options.incrRatio() ? OpFunction.INCR : OpFunction.SET;
    }

    private synchronized int newProcess() {
        return nextProcess++;
    }
}
