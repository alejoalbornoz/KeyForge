package io.keyforge.raft.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.raft.Clock;
import io.keyforge.raft.LogEntry;
import io.keyforge.raft.RaftConfig;
import io.keyforge.raft.RaftMessage;
import io.keyforge.raft.storage.FileRaftStorage;

/** The runtime of a single-node cluster, which needs no transport. */
@Timeout(60)
class RaftRuntimeTest {

    @TempDir
    Path dir;

    private static final class CountingMachine implements StateMachine {
        final AtomicLong applied = new AtomicLong();

        @Override
        public long durableAppliedIndex() {
            return 0;
        }

        @Override
        public Object apply(LogEntry entry) {
            return applied.incrementAndGet();
        }

        @Override
        public void writeSnapshot(OutputStream out) throws IOException {
            out.write(1);
        }

        @Override
        public void restoreSnapshot(long index, long term, InputStream in) {
        }
    }

    private static final class NoTransport implements Transport {
        @Override
        public void start(Consumer<RaftMessage> inbound) {
        }

        @Override
        public void send(RaftMessage message) {
        }

        @Override
        public void close() {
        }
    }

    /** Starts the node and waits until it has elected itself. */
    private RaftRuntime start(CountingMachine machine) throws Exception {
        RaftConfig config = RaftConfig.of("solo", List.of("solo")).withTimeouts(50, 100, 10);
        RaftRuntime runtime = RaftRuntime.start(config, Clock.system(), new NoTransport(), FileRaftStorage.open(dir),
                machine, new RaftRuntime.Options(5, 1_000_000));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!runtime.isLeader() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        return runtime;
    }

    @Test
    void aSingleNodeCommitsAndServesReads() throws Exception {
        CountingMachine machine = new CountingMachine();
        try (RaftRuntime runtime = start(machine)) {
            Object result = runtime.propose("x".getBytes(StandardCharsets.UTF_8)).get(10, TimeUnit.SECONDS);
            assertThat(result).isEqualTo(2L); // the leader's no-op came first
            assertThat(runtime.readBarrier().get(10, TimeUnit.SECONDS)).isEqualTo(2L);
        }
    }

    /**
     * Closing used to interrupt the Raft thread, and an interrupt during the log's fsync closes the file channel: the
     * runtime then reported a clean stop as "The Raft node failed".
     */
    @Test
    void closingUnderLoadIsNotAFailure() throws Exception {
        for (int round = 0; round < 5; round++) {
            CountingMachine machine = new CountingMachine();
            RaftRuntime runtime = start(machine);
            runtime.propose(new byte[] {0}).get(10, TimeUnit.SECONDS);
            AtomicBoolean writing = new AtomicBoolean(true);
            List<Thread> writers = new java.util.ArrayList<>();
            for (int i = 0; i < 4; i++) {
                // Each writer waits for its write, so the log is fsynced continually without the queue growing.
                writers.add(Thread.ofVirtual().start(() -> {
                    while (writing.get()) {
                        try {
                            runtime.propose(new byte[] {1}).get(10, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            return; // closed
                        }
                    }
                }));
            }
            Thread.sleep(100);
            runtime.close();
            writing.set(false);
            for (Thread writer : writers) {
                writer.join();
            }
            assertThat(runtime.failure()).as("round %d", round).isNull();
            // The next round reopens the same directory: the log survived the stop intact.
        }
    }
}
