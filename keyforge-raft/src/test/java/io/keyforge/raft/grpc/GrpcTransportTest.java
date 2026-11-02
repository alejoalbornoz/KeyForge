package io.keyforge.raft.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.raft.RaftMessage;

/** Connection handling of the gRPC transport, between two transports on localhost. */
@Timeout(60)
class GrpcTransportTest {

    @TempDir
    Path root;

    private final List<GrpcTransport> started = new ArrayList<>();

    @AfterEach
    void closeAll() {
        started.forEach(GrpcTransport::close);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private GrpcTransport transport(String id, Map<String, PeerAddress> members) {
        GrpcTransport transport = new GrpcTransport(id, "127.0.0.1", members.get(id).port(), members,
                root.resolve(id + "-" + started.size()));
        started.add(transport);
        return transport;
    }

    private static RaftMessage.VoteRequest preVote() {
        return new RaftMessage.VoteRequest("a", "b", 1, 0, 0, true);
    }

    /**
     * Regression test for a liveness bug the Phase 5 nemesis found: a stream opened while the peer was unreachable
     * failed at once, its failure ran before it was stored, and the dead stream then swallowed every later message to
     * that peer. After partitions healed, a whole cluster stayed without a leader.
     */
    @Test
    void aSenderReachesAPeerThatComesUpLater() throws Exception {
        for (int round = 0; round < 3; round++) {
            Map<String, PeerAddress> members = Map.of("a", new PeerAddress("127.0.0.1", freePort()),
                    "b", new PeerAddress("127.0.0.1", freePort()));
            GrpcTransport a = transport("a", members);
            a.start(message -> { });
            for (int i = 0; i < 40; i++) { // b is down: every attempt to open a stream fails
                a.send(preVote());
                Thread.sleep(25);
            }
            AtomicInteger received = new AtomicInteger();
            transport("b", members).start(message -> received.incrementAndGet());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (received.get() == 0 && System.nanoTime() < deadline) {
                a.send(preVote());
                Thread.sleep(25);
            }
            assertThat(received.get()).as("messages b received within 5 s of starting, round %d", round)
                    .isPositive();
        }
    }

    @Test
    void aForwardWithoutAConnectionIsNotSent() throws Exception {
        Map<String, PeerAddress> members = Map.of("a", new PeerAddress("127.0.0.1", freePort()),
                "b", new PeerAddress("127.0.0.1", freePort()));
        GrpcTransport a = transport("a", members);
        a.start(message -> { });
        assertThatThrownBy(() -> a.forward("b", new byte[] {1}, Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ForwardNotSentException.class);
    }

    /** A forward that was on the wire when the connection broke may have been executed: it must not look unsent. */
    @Test
    void aForwardLostAfterSendingIsNotReportedAsNotSent() throws Exception {
        Map<String, PeerAddress> members = Map.of("a", new PeerAddress("127.0.0.1", freePort()),
                "b", new PeerAddress("127.0.0.1", freePort()));
        GrpcTransport a = transport("a", members);
        a.start(message -> { });
        GrpcTransport b = transport("b", members);
        CountDownLatch executing = new CountDownLatch(1);
        CountDownLatch never = new CountDownLatch(1);
        b.setForwardHandler((from, request) -> {
            if (request[0] == 2) {
                executing.countDown();
                try {
                    never.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return new ForwardResult.Reply(request);
        });
        b.start(message -> { });

        // The first forwards find no connection yet; once one is established, a forward goes through.
        ForwardResult warmUp = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (warmUp == null && System.nanoTime() < deadline) {
            try {
                warmUp = a.forward("b", new byte[] {1}, Duration.ofSeconds(2)).get(5, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(ForwardNotSentException.class);
                Thread.sleep(20);
            }
        }
        assertThat(warmUp).isInstanceOf(ForwardResult.Reply.class);

        CompletableFuture<ForwardResult> inFlight = a.forward("b", new byte[] {2}, Duration.ofSeconds(10));
        assertThat(executing.await(5, TimeUnit.SECONDS)).isTrue();
        b.close();
        assertThatThrownBy(() -> inFlight.get(15, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .satisfies(e -> assertThat(e.getCause()).isNotInstanceOf(ForwardNotSentException.class));
        never.countDown();
    }
}
