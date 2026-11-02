package io.keyforge.server.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import io.grpc.Status;
import io.keyforge.protocol.RespEncoder;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespInteger;
import io.keyforge.protocol.RespValue;
import io.keyforge.raft.grpc.ForwardNotSentException;
import io.keyforge.raft.grpc.ForwardResult;
import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.command.StandardCommands;
import io.keyforge.storage.InMemoryKeyValueStore;

/**
 * The forwarder's retry rules (ADR 0017): it retries only attempts that certainly had no effect, and never answers
 * {@code CLUSTERDOWN} once an attempt may have reached a leader.
 */
class LeaderForwarderTest {

    private static final byte[][] INCR = {b("INCR"), b("counter")};

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** A scripted transport: each forward takes the next outcome and records where it went. */
    private final Deque<Function<String, CompletableFuture<ForwardResult>>> outcomes = new ArrayDeque<>();
    private final List<String> attempts = new ArrayList<>();
    private final AtomicReference<String> leader = new AtomicReference<>("node-2");

    private LeaderForwarder forwarder(Duration timeout) {
        CommandDispatcher local = new CommandDispatcher(StandardCommands.create(new InMemoryKeyValueStore(),
                InstantSource.system(), "test", Map.of()));
        return new LeaderForwarder("node-1", leader::get, (node, request, remaining) -> {
            attempts.add(node);
            return outcomes.isEmpty()
                    ? CompletableFuture.failedFuture(new ForwardNotSentException(node, "TRANSIENT_FAILURE"))
                    : outcomes.poll().apply(node);
        }, () -> local, timeout, 1 << 20, 1024);
    }

    private static Function<String, CompletableFuture<ForwardResult>> reply(RespValue value) {
        return node -> CompletableFuture.completedFuture(new ForwardResult.Reply(RespEncoder.encode(value)));
    }

    private static Function<String, CompletableFuture<ForwardResult>> notLeader(String hint) {
        return node -> CompletableFuture.completedFuture(new ForwardResult.NotLeader(hint));
    }

    private static Function<String, CompletableFuture<ForwardResult>> failure(Throwable cause) {
        return node -> CompletableFuture.failedFuture(cause);
    }

    @Test
    void aNotLeaderAnswerIsRetriedOnTheLeaderItNames() {
        outcomes.add(notLeader("node-3"));
        outcomes.add(reply(RespInteger.of(7)));
        assertThat(forwarder(Duration.ofSeconds(5)).forward(INCR, "node-2")).isEqualTo(RespInteger.of(7));
        assertThat(attempts).containsExactly("node-2", "node-3");
    }

    @Test
    void aRequestThatWasNeverSentIsRetriedOnceALeaderIsReachable() {
        outcomes.add(failure(new ForwardNotSentException("node-2", "TRANSIENT_FAILURE")));
        outcomes.add(reply(RespInteger.of(1)));
        leader.set("node-3");
        assertThat(forwarder(Duration.ofSeconds(5)).forward(INCR, "node-2")).isEqualTo(RespInteger.of(1));
        assertThat(attempts).containsExactly("node-2", "node-3");
    }

    /** The bug the Phase 5 checker found: a leader killed after committing an INCR, which a retry applied again. */
    @Test
    void aRequestLostAfterSendingIsNeverRetried() {
        outcomes.add(failure(Status.UNAVAILABLE.withDescription("connection reset").asRuntimeException()));
        outcomes.add(reply(RespInteger.of(99)));
        RespValue reply = forwarder(Duration.ofSeconds(5)).forward(INCR, "node-2");
        assertThat(reply).isInstanceOf(RespError.class);
        assertThat(((RespError) reply).message()).contains("may or may not have been applied")
                .doesNotStartWith("CLUSTERDOWN");
        assertThat(attempts).containsExactly("node-2");
    }

    @Test
    void aTimeoutAfterSendingLeavesTheOutcomeUnknown() {
        outcomes.add(failure(Status.DEADLINE_EXCEEDED.asRuntimeException()));
        RespValue reply = forwarder(Duration.ofSeconds(5)).forward(INCR, "node-2");
        assertThat(((RespError) reply).message()).contains("may or may not have been applied");
    }

    @Test
    void clusterDownOnlyWhenNoAttemptWasSent() {
        leader.set(null);
        RespValue reply = forwarder(Duration.ofMillis(200)).forward(INCR, "node-2");
        assertThat(((RespError) reply).message()).startsWith("CLUSTERDOWN");
        assertThat(attempts).isNotEmpty().allMatch("node-2"::equals);
    }
}
