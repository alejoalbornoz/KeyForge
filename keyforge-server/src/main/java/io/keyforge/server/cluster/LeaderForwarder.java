package io.keyforge.server.cluster;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Status;
import io.keyforge.protocol.RespDecoder;
import io.keyforge.protocol.RespEncoder;
import io.keyforge.protocol.RespError;
import io.keyforge.protocol.RespRequestDecoder;
import io.keyforge.protocol.RespValue;
import io.keyforge.raft.grpc.ForwardHandler;
import io.keyforge.raft.grpc.ForwardNotSentException;
import io.keyforge.raft.grpc.ForwardResult;
import io.keyforge.server.command.ClientSession;
import io.keyforge.server.command.CommandDispatcher;
import io.keyforge.server.command.CommandRouter;
import io.keyforge.server.command.RedirectException;

/**
 * Client routing in a cluster: a node that is not the leader sends data commands to the leader over gRPC and relays the
 * leader's reply, so clients such as {@code redis-benchmark} can talk to any node.
 *
 * <p>The command travels as the client sent it (RESP), so the leader parses it and turns relative TTLs into absolute
 * instants with its own clock. A node that receives a forwarded command executes it locally and never forwards it
 * again; if it has lost leadership it answers with the leader it now knows of, and the sender retries there until the
 * request timeout. While no leader is known (an election), the sender waits for one.
 *
 * <p><b>Retries are only safe when the command certainly had no effect</b> (ADR 0017): a {@code NotLeader} answer, or a
 * request that was never sent because there was no connection. Once a request is on the wire, a broken connection or a
 * timeout leaves its outcome unknown, so the client gets an error saying so instead of a retry that could apply it
 * twice. {@code CLUSTERDOWN}, which clients may treat as a definite failure, is sent only when no attempt was sent.
 */
public final class LeaderForwarder implements CommandRouter, ForwardHandler {

    private static final Logger log = LoggerFactory.getLogger(LeaderForwarder.class);

    private static final long PAUSE_MILLIS = 20;

    /** Sends a request to another node: {@link io.keyforge.raft.grpc.GrpcTransport#forward} in production. */
    @FunctionalInterface
    public interface ForwardClient {
        CompletableFuture<ForwardResult> forward(String node, byte[] request, Duration timeout);
    }

    private final String nodeId;
    private final Supplier<String> leaderId;
    private final ForwardClient client;
    private final Supplier<CommandDispatcher> dispatcher;
    private final long timeoutNanos;
    private final int maxBulkLength;
    private final int maxArrayLength;

    /**
     * @param nodeId     this node
     * @param leaderId   the leader this node currently knows of, or {@code null}
     * @param dispatcher this node's dispatcher, for commands forwarded here (supplied lazily: the dispatcher needs this
     *                   router first)
     */
    public LeaderForwarder(String nodeId, Supplier<String> leaderId, ForwardClient client,
                           Supplier<CommandDispatcher> dispatcher, Duration timeout, int maxBulkLength,
                           int maxArrayLength) {
        this.nodeId = nodeId;
        this.leaderId = leaderId;
        this.client = client;
        this.dispatcher = dispatcher;
        this.timeoutNanos = timeout.toNanos();
        this.maxBulkLength = maxBulkLength;
        this.maxArrayLength = maxArrayLength;
    }

    @Override
    public RespValue forward(byte[][] arguments, String target) {
        long deadline = System.nanoTime() + timeoutNanos;
        byte[] request = RespEncoder.encodeCommand(Arrays.asList(arguments));
        String next = target;
        while (true) {
            if (next == null) {
                next = leaderId.get();
            }
            if (next != null && next.equals(nodeId)) {
                // This node became the leader meanwhile.
                try {
                    return dispatcher.get().dispatchLocally(new ClientSession(), arguments);
                } catch (RedirectException e) {
                    next = otherThanSelf(e.target());
                }
            } else if (next != null) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                try {
                    ForwardResult result = client.forward(next, request, Duration.ofNanos(remaining))
                            .get(remaining + TimeUnit.SECONDS.toNanos(1), TimeUnit.NANOSECONDS);
                    switch (result) {
                        case ForwardResult.Reply reply -> {
                            return decodeReply(reply.reply());
                        }
                        case ForwardResult.NotLeader notLeader -> next = notLeader.leaderHint() == null
                                || notLeader.leaderHint().equals(next) ? null : otherThanSelf(notLeader.leaderHint());
                    }
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof ForwardNotSentException notSent)) {
                        // The request was sent: the leader may have committed it before the connection broke, so a
                        // retry could apply it twice. Only the client can decide what to do (ADR 0017).
                        return RespError.err("forwarding to the leader " + next + " failed ("
                                + Status.fromThrowable(e.getCause()).getCode()
                                + "); the command may or may not have been applied");
                    }
                    // Nothing was sent: the leader is unreachable, most likely gone. Wait for the next one.
                    log.debug("Leader {} unreachable: {}", next, notSent.getMessage());
                    next = null;
                } catch (TimeoutException e) {
                    return RespError.err("timed out waiting for the leader " + next
                            + "; the command may or may not have been applied");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return RespError.err("interrupted while forwarding to the leader");
                }
            }
            if (System.nanoTime() - deadline >= 0) {
                break;
            }
            pause();
            if (next == null) {
                next = leaderId.get();
            }
        }
        return new RespError("CLUSTERDOWN no Raft leader is available");
    }

    /** Executes a command another node forwarded here, without forwarding it any further. */
    @Override
    public ForwardResult handle(String from, byte[] request) {
        byte[][] arguments = new RespRequestDecoder(maxBulkLength, maxArrayLength).decode(ByteBuffer.wrap(request));
        if (arguments == null) {
            return new ForwardResult.Reply(RespEncoder.encode(RespError.err("incomplete forwarded request")));
        }
        try {
            return new ForwardResult.Reply(RespEncoder.encode(
                    dispatcher.get().dispatchLocally(new ClientSession(), arguments)));
        } catch (RedirectException e) {
            return new ForwardResult.NotLeader(e.target());
        }
    }

    private String otherThanSelf(String node) {
        return node == null || node.equals(nodeId) ? null : node;
    }

    private static RespValue decodeReply(byte[] reply) {
        RespValue value = new RespDecoder().decode(ByteBuffer.wrap(reply));
        return value != null ? value : RespError.err("incomplete reply from the leader");
    }

    private static void pause() {
        try {
            Thread.sleep(PAUSE_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
