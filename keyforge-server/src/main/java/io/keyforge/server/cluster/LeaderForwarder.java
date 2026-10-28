package io.keyforge.server.cluster;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
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
import io.keyforge.raft.grpc.ForwardResult;
import io.keyforge.raft.grpc.GrpcTransport;
import io.keyforge.raft.runtime.RaftRuntime;
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
 */
public final class LeaderForwarder implements CommandRouter, ForwardHandler {

    private static final Logger log = LoggerFactory.getLogger(LeaderForwarder.class);

    private static final long PAUSE_MILLIS = 20;

    private final RaftRuntime raft;
    private final GrpcTransport transport;
    private final Supplier<CommandDispatcher> dispatcher;
    private final long timeoutNanos;
    private final int maxBulkLength;
    private final int maxArrayLength;

    /**
     * @param dispatcher this node's dispatcher, for commands forwarded here (supplied lazily: the dispatcher needs this
     *                   router first)
     */
    public LeaderForwarder(RaftRuntime raft, GrpcTransport transport, Supplier<CommandDispatcher> dispatcher,
                           Duration timeout, int maxBulkLength, int maxArrayLength) {
        this.raft = raft;
        this.transport = transport;
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
                next = raft.leaderId();
            }
            if (next != null && next.equals(raft.nodeId())) {
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
                    ForwardResult result = transport.forward(next, request, Duration.ofNanos(remaining))
                            .get(remaining + TimeUnit.SECONDS.toNanos(1), TimeUnit.NANOSECONDS);
                    switch (result) {
                        case ForwardResult.Reply reply -> {
                            return decodeReply(reply.reply());
                        }
                        case ForwardResult.NotLeader notLeader -> next = notLeader.leaderHint() == null
                                || notLeader.leaderHint().equals(next) ? null : otherThanSelf(notLeader.leaderHint());
                    }
                } catch (ExecutionException e) {
                    Status status = Status.fromThrowable(e.getCause());
                    if (status.getCode() != Status.Code.UNAVAILABLE) {
                        return RespError.err("forwarding to the leader " + next + " failed (" + status.getCode()
                                + "); the command may or may not have been applied");
                    }
                    // The leader is unreachable, most likely gone: wait for the next one.
                    log.debug("Leader {} unreachable: {}", next, status);
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
                next = raft.leaderId();
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
        return node == null || node.equals(raft.nodeId()) ? null : node;
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
