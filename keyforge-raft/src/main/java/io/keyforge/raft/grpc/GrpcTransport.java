package io.keyforge.raft.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.google.protobuf.ByteString;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.keyforge.raft.RaftMessage;
import io.keyforge.raft.SnapshotPayload;
import io.keyforge.raft.grpc.proto.Envelope;
import io.keyforge.raft.grpc.proto.ForwardRequest;
import io.keyforge.raft.grpc.proto.ForwardResponse;
import io.keyforge.raft.grpc.proto.NotLeader;
import io.keyforge.raft.grpc.proto.RaftTransportGrpc;
import io.keyforge.raft.grpc.proto.SnapshotChunk;
import io.keyforge.raft.grpc.proto.SnapshotHeader;
import io.keyforge.raft.grpc.proto.SnapshotTransferred;
import io.keyforge.raft.runtime.Transport;

/**
 * The production {@link Transport}: gRPC over plaintext HTTP/2 (see {@code src/main/proto/keyforge/raft/v1/raft.proto}).
 *
 * <ul>
 *   <li><b>Exchange</b> (bidirectional streaming). Long-lived streams between each pair of nodes carry every Raft
 *       message: AppendEntries, heartbeats, votes and their answers. A node sends on the stream its peer opened to it
 *       when there is one, writing on its response side, and otherwise opens its own. A message that finds no stream to
 *       travel on is dropped; Raft retransmits.</li>
 *   <li><b>InstallSnapshot</b> (client streaming). A snapshot travels in 1 MiB chunks with flow control, on its own
 *       call, so it never delays heartbeats. The receiver spools it to a file and hands Raft an
 *       {@link RaftMessage.InstallSnapshot} whose payload is that file.</li>
 *   <li><b>Forward</b> (unary). A node that is not the leader hands a client request to the leader through a
 *       {@link ForwardHandler}.</li>
 * </ul>
 *
 * <p>Only the runtime's Raft thread calls {@link #send}. gRPC callbacks run on virtual threads.
 */
public final class GrpcTransport implements Transport {

    private static final Logger log = LoggerFactory.getLogger(GrpcTransport.class);

    private static final int CHUNK_BYTES = 1 << 20;
    private static final int MAX_MESSAGE_BYTES = 1 << 30;
    private static final long RECONNECT_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final Duration SNAPSHOT_TRANSFER_TIMEOUT = Duration.ofMinutes(10);

    private final String nodeId;
    private final String bindHost;
    private final int bindPort;
    private final Map<String, PeerAddress> peers;
    private final Path spoolDirectory;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    /** Our stream to each peer (request side). */
    private final Map<String, StreamObserver<Envelope>> outbound = new ConcurrentHashMap<>();
    /** The response side of each peer's stream to us, where answers to its messages go. */
    private final Map<String, StreamObserver<Envelope>> inbound = new ConcurrentHashMap<>();
    private final Map<String, Long> reconnectAfter = new ConcurrentHashMap<>();
    private final Map<String, Long> lastReconnect = new ConcurrentHashMap<>();
    private final Set<String> snapshotsInFlight = ConcurrentHashMap.newKeySet();

    private volatile Consumer<RaftMessage> receiver = message -> { };
    private volatile ForwardHandler forwardHandler;
    private Server server;
    private volatile boolean closed;

    /**
     * @param peers          the other members' addresses, by node id
     * @param spoolDirectory where incoming snapshots are written while they arrive; created if missing
     */
    public GrpcTransport(String nodeId, String bindHost, int bindPort, Map<String, PeerAddress> peers,
                         Path spoolDirectory) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.bindHost = Objects.requireNonNull(bindHost, "bindHost");
        this.bindPort = bindPort;
        this.peers = Map.copyOf(peers);
        this.spoolDirectory = Objects.requireNonNull(spoolDirectory, "spoolDirectory");
    }

    /** Installs the handler for requests other nodes forward here. */
    public void setForwardHandler(ForwardHandler handler) {
        this.forwardHandler = handler;
    }

    /** Binds the server. {@link #start} binds too if this was not called; tests use it to learn the port first. */
    public synchronized void bind() {
        if (server != null) {
            return;
        }
        try {
            Files.createDirectories(spoolDirectory);
            try (DirectoryStream<Path> leftovers = Files.newDirectoryStream(spoolDirectory, "incoming-*")) {
                for (Path leftover : leftovers) {
                    Files.deleteIfExists(leftover);
                }
            }
            server = NettyServerBuilder.forAddress(new InetSocketAddress(bindHost, bindPort))
                    .addService(new Service())
                    .executor(executor)
                    .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                    .permitKeepAliveTime(1, TimeUnit.SECONDS)
                    .permitKeepAliveWithoutCalls(true)
                    // Notice dead peers' streams to us, so that sends fall back to our own stream.
                    .keepAliveTime(5, TimeUnit.SECONDS)
                    .keepAliveTimeout(5, TimeUnit.SECONDS)
                    .build()
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot bind the Raft transport to " + bindHost + ":" + bindPort, e);
        }
        log.info("Raft transport listening on {}:{}", bindHost, port());
    }

    /** The bound port; useful when the configured port is 0. */
    public int port() {
        return server.getPort();
    }

    @Override
    public void start(Consumer<RaftMessage> inboundMessages) {
        this.receiver = Objects.requireNonNull(inboundMessages, "inboundMessages");
        bind();
    }

    @Override
    public void send(RaftMessage message) {
        if (closed || !peers.containsKey(message.to())) {
            return;
        }
        if (message instanceof RaftMessage.InstallSnapshot install) {
            sendSnapshot(install);
            return;
        }
        Envelope envelope = ProtoCodec.encode(message);
        String peer = message.to();
        // Either stream of a pair carries any message. The one the peer opened to us is preferred: it proves the peer
        // is up and reachable now, whereas ours may be a call still waiting for a connection (gRPC buffers it while it
        // resolves and connects, with exponential backoff) or a connection to a previous incarnation of the peer. A
        // restarted peer opens its stream with its first message, so it hears from us at once.
        if (sendOnInbound(peer, envelope)) {
            return;
        }
        StreamObserver<Envelope> stream = outboundStream(peer);
        if (stream != null && !tryNext(stream, envelope)) {
            outbound.remove(peer, stream);
        }
    }

    private boolean sendOnInbound(String peer, Envelope envelope) {
        StreamObserver<Envelope> back = inbound.get(peer);
        if (back == null) {
            return false;
        }
        if (tryNext(back, envelope)) {
            return true;
        }
        inbound.remove(peer, back);
        return false;
    }

    /**
     * Forwards a client request to {@code peer}. Completes with the peer's result, or exceptionally if the call fails
     * or times out (the request may or may not have been executed then).
     */
    public CompletableFuture<ForwardResult> forward(String peer, byte[] request, Duration timeout) {
        CompletableFuture<ForwardResult> result = new CompletableFuture<>();
        if (!peers.containsKey(peer)) {
            result.completeExceptionally(new IllegalArgumentException("unknown peer " + peer));
            return result;
        }
        RaftTransportGrpc.newStub(reconnectingChannel(peer))
                .withDeadlineAfter(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .forward(ForwardRequest.newBuilder().setFrom(nodeId).setPayload(ByteString.copyFrom(request)).build(),
                        new StreamObserver<>() {
                            @Override
                            public void onNext(ForwardResponse response) {
                                result.complete(switch (response.getResultCase()) {
                                    case REPLY -> new ForwardResult.Reply(response.getReply().toByteArray());
                                    case NOT_LEADER -> new ForwardResult.NotLeader(
                                            emptyToNull(response.getNotLeader().getLeaderHint()));
                                    case RESULT_NOT_SET -> new ForwardResult.NotLeader(null);
                                });
                            }

                            @Override
                            public void onError(Throwable t) {
                                result.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
                                result.completeExceptionally(new IllegalStateException("no forward response"));
                            }
                        });
        return result;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        outbound.values().forEach(GrpcTransport::completeQuietly);
        inbound.values().forEach(GrpcTransport::completeQuietly);
        outbound.clear();
        inbound.clear();
        channels.values().forEach(ManagedChannel::shutdownNow);
        Server bound;
        synchronized (this) {
            bound = server;
        }
        if (bound != null) {
            bound.shutdownNow();
        }
        try {
            for (ManagedChannel channel : channels.values()) {
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            if (bound != null) {
                bound.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Outbound

    private ManagedChannel channel(String peer) {
        return channels.computeIfAbsent(peer, id -> {
            PeerAddress address = peers.get(id);
            return NettyChannelBuilder.forAddress(address.host(), address.port())
                    .usePlaintext()
                    .executor(executor)
                    .maxInboundMessageSize(MAX_MESSAGE_BYTES)
                    .keepAliveTime(5, TimeUnit.SECONDS)
                    .keepAliveTimeout(5, TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(true)
                    .build();
        });
    }

    /**
     * The channel to {@code peer}, retrying its connection now if it is failing. gRPC backs off exponentially between
     * connection attempts, up to two minutes; a peer that restarts after a crash must be reachable again within a
     * heartbeat or two, or it cannot catch up.
     */
    private ManagedChannel reconnectingChannel(String peer) {
        ManagedChannel channel = channel(peer);
        if (channel.getState(true) == ConnectivityState.TRANSIENT_FAILURE) {
            long now = System.nanoTime();
            Long last = lastReconnect.get(peer);
            if (last == null || now - last >= RECONNECT_BACKOFF_NANOS) {
                lastReconnect.put(peer, now);
                channel.resetConnectBackoff();
            }
        }
        return channel;
    }

    /** Our stream to {@code peer}, opening it if needed; {@code null} while backing off after a failure. */
    private StreamObserver<Envelope> outboundStream(String peer) {
        StreamObserver<Envelope> stream = outbound.get(peer);
        if (stream != null) {
            return stream;
        }
        Long after = reconnectAfter.get(peer);
        if (after != null && System.nanoTime() - after < 0) {
            return null;
        }
        ClientResponseObserver<Envelope, Envelope> responses = new ClientResponseObserver<>() {
            private ClientCallStreamObserver<Envelope> requests;

            @Override
            public void beforeStart(ClientCallStreamObserver<Envelope> requestStream) {
                this.requests = requestStream;
            }

            @Override
            public void onNext(Envelope envelope) {
                deliver(envelope);
            }

            @Override
            public void onError(Throwable t) {
                lost(t);
            }

            @Override
            public void onCompleted() {
                lost(null);
            }

            private void lost(Throwable t) {
                if (outbound.remove(peer, requests)) {
                    reconnectAfter.put(peer, System.nanoTime() + RECONNECT_BACKOFF_NANOS);
                    log.debug("Stream to {} closed: {}", peer, t == null ? "completed" : Status.fromThrowable(t));
                }
            }
        };
        StreamObserver<Envelope> requests = RaftTransportGrpc.newStub(reconnectingChannel(peer)).exchange(responses);
        outbound.put(peer, requests);
        return requests;
    }

    private void sendSnapshot(RaftMessage.InstallSnapshot install) {
        String peer = install.to();
        if (!snapshotsInFlight.add(peer)) {
            return; // one transfer per follower at a time; the leader retries after its timeout
        }
        executor.execute(() -> {
            MDC.put("nodeId", nodeId);
            long started = System.nanoTime();
            try {
                streamSnapshot(install);
                log.info("Sent snapshot {}@{} ({} bytes) to {} in {} ms", install.lastIncludedIndex(),
                        install.lastIncludedTerm(), install.payload().size(), peer,
                        (System.nanoTime() - started) / 1_000_000);
            } catch (Exception e) {
                log.warn("Could not send snapshot {} to {}: {}", install.lastIncludedIndex(), peer, e.toString());
            } finally {
                snapshotsInFlight.remove(peer);
            }
        });
    }

    private void streamSnapshot(RaftMessage.InstallSnapshot install) throws Exception {
        CompletableFuture<Void> done = new CompletableFuture<>();
        ClientCallStreamObserver<SnapshotChunk> chunks = (ClientCallStreamObserver<SnapshotChunk>)
                RaftTransportGrpc.newStub(reconnectingChannel(install.to()))
                        .withDeadlineAfter(SNAPSHOT_TRANSFER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                        .installSnapshot(new StreamObserver<>() {
                            @Override
                            public void onNext(SnapshotTransferred value) {
                            }

                            @Override
                            public void onError(Throwable t) {
                                done.completeExceptionally(t);
                            }

                            @Override
                            public void onCompleted() {
                                done.complete(null);
                            }
                        });
        SnapshotHeader header = SnapshotHeader.newBuilder()
                .setFrom(install.from())
                .setTo(install.to())
                .setTerm(install.term())
                .setLastIncludedIndex(install.lastIncludedIndex())
                .setLastIncludedTerm(install.lastIncludedTerm())
                .setRound(install.round())
                .setSize(install.payload().size())
                .build();
        try (InputStream in = install.payload().open()) {
            byte[] buffer = new byte[CHUNK_BYTES];
            boolean first = true;
            while (true) {
                int read = in.readNBytes(buffer, 0, buffer.length);
                if (read == 0 && !first) {
                    break;
                }
                while (!chunks.isReady()) {
                    if (done.isDone() || closed) {
                        done.get();
                        throw new IllegalStateException("the transport closed");
                    }
                    LockSupport.parkNanos(1_000_000); // flow control: the receiver is behind
                }
                SnapshotChunk.Builder chunk = SnapshotChunk.newBuilder().setData(ByteString.copyFrom(buffer, 0, read));
                if (first) {
                    chunk.setHeader(header);
                    first = false;
                }
                chunks.onNext(chunk.build());
                if (read < buffer.length) {
                    break;
                }
            }
        } catch (Exception e) {
            chunks.onError(e);
            throw e;
        }
        chunks.onCompleted();
        done.get(SNAPSHOT_TRANSFER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Inbound

    private void deliver(Envelope envelope) {
        RaftMessage message;
        try {
            message = ProtoCodec.decode(envelope);
        } catch (RuntimeException e) {
            log.warn("Dropping a malformed message from {}: {}", envelope.getFrom(), e.toString());
            return;
        }
        if (message.to().equals(nodeId)) {
            receiver.accept(message);
        }
    }

    private final class Service extends RaftTransportGrpc.RaftTransportImplBase {

        @Override
        public StreamObserver<Envelope> exchange(StreamObserver<Envelope> responseStream) {
            // The Raft thread writes answers while gRPC threads may complete the stream: serialize the two.
            StreamObserver<Envelope> responses = new SerializedObserver<>(responseStream);
            return new StreamObserver<>() {
                private String peer;

                @Override
                public void onNext(Envelope envelope) {
                    if (peer == null && peers.containsKey(envelope.getFrom())) {
                        peer = envelope.getFrom();
                        StreamObserver<Envelope> previous = inbound.put(peer, responses);
                        if (previous != null && previous != responses) {
                            completeQuietly(previous);
                        }
                    }
                    deliver(envelope);
                }

                @Override
                public void onError(Throwable t) {
                    forget();
                }

                @Override
                public void onCompleted() {
                    forget();
                    completeQuietly(responses);
                }

                private void forget() {
                    if (peer != null) {
                        inbound.remove(peer, responses);
                    }
                }
            };
        }

        @Override
        public StreamObserver<SnapshotChunk> installSnapshot(StreamObserver<SnapshotTransferred> response) {
            return new StreamObserver<>() {
                private SnapshotHeader header;
                private Path file;
                private OutputStream out;
                private long received;

                @Override
                public void onNext(SnapshotChunk chunk) {
                    try {
                        if (out == null) {
                            if (!chunk.hasHeader()) {
                                throw new IllegalStateException("the first snapshot chunk has no header");
                            }
                            header = chunk.getHeader();
                            file = Files.createTempFile(spoolDirectory, "incoming-", ".snap");
                            out = Files.newOutputStream(file);
                        }
                        chunk.getData().writeTo(out);
                        received += chunk.getData().size();
                    } catch (IOException | RuntimeException e) {
                        abort();
                        response.onError(Status.INTERNAL.withDescription(e.toString()).asRuntimeException());
                    }
                }

                @Override
                public void onError(Throwable t) {
                    abort();
                }

                @Override
                public void onCompleted() {
                    try {
                        if (out == null) {
                            throw new IllegalStateException("empty snapshot transfer");
                        }
                        out.close();
                        out = null;
                        if (received != header.getSize()) {
                            throw new IllegalStateException("received " + received + " snapshot bytes, expected "
                                    + header.getSize());
                        }
                        receiver.accept(new RaftMessage.InstallSnapshot(header.getFrom(), header.getTo(),
                                header.getTerm(), header.getLastIncludedIndex(), header.getLastIncludedTerm(),
                                header.getRound(), new SpooledSnapshot(file, received)));
                        response.onNext(SnapshotTransferred.getDefaultInstance());
                        response.onCompleted();
                    } catch (IOException | RuntimeException e) {
                        abort();
                        response.onError(Status.INTERNAL.withDescription(e.toString()).asRuntimeException());
                    }
                }

                private void abort() {
                    try {
                        if (out != null) {
                            out.close();
                        }
                        if (file != null) {
                            Files.deleteIfExists(file);
                        }
                    } catch (IOException e) {
                        log.debug("Could not discard a partial snapshot {}: {}", file, e.toString());
                    }
                    out = null;
                }
            };
        }

        @Override
        public void forward(ForwardRequest request, StreamObserver<ForwardResponse> response) {
            ForwardHandler handler = forwardHandler;
            ForwardResult result = handler == null
                    ? new ForwardResult.NotLeader(null)
                    : handler.handle(request.getFrom(), request.getPayload().toByteArray());
            ForwardResponse.Builder builder = ForwardResponse.newBuilder();
            switch (result) {
                case ForwardResult.Reply reply -> builder.setReply(ByteString.copyFrom(reply.reply()));
                case ForwardResult.NotLeader notLeader -> builder.setNotLeader(NotLeader.newBuilder()
                        .setLeaderHint(notLeader.leaderHint() == null ? "" : notLeader.leaderHint()));
            }
            response.onNext(builder.build());
            response.onCompleted();
        }
    }

    /** A snapshot spooled to a file by the receiving side; deleted once Raft has handled it. */
    private record SpooledSnapshot(Path file, long size) implements SnapshotPayload {

        @Override
        public InputStream open() throws IOException {
            return Files.newInputStream(file);
        }

        @Override
        public void discard() {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                log.debug("Could not delete spooled snapshot {}: {}", file, e.toString());
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------

    /** Serializes calls on a stream observer, which gRPC does not allow concurrently, and drops calls after the end. */
    private static final class SerializedObserver<T> implements StreamObserver<T> {
        private final StreamObserver<T> delegate;
        private boolean done;

        SerializedObserver(StreamObserver<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public synchronized void onNext(T value) {
            if (done) {
                throw new IllegalStateException("stream closed");
            }
            delegate.onNext(value);
        }

        @Override
        public synchronized void onError(Throwable t) {
            if (!done) {
                done = true;
                delegate.onError(t);
            }
        }

        @Override
        public synchronized void onCompleted() {
            if (!done) {
                done = true;
                delegate.onCompleted();
            }
        }
    }

    private static boolean tryNext(StreamObserver<Envelope> stream, Envelope envelope) {
        try {
            stream.onNext(envelope);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static void completeQuietly(StreamObserver<?> stream) {
        try {
            stream.onCompleted();
        } catch (RuntimeException e) {
            // Already closed.
        }
    }

    private static String emptyToNull(String text) {
        return text == null || text.isEmpty() ? null : text;
    }
}
