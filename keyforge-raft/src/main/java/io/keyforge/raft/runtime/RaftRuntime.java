package io.keyforge.raft.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import io.keyforge.raft.Clock;
import io.keyforge.raft.LogEntry;
import io.keyforge.raft.PersistentState;
import io.keyforge.raft.RaftAction;
import io.keyforge.raft.RaftConfig;
import io.keyforge.raft.RaftEvent;
import io.keyforge.raft.RaftMessage;
import io.keyforge.raft.RaftNode;
import io.keyforge.raft.RaftStatus;
import io.keyforge.raft.Role;
import io.keyforge.raft.storage.FileRaftStorage;
import io.keyforge.raft.storage.SnapshotStore;

/**
 * Drives a {@link RaftNode} in production: real time, durable storage, a transport and a state machine.
 *
 * <p><b>The Raft thread</b> ({@code keyforge-raft}) owns the core. It takes events (messages from the transport, client
 * requests, snapshot notices) in batches of up to {@value #MAX_BATCH}, adds a tick every
 * {@link Options#tickMillis()}, and hands each to the core. Then it executes the collected actions in two phases:
 * first every persistence action followed by a single fsync, then everything else in order (sends, applies, replies).
 * A message therefore never leaves before the state it depends on is on disk, and one fsync covers a whole batch.
 *
 * <p><b>The apply thread</b> ({@code keyforge-raft-apply}) applies committed entries to the {@link StateMachine},
 * completes the futures of the proposals they belong to, serves reads once their read index is applied, and writes a
 * snapshot every {@link Options#snapshotThreshold()} entries. A slow state machine thus never delays heartbeats.
 *
 * <p>Logs carry the node id, the term and the log index in the MDC ({@code nodeId}, {@code term}, {@code index}).
 */
public final class RaftRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftRuntime.class);

    private static final int MAX_BATCH = 1024;

    /**
     * @param tickMillis        how often the core's timers are checked
     * @param snapshotThreshold applied entries after which a new snapshot is taken and the log compacted
     */
    public record Options(long tickMillis, long snapshotThreshold) {

        public static final long DEFAULT_TICK_MILLIS = 10;
        public static final long DEFAULT_SNAPSHOT_THRESHOLD = 100_000;

        public Options {
            if (tickMillis < 1 || snapshotThreshold < 1) {
                throw new IllegalArgumentException("tickMillis and snapshotThreshold must be positive");
            }
        }

        public static Options defaults() {
            return new Options(DEFAULT_TICK_MILLIS, DEFAULT_SNAPSHOT_THRESHOLD);
        }
    }

    private final RaftConfig config;
    private final Clock clock;
    private final Transport transport;
    private final FileRaftStorage storage;
    private final StateMachine stateMachine;
    private final Options options;
    private final RaftNode node;
    private final String nodeId;

    private final LinkedBlockingQueue<RaftEvent> events = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<ApplyTask> applyQueue = new LinkedBlockingQueue<>();
    private final AtomicLong nextRequestId = new AtomicLong(1);
    /** Proposals waiting for the core to accept or reject them. */
    private final Map<Long, CompletableFuture<Object>> proposals = new ConcurrentHashMap<>();
    /** Accepted proposals waiting for their entry to be applied, by log index. */
    private final Map<Long, Pending> accepted = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<Long>> reads = new ConcurrentHashMap<>();

    private final Thread raftThread;
    private final Thread applyThread;
    private volatile RaftStatus status;
    private volatile long appliedIndex;
    private volatile boolean stopping;
    private volatile Throwable failure;

    private record Pending(long term, CompletableFuture<Object> future) {
    }

    private sealed interface ApplyTask {
    }

    private record ApplyEntries(List<LogEntry> entries) implements ApplyTask {
    }

    private record Restore(long index, long term) implements ApplyTask {
    }

    private record Read(long readIndex, CompletableFuture<Long> future) implements ApplyTask {
    }

    private record Stop() implements ApplyTask {
    }

    private RaftRuntime(RaftConfig config, Clock clock, Transport transport, FileRaftStorage storage,
                        StateMachine stateMachine, Options options, long applied) {
        this.config = config;
        this.clock = clock;
        this.transport = transport;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.options = options;
        this.nodeId = config.nodeId();
        this.node = new RaftNode(config, clock, new SplittableRandom(), storage.recovered(), applied);
        this.appliedIndex = applied;
        this.status = node.status();
        Map<String, String> mdc = MDC.getCopyOfContextMap();
        this.raftThread = Thread.ofPlatform().name("keyforge-raft").unstarted(() -> withMdc(mdc, this::runRaft));
        this.applyThread = Thread.ofPlatform().name("keyforge-raft-apply")
                .unstarted(() -> withMdc(mdc, this::runApply));
    }

    /**
     * Recovers the node from {@code storage} and starts it. If the state machine's durable state is older than the
     * latest snapshot it is restored from the snapshot first; the log entries after it are applied as they are found
     * committed.
     */
    public static RaftRuntime start(RaftConfig config, Clock clock, Transport transport, FileRaftStorage storage,
                                    StateMachine stateMachine, Options options) throws IOException {
        Objects.requireNonNull(transport, "transport");
        PersistentState state = storage.recovered();
        long applied = stateMachine.durableAppliedIndex();
        Optional<SnapshotStore.SnapshotFile> snapshot = storage.snapshots().latest();
        if (snapshot.isPresent() && applied < snapshot.get().index()) {
            SnapshotStore.SnapshotFile file = snapshot.get();
            log.info("The state machine is at index {}, behind snapshot {}@{}: restoring it", applied, file.index(),
                    file.term());
            try (InputStream in = SnapshotStore.openBody(file)) {
                stateMachine.restoreSnapshot(file.index(), file.term(), in);
            }
            applied = file.index();
        }
        if (applied > state.lastIndex() || applied < state.snapshotIndex()) {
            throw new IllegalStateException("The state machine has applied index " + applied + " but the Raft log in "
                    + storage.directory() + " covers (" + state.snapshotIndex() + ", " + state.lastIndex()
                    + "]: the Raft data and the store's data do not belong together");
        }
        RaftRuntime runtime = new RaftRuntime(config, clock, transport, storage, stateMachine, options, applied);
        log.info("Starting Raft node {} of {}: term {}, applied {}, log ({}, {}]", config.nodeId(), config.members(),
                state.term(), applied, state.snapshotIndex(), state.lastIndex());
        runtime.applyThread.start();
        runtime.raftThread.start();
        transport.start(runtime::receive);
        return runtime;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Public API (any thread)

    /**
     * Proposes a command. The future completes with the state machine's result once the entry is committed and
     * applied here, or fails with {@link NotLeaderException} (not the leader, or the entry was overwritten after a
     * leader change; safe to retry) or {@link OutcomeUnknownException}. The caller bounds the wait.
     */
    public CompletableFuture<Object> propose(byte[] command) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        if (!usable(future)) {
            return future;
        }
        long requestId = nextRequestId.getAndIncrement();
        proposals.put(requestId, future);
        events.add(new RaftEvent.Propose(requestId, command));
        return future;
    }

    /**
     * Starts a linearizable read (ReadIndex). The future completes once this node, as the confirmed leader, has applied
     * every entry committed before the call; the caller then reads its local state. It fails with
     * {@link NotLeaderException} on a node that is not the leader.
     */
    public CompletableFuture<Long> readBarrier() {
        CompletableFuture<Long> future = new CompletableFuture<>();
        if (!usable(future)) {
            return future;
        }
        long requestId = nextRequestId.getAndIncrement();
        reads.put(requestId, future);
        events.add(new RaftEvent.ReadIndex(requestId));
        return future;
    }

    /** Hands a message from the transport to the core. */
    public void receive(RaftMessage message) {
        if (!stopping) {
            events.add(message);
        }
    }

    /** The core's state after the last processed batch. */
    public RaftStatus status() {
        return status;
    }

    public boolean isLeader() {
        return status.role() == Role.LEADER;
    }

    /** The leader this node knows of, or {@code null}. */
    public String leaderId() {
        return status.leaderId();
    }

    public String nodeId() {
        return nodeId;
    }

    public RaftConfig config() {
        return config;
    }

    /** Index of the last entry applied to the state machine. */
    public long appliedIndex() {
        return appliedIndex;
    }

    /** Why the runtime stopped working, or {@code null}. */
    public Throwable failure() {
        return failure;
    }

    @Override
    public void close() {
        if (stopping) {
            return;
        }
        stopping = true;
        raftThread.interrupt();
        join(raftThread);
        // Only the Raft thread sends, so the transport closes once it has stopped.
        transport.close();
        applyQueue.add(new Stop());
        join(applyThread);
        RuntimeException closed = new IllegalStateException("The Raft node is shutting down");
        proposals.values().forEach(future -> future.completeExceptionally(closed));
        accepted.values().forEach(pending -> pending.future().completeExceptionally(
                new OutcomeUnknownException("The Raft node shut down before the proposal was applied")));
        reads.values().forEach(future -> future.completeExceptionally(closed));
        try {
            storage.close();
        } catch (IOException e) {
            log.warn("Could not close the Raft storage", e);
        }
        log.info("Raft node {} stopped", nodeId);
    }

    private boolean usable(CompletableFuture<?> future) {
        Throwable cause = failure;
        if (cause != null) {
            future.completeExceptionally(new IllegalStateException("The Raft node failed", cause));
            return false;
        }
        if (stopping) {
            future.completeExceptionally(new IllegalStateException("The Raft node is shutting down"));
            return false;
        }
        return true;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Raft thread

    private void runRaft() {
        List<RaftEvent> batch = new ArrayList<>(MAX_BATCH);
        List<RaftAction> actions = new ArrayList<>();
        long nextTick = clock.millis() + options.tickMillis();
        updateMdc();
        try {
            while (!stopping) {
                long wait = nextTick - clock.millis();
                RaftEvent first = wait > 0 ? events.poll(wait, TimeUnit.MILLISECONDS) : events.poll();
                if (first != null) {
                    batch.add(first);
                    events.drainTo(batch, MAX_BATCH - 1);
                }
                if (clock.millis() >= nextTick) {
                    batch.add(RaftEvent.TICK);
                    nextTick = clock.millis() + options.tickMillis();
                }
                for (RaftEvent event : batch) {
                    actions.addAll(node.handle(event));
                }
                execute(actions);
                for (RaftEvent event : batch) {
                    if (event instanceof RaftMessage.InstallSnapshot install) {
                        install.payload().discard();
                    }
                }
                actions.clear();
                batch.clear();
                status = node.status();
                updateMdc();
            }
        } catch (InterruptedException e) {
            // close()
        } catch (Throwable t) {
            fail(t);
        }
    }

    private void execute(List<RaftAction> actions) throws IOException {
        boolean persisted = false;
        for (RaftAction action : actions) {
            if (action instanceof RaftAction.Persist persist) {
                storage.apply(persist);
                persisted = true;
            }
        }
        if (persisted) {
            storage.sync();
        }
        for (RaftAction action : actions) {
            switch (action) {
                case RaftAction.RestoreSnapshot restore -> applyQueue.add(new Restore(restore.index(), restore.term()));
                case RaftAction.Persist _ -> {
                }
                case RaftAction.Send send -> transport.send(send.message());
                case RaftAction.SendSnapshot request -> sendSnapshot(request);
                case RaftAction.Apply apply -> applyQueue.add(new ApplyEntries(apply.entries()));
                case RaftAction.ProposalAccepted accepted -> {
                    CompletableFuture<Object> future = proposals.remove(accepted.requestId());
                    if (future != null) {
                        Pending overwritten = this.accepted.put(accepted.index(),
                                new Pending(accepted.term(), future));
                        if (overwritten != null) {
                            // An earlier term's proposal at this index was truncated away: it never committed.
                            overwritten.future().completeExceptionally(new NotLeaderException(nodeId));
                        }
                    }
                }
                case RaftAction.ProposalRejected rejected -> {
                    CompletableFuture<Object> future = proposals.remove(rejected.requestId());
                    if (future != null) {
                        future.completeExceptionally(new NotLeaderException(rejected.leaderHint()));
                    }
                }
                case RaftAction.ReadReady ready -> {
                    CompletableFuture<Long> future = reads.remove(ready.requestId());
                    if (future != null) {
                        applyQueue.add(new Read(ready.readIndex(), future));
                    }
                }
                case RaftAction.ReadRejected rejected -> {
                    CompletableFuture<Long> future = reads.remove(rejected.requestId());
                    if (future != null) {
                        future.completeExceptionally(new NotLeaderException(rejected.leaderHint()));
                    }
                }
                case RaftAction.RoleChanged changed -> logRoleChange(changed);
            }
        }
    }

    private void sendSnapshot(RaftAction.SendSnapshot request) {
        Optional<SnapshotStore.SnapshotFile> latest = storage.snapshots().latest();
        if (latest.isEmpty()) {
            log.error("Asked to send snapshot {} to {} but there is none", request.snapshotIndex(), request.to());
            return;
        }
        SnapshotStore.SnapshotFile file = latest.get();
        log.info("Sending snapshot {}@{} ({} bytes) to {}", file.index(), file.term(), file.bodySize(), request.to());
        transport.send(new RaftMessage.InstallSnapshot(nodeId, request.to(), request.term(), file.index(),
                file.term(), request.round(), file.payload()));
    }

    private void logRoleChange(RaftAction.RoleChanged changed) {
        putMdc(changed.term(), status.lastLogIndex());
        switch (changed.role()) {
            case LEADER -> log.info("Became the leader of term {}", changed.term());
            case CANDIDATE -> log.info("Starting an election for term {}", changed.term());
            case PRE_CANDIDATE -> log.debug("Election timeout: asking for pre-votes for term {}", changed.term() + 1);
            case FOLLOWER -> log.info("Following {} in term {}",
                    changed.leaderId() == null ? "no leader yet" : changed.leaderId(), changed.term());
        }
    }

    private void updateMdc() {
        RaftStatus current = status;
        putMdc(current.term(), current.lastLogIndex());
    }

    /** Structured keys {@code term} and {@code index}, and both as text in {@code raft} for plain log patterns. */
    private static void putMdc(long term, long index) {
        MDC.put("term", Long.toString(term));
        MDC.put("index", Long.toString(index));
        MDC.put("raft", " term=" + term + " index=" + index);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Apply thread

    private void runApply() {
        PriorityQueue<Read> waitingReads = new PriorityQueue<>((a, b) -> Long.compare(a.readIndex(), b.readIndex()));
        long lastSnapshotIndex = storage.snapshots().latest().map(SnapshotStore.SnapshotFile::index).orElse(0L);
        long appliedTerm = 0;
        try {
            while (true) {
                ApplyTask task = applyQueue.take();
                switch (task) {
                    case Stop _ -> {
                        return;
                    }
                    case ApplyEntries apply -> {
                        LogEntry first = apply.entries().getFirst();
                        putMdc(first.term(), first.index());
                        for (LogEntry entry : apply.entries()) {
                            if (entry.index() <= appliedIndex) {
                                continue; // covered by a snapshot restored ahead of it
                            }
                            if (entry.index() != appliedIndex + 1) {
                                throw new IllegalStateException("asked to apply " + entry + " after index "
                                        + appliedIndex);
                            }
                            Object result;
                            try {
                                result = stateMachine.apply(entry);
                            } catch (RuntimeException e) {
                                throw new IllegalStateException("applying " + entry + " failed", e);
                            }
                            appliedIndex = entry.index();
                            appliedTerm = entry.term();
                            Pending pending = accepted.remove(entry.index());
                            if (pending != null) {
                                if (pending.term() == entry.term()) {
                                    pending.future().complete(result);
                                } else {
                                    // A later leader overwrote the proposal's entry: it never committed.
                                    pending.future().completeExceptionally(new NotLeaderException(status.leaderId()));
                                }
                            }
                        }
                        if (appliedIndex - lastSnapshotIndex >= options.snapshotThreshold()) {
                            lastSnapshotIndex = takeSnapshot(appliedIndex, appliedTerm);
                        }
                    }
                    case Restore restore -> {
                        // Restore the newest snapshot, which may already supersede this one: a second install can
                        // happen before this task runs, and the older file is gone by then. Entries the newer
                        // snapshot covers are skipped above.
                        SnapshotStore.SnapshotFile file = storage.snapshots().latest()
                                .filter(latest -> latest.index() >= restore.index())
                                .orElseThrow(() -> new IllegalStateException("snapshot " + restore.index()
                                        + " to restore is missing"));
                        if (file.index() > appliedIndex) {
                            try (InputStream in = SnapshotStore.openBody(file)) {
                                stateMachine.restoreSnapshot(file.index(), file.term(), in);
                            }
                            appliedIndex = file.index();
                            appliedTerm = file.term();
                            lastSnapshotIndex = file.index();
                            long restored = file.index();
                            accepted.keySet().stream().filter(index -> index <= restored).toList()
                                    .forEach(index -> accepted.remove(index).future().completeExceptionally(
                                            new OutcomeUnknownException("replaced by a snapshot from the leader")));
                            log.info("Restored the state machine from the leader's snapshot {}@{}", file.index(),
                                    file.term());
                        }
                    }
                    case Read read -> waitingReads.add(read);
                }
                while (!waitingReads.isEmpty() && waitingReads.peek().readIndex() <= appliedIndex) {
                    Read read = waitingReads.poll();
                    read.future().complete(read.readIndex());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            fail(t);
            waitingReads.forEach(read -> read.future().completeExceptionally(
                    new IllegalStateException("The Raft node failed", t)));
        }
    }

    private long takeSnapshot(long index, long term) throws IOException {
        long started = System.nanoTime();
        SnapshotStore.SnapshotFile file = storage.snapshots().write(index, term, stateMachine::writeSnapshot);
        events.add(new RaftEvent.SnapshotTaken(index, term));
        log.info("Took snapshot {}@{} ({} bytes) in {} ms", index, term, file.bodySize(),
                (System.nanoTime() - started) / 1_000_000);
        return index;
    }

    // ---------------------------------------------------------------------------------------------------------------

    private void fail(Throwable cause) {
        if (failure == null) {
            failure = cause;
            log.error("The Raft node failed and stops serving", cause);
        }
        stopping = true;
        RuntimeException failed = new IllegalStateException("The Raft node failed", cause);
        proposals.values().forEach(future -> future.completeExceptionally(failed));
        accepted.values().forEach(pending -> pending.future().completeExceptionally(failed));
        reads.values().forEach(future -> future.completeExceptionally(failed));
    }

    private void withMdc(Map<String, String> mdc, Runnable body) {
        if (mdc != null) {
            MDC.setContextMap(mdc);
        }
        MDC.put("nodeId", nodeId);
        body.run();
    }

    private static void join(Thread thread) {
        try {
            thread.join(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
