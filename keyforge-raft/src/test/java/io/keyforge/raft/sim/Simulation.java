package io.keyforge.raft.sim;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

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
import io.keyforge.raft.SnapshotPayload;

/**
 * A deterministic, seeded simulation of a 3–5 node Raft cluster on a hostile network.
 *
 * <p>Every source of nondeterminism comes from one {@link SplittableRandom} seeded with the seed: the cluster size and
 * fault rates, message loss, duplication and delays (which reorder messages), partitions (symmetric and one-way), node
 * crashes and restarts (including crashes part-way through the actions of one event, between a persist and a send),
 * each node's clock rate, election timeouts, and the client workload. Time is virtual. The same seed therefore replays
 * the same run exactly, and {@link SimulationResult#traceHash()} proves it.
 *
 * <p>After every step {@link Invariants} checks election safety, log matching, state machine safety, leader
 * completeness and read linearizability, and each live node's durable state is compared with its core. After the fault
 * phase, the network heals, every node restarts, and the run fails unless the cluster elects a leader and every node
 * applies a final write (liveness).
 */
public final class Simulation {

    /** Deliberate bugs, for checking that the simulator catches them. */
    enum Mutation {
        NONE,
        /** The driver sends messages before persisting the term, vote and log changes that precede them. */
        SEND_BEFORE_PERSIST,
        /** The disk never stores the vote, so a restarted node can vote twice in one term. */
        FORGET_VOTE,
        /** A node that believes it is the leader serves reads at its commit index without ReadIndex's round trip. */
        UNCONFIRMED_LEADER_READS
    }

    /** Knobs of one run; {@link #random} draws them from the seed. */
    record Options(int nodes, long faultMillis, long quietMillis, double dropRate, double duplicateRate,
                   long minDelayMillis, long maxDelayMillis, double slowRate, long slowDelayMillis,
                   double crashesPerSecond, double partitionsPerSecond, double midActionCrashRate,
                   int snapshotThreshold, double imageFlushRate, long writeIntervalMillis, long readIntervalMillis,
                   int maxEntriesPerAppend, int maxInflightAppends, boolean preVote, boolean checkQuorum,
                   Mutation mutation) {

        static Options random(SplittableRandom rng) {
            return new Options(
                    3 + rng.nextInt(3),
                    3_000 + rng.nextLong(4_000),
                    10_000,
                    rng.nextDouble() * 0.15,
                    rng.nextDouble() * 0.05,
                    1,
                    5 + rng.nextLong(30),
                    rng.nextDouble() * 0.05,
                    100 + rng.nextLong(400),
                    rng.nextDouble() * 1.5,
                    rng.nextDouble() * 0.8,
                    rng.nextDouble() * 0.01,
                    5 + rng.nextInt(60),
                    rng.nextDouble() * 0.2,
                    5 + rng.nextLong(40),
                    10 + rng.nextLong(60),
                    1 + rng.nextInt(32),
                    1 + rng.nextInt(8),
                    rng.nextInt(5) != 0,
                    rng.nextInt(5) != 0,
                    Mutation.NONE);
        }

        Options withMutation(Mutation newMutation) {
            return new Options(nodes, faultMillis, quietMillis, dropRate, duplicateRate, minDelayMillis,
                    maxDelayMillis, slowRate, slowDelayMillis, crashesPerSecond, partitionsPerSecond,
                    midActionCrashRate, snapshotThreshold, imageFlushRate, writeIntervalMillis, readIntervalMillis,
                    maxEntriesPerAppend, maxInflightAppends, preVote, checkQuorum, newMutation);
        }
    }

    private static final long TICK_MILLIS = 10;
    private static final long ELECTION_TIMEOUT_MIN = 150;
    private static final long ELECTION_TIMEOUT_MAX = 300;
    private static final long HEARTBEAT = 50;
    private static final int MAX_ATTEMPTS = 30;
    private static final long WRITE_TIMEOUT_MILLIS = 2_000;
    private static final int KEYS = 8;
    private static final int TRACE_LINES = 400;
    private static final long MAX_STEPS = 5_000_000;

    private final long seed;
    private final SplittableRandom rng;
    private final Options options;
    private final List<SimNode> nodes = new ArrayList<>();
    private final List<String> members;
    private final Invariants invariants = new Invariants();
    private final PriorityQueue<Scheduled> queue =
            new PriorityQueue<>(Comparator.comparingLong(Scheduled::time).thenComparingLong(Scheduled::seq));
    private final boolean[][] blocked;
    /** Client requests on their way to (or waiting at) a node, by request id. */
    private final Map<Long, Attempt> requests = new HashMap<>();
    private final ArrayDeque<String> trace;

    private long now;
    private long sequence;
    private long nextRequestId = 1;
    private long nextOpId = 1;
    private boolean faults = true;
    private long traceHash = 17;
    private ClientOp finalWrite;

    // Statistics.
    private int leadersElected;
    private int writesAcknowledged;
    private int writesFailed;
    private int readsServed;
    private int snapshotsInstalled;
    private int crashes;
    private long steps;

    private Simulation(long seed, Options options, boolean verbose) {
        this.seed = seed;
        this.rng = new SplittableRandom(seed);
        this.options = options != null ? options : Options.random(rng);
        this.members = IntStream.rangeClosed(1, this.options.nodes()).mapToObj(i -> "n" + i).toList();
        this.blocked = new boolean[this.options.nodes()][this.options.nodes()];
        this.trace = verbose ? new ArrayDeque<>() : null;
        for (int i = 0; i < this.options.nodes(); i++) {
            nodes.add(new SimNode(i, members.get(i), 0.95 + rng.nextDouble() * 0.1, rng.nextLong(1_000_000)));
        }
    }

    /** Runs the simulation for {@code seed}; throws {@link InvariantViolation} (or the core's exception) on failure. */
    public static SimulationResult run(long seed) {
        return new Simulation(seed, null, false).execute();
    }

    /** Runs {@code seed} with a deliberate bug; the options are still drawn from the seed. */
    static SimulationResult runMutated(long seed, Mutation mutation) {
        SplittableRandom optionsRng = new SplittableRandom(seed);
        Options options = Options.random(optionsRng).withMutation(mutation);
        return new Simulation(seed, options, false).execute();
    }

    /** Re-runs {@code seed} with tracing, and returns the last events before the failure (or the end). */
    static String traceOf(long seed, Mutation mutation) {
        Options options = mutation == Mutation.NONE ? null
                : Options.random(new SplittableRandom(seed)).withMutation(mutation);
        Simulation simulation = new Simulation(seed, options, true);
        try {
            simulation.execute();
            return "(no failure)\n" + String.join("\n", simulation.trace);
        } catch (RuntimeException e) {
            return e + "\n--- last " + simulation.trace.size() + " events ---\n" + String.join("\n", simulation.trace);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Main loop

    private SimulationResult execute() {
        for (SimNode node : nodes) {
            start(node);
        }
        schedule(exponential(options.writeIntervalMillis()), new ClientWrite());
        schedule(exponential(options.readIntervalMillis()), new ClientRead());
        if (options.crashesPerSecond() > 0) {
            schedule(exponential(1000 / options.crashesPerSecond()), new Crash());
        }
        if (options.partitionsPerSecond() > 0) {
            schedule(exponential(1000 / options.partitionsPerSecond()), new PartitionStart());
        }
        schedule(options.faultMillis(), new EndFaults());
        long end = options.faultMillis() + options.quietMillis();
        try {
            while (!queue.isEmpty()) {
                Scheduled next = queue.poll();
                if (next.time() > end) {
                    break;
                }
                now = next.time();
                if (++steps > MAX_STEPS) {
                    throw new InvariantViolation("too many steps");
                }
                process(next.event());
            }
            checkLiveness();
        } catch (RuntimeException e) {
            throw new SimulationFailure(seed, options, now, steps, e);
        }
        return new SimulationResult(seed, options.nodes(), steps, traceHash, leadersElected, writesAcknowledged,
                writesFailed, readsServed, snapshotsInstalled, crashes, invariants.maxApplied());
    }

    private void process(SimEvent event) {
        record(event);
        switch (event) {
            case Deliver deliver -> {
                RaftMessage message = deliver.message();
                int from = indexOf(message.from());
                int to = indexOf(message.to());
                if (!blocked[from][to]) {
                    handle(nodes.get(to), message);
                }
            }
            case Tick tick -> {
                SimNode node = nodes.get(tick.node());
                if (node.up && node.incarnation == tick.incarnation()) {
                    handle(node, RaftEvent.TICK);
                    schedule(TICK_MILLIS, tick);
                }
            }
            case Internal internal -> {
                SimNode node = nodes.get(internal.node());
                if (node.up && node.incarnation == internal.incarnation()) {
                    handle(node, internal.event());
                }
            }
            case ClientRequest request -> deliverClientRequest(request);
            case ClientWrite _ -> {
                if (faults) {
                    String key = "k" + rng.nextInt(KEYS);
                    ClientOp op = new ClientOp(nextOpId++, true, key, KvMachine.command(key, "v" + nextOpId),
                            invariants.maxApplied());
                    submit(op, pickTarget());
                    schedule(exponential(options.writeIntervalMillis()), event);
                }
            }
            case ClientRead _ -> {
                if (faults) {
                    ClientOp op = new ClientOp(nextOpId++, false, "k" + rng.nextInt(KEYS), null,
                            invariants.maxApplied());
                    submit(op, pickTarget());
                    schedule(exponential(options.readIntervalMillis()), event);
                }
            }
            case Retry retry -> retry(retry.op(), retry.hint());
            case WriteTimeout timeout -> {
                if (!timeout.op().done && timeout.op().attempts == timeout.attempt()) {
                    retry(timeout.op(), null);
                }
            }
            case Crash _ -> {
                if (faults) {
                    List<SimNode> up = nodes.stream().filter(node -> node.up).toList();
                    if (!up.isEmpty()) {
                        SimNode victim = up.get(rng.nextInt(up.size()));
                        crash(victim);
                        schedule(100 + rng.nextLong(3_000), new Restart(victim.index));
                    }
                    schedule(exponential(1000 / options.crashesPerSecond()), event);
                }
            }
            case Restart restart -> {
                SimNode node = nodes.get(restart.node());
                if (!node.up) {
                    start(node);
                }
            }
            case PartitionStart _ -> {
                if (faults) {
                    partition();
                    schedule(200 + rng.nextLong(3_000), new PartitionHeal());
                }
            }
            case PartitionHeal _ -> {
                heal();
                if (faults) {
                    schedule(exponential(1000 / options.partitionsPerSecond()), new PartitionStart());
                }
            }
            case EndFaults _ -> {
                faults = false;
                heal();
                for (SimNode node : nodes) {
                    if (!node.up) {
                        start(node);
                    }
                }
                String key = "k" + rng.nextInt(KEYS);
                finalWrite = new ClientOp(nextOpId++, true, key, KvMachine.command(key, "final"),
                        invariants.maxApplied());
                submit(finalWrite, pickTarget());
            }
        }
    }

    private void handle(SimNode node, RaftEvent event) {
        if (!node.up) {
            return;
        }
        List<RaftAction> actions = node.core.handle(event);
        if (options.mutation() == Mutation.SEND_BEFORE_PERSIST) {
            List<RaftAction> reordered = new ArrayList<>();
            actions.stream().filter(action -> !(action instanceof RaftAction.Persist)).forEach(reordered::add);
            actions.stream().filter(action -> action instanceof RaftAction.Persist).forEach(reordered::add);
            actions = reordered;
        }
        int limit = actions.size();
        boolean crashAfter = false;
        if (faults && !actions.isEmpty() && rng.nextDouble() < options.midActionCrashRate()) {
            // The process dies part-way through: only a prefix of the actions took effect.
            limit = rng.nextInt(actions.size());
            crashAfter = true;
        }
        for (int i = 0; i < limit && node.up; i++) {
            execute(node, actions.get(i));
        }
        if (crashAfter && node.up) {
            crash(node);
            schedule(50 + rng.nextLong(1_000), new Restart(node.index));
        }
        if (node.up) {
            checkNode(node);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The simulated driver

    private void execute(SimNode node, RaftAction action) {
        SimDisk disk = node.disk;
        switch (action) {
            case RaftAction.PersistState state -> {
                disk.term = state.term();
                disk.votedFor = options.mutation() == Mutation.FORGET_VOTE ? null : state.votedFor();
            }
            case RaftAction.AppendLog append -> {
                for (LogEntry entry : append.entries()) {
                    long previousTerm = entry.index() - 1 >= disk.logBaseIndex ? disk.termAt(entry.index() - 1) : -1;
                    invariants.onPersisted(node.id, entry, previousTerm);
                    disk.append(List.of(entry));
                }
            }
            case RaftAction.TruncateLog truncate -> disk.truncateFrom(truncate.fromIndex());
            case RaftAction.CompactLog compact -> disk.compactThrough(compact.snapshotIndex(), compact.snapshotTerm());
            case RaftAction.RestoreSnapshot restore -> {
                byte[] bytes = read(restore.payload());
                disk.installSnapshot(restore.index(), restore.term(), bytes);
                node.machine = KvMachine.deserialize(bytes);
                if (node.machine.appliedIndex() != restore.index() || node.machine.appliedTerm() != restore.term()) {
                    throw new InvariantViolation(node.id + " installed a snapshot of " + node.machine.appliedIndex()
                            + "@" + node.machine.appliedTerm() + " announced as " + restore.index() + "@"
                            + restore.term());
                }
                invariants.checkState(node.id, "installed", node.machine);
                if (rng.nextBoolean()) {
                    disk.image = node.machine.copy();
                }
                // Writes this node accepted up to the snapshot are settled by what the cluster applied there.
                List<Long> covered = node.pendingWrites.keySet().stream()
                        .filter(index -> index <= restore.index()).sorted().toList();
                for (long index : covered) {
                    completeWrite(node.pendingWrites.remove(index), invariants.appliedEntry(index));
                }
                snapshotsInstalled++;
            }
            case RaftAction.Send send -> send(send.message());
            case RaftAction.SendSnapshot request -> {
                if (disk.snapshot == null || disk.snapshotIndex < request.snapshotIndex()) {
                    throw new InvariantViolation(node.id + " was asked to send snapshot " + request.snapshotIndex()
                            + " but holds " + disk.snapshotIndex);
                }
                send(new RaftMessage.InstallSnapshot(node.id, request.to(), request.term(), disk.snapshotIndex,
                        disk.snapshotTerm, request.round(), SnapshotPayload.of(disk.snapshot)));
            }
            case RaftAction.Apply apply -> {
                for (LogEntry entry : apply.entries()) {
                    node.machine.apply(entry);
                    invariants.onApplied(node.id, entry, node.machine);
                    Attempt attempt = node.pendingWrites.remove(entry.index());
                    if (attempt != null) {
                        completeWrite(attempt, entry);
                    }
                    if (rng.nextDouble() < options.imageFlushRate()) {
                        disk.image = node.machine.copy();
                    }
                }
                if (node.machine.appliedIndex() - disk.snapshotIndex >= options.snapshotThreshold()) {
                    disk.snapshotIndex = node.machine.appliedIndex();
                    disk.snapshotTerm = node.machine.appliedTerm();
                    disk.snapshot = node.machine.serialize();
                    schedule(0, new Internal(node.index, node.incarnation,
                            new RaftEvent.SnapshotTaken(disk.snapshotIndex, disk.snapshotTerm)));
                }
            }
            case RaftAction.ProposalAccepted accepted -> {
                Attempt attempt = requests.remove(accepted.requestId());
                if (attempt != null) {
                    attempt.op().index = accepted.index();
                    node.pendingWrites.put(accepted.index(), new Attempt(attempt.op(), attempt.number(),
                            accepted.term()));
                    // Like a real client, give up waiting after a while and try again.
                    schedule(WRITE_TIMEOUT_MILLIS, new WriteTimeout(attempt.op(), attempt.number()));
                }
            }
            case RaftAction.ProposalRejected rejected -> {
                Attempt attempt = requests.remove(rejected.requestId());
                if (attempt != null) {
                    schedule(5 + rng.nextLong(20), new Retry(attempt.op(), rejected.leaderHint()));
                }
            }
            case RaftAction.ReadReady ready -> {
                Attempt attempt = requests.remove(ready.requestId());
                if (attempt != null) {
                    serveRead(node, attempt.op(), ready.readIndex());
                }
            }
            case RaftAction.ReadRejected rejected -> {
                Attempt attempt = requests.remove(rejected.requestId());
                if (attempt != null) {
                    schedule(5 + rng.nextLong(20), new Retry(attempt.op(), rejected.leaderHint()));
                }
            }
            case RaftAction.RoleChanged changed -> {
                if (changed.role() == Role.LEADER) {
                    invariants.onLeaderElected(node.id, changed.term(), disk);
                    leadersElected++;
                }
            }
        }
    }

    /** A live node's durable state must mirror its core after every event. */
    private void checkNode(SimNode node) {
        RaftStatus status = node.core.status();
        SimDisk disk = node.disk;
        if (disk.lastIndex() != status.lastLogIndex() || disk.lastTerm() != status.lastLogTerm()) {
            throw new InvariantViolation(node.id + ": the disk log ends at " + disk.lastIndex() + "@" + disk.lastTerm()
                    + " but the core's at " + status.lastLogIndex() + "@" + status.lastLogTerm());
        }
        if (disk.term != status.term()) {
            throw new InvariantViolation(node.id + ": the disk holds term " + disk.term + " but the core is in "
                    + status.term());
        }
        if (node.machine.appliedIndex() != status.lastApplied() || status.commitIndex() < status.lastApplied()) {
            throw new InvariantViolation(node.id + ": applied " + node.machine.appliedIndex() + ", core says applied "
                    + status.lastApplied() + " and committed " + status.commitIndex());
        }
    }

    private void start(SimNode node) {
        SimDisk disk = node.disk;
        PersistentState state = disk.load();
        KvMachine machine;
        if (disk.image != null && disk.image.appliedIndex() >= state.snapshotIndex()) {
            machine = disk.image.copy();
        } else if (disk.snapshot != null) {
            machine = KvMachine.deserialize(disk.snapshot);
        } else {
            machine = new KvMachine();
        }
        invariants.checkState(node.id, "restarted with", machine);
        RaftConfig config = new RaftConfig(node.id, members, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, HEARTBEAT,
                options.maxEntriesPerAppend(), 1 << 20, options.maxInflightAppends(), 1_000, options.preVote(),
                options.checkQuorum());
        node.core = new RaftNode(config, node.clock, new SplittableRandom(rng.nextLong()), state,
                machine.appliedIndex());
        node.machine = machine;
        node.up = true;
        node.incarnation++;
        schedule(rng.nextLong(TICK_MILLIS), new Tick(node.index, node.incarnation));
    }

    private void crash(SimNode node) {
        node.up = false;
        node.core = null;
        node.machine = null;
        node.incarnation++;
        node.pendingWrites.clear();
        crashes++;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Network

    private void send(RaftMessage message) {
        int from = indexOf(message.from());
        int to = indexOf(message.to());
        if (blocked[from][to]) {
            return;
        }
        if (faults && rng.nextDouble() < options.dropRate()) {
            return;
        }
        int copies = faults && rng.nextDouble() < options.duplicateRate() ? 2 : 1;
        for (int i = 0; i < copies; i++) {
            schedule(delay(), new Deliver(message));
        }
    }

    private long delay() {
        if (faults && rng.nextDouble() < options.slowRate()) {
            return options.minDelayMillis() + rng.nextLong(options.slowDelayMillis());
        }
        return options.minDelayMillis() + rng.nextLong(options.maxDelayMillis() - options.minDelayMillis() + 1);
    }

    private void partition() {
        int n = nodes.size();
        switch (rng.nextInt(3)) {
            case 0 -> {
                int isolated = rng.nextInt(n);
                for (int other = 0; other < n; other++) {
                    blocked[isolated][other] = other != isolated;
                    blocked[other][isolated] = other != isolated;
                }
            }
            case 1 -> {
                boolean[] side = new boolean[n];
                for (int i = 0; i < n; i++) {
                    side[i] = rng.nextBoolean();
                }
                for (int a = 0; a < n; a++) {
                    for (int b = 0; b < n; b++) {
                        blocked[a][b] = side[a] != side[b];
                    }
                }
            }
            default -> {
                int from = rng.nextInt(n);
                int to = (from + 1 + rng.nextInt(n - 1)) % n;
                blocked[from][to] = true;
            }
        }
    }

    private void heal() {
        for (boolean[] row : blocked) {
            java.util.Arrays.fill(row, false);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Clients

    private int pickTarget() {
        // Usually the node most recently seen as leader, sometimes any node (exercising rejections).
        if (rng.nextInt(4) != 0) {
            for (SimNode node : nodes) {
                if (node.up && node.core.role() == Role.LEADER) {
                    return node.index;
                }
            }
        }
        return rng.nextInt(nodes.size());
    }

    private void submit(ClientOp op, int target) {
        op.attempts++;
        long requestId = nextRequestId++;
        requests.put(requestId, new Attempt(op, op.attempts, 0));
        RaftEvent event = op.write
                ? new RaftEvent.Propose(requestId, op.command)
                : new RaftEvent.ReadIndex(requestId);
        schedule(1 + rng.nextLong(5), new ClientRequest(target, requestId, op, event));
    }

    private void deliverClientRequest(ClientRequest request) {
        SimNode node = nodes.get(request.node());
        if (!node.up) {
            requests.remove(request.requestId());
            schedule(10 + rng.nextLong(30), new Retry(request.op(), null));
            return;
        }
        if (!request.op().write && options.mutation() == Mutation.UNCONFIRMED_LEADER_READS
                && node.core.role() == Role.LEADER) {
            requests.remove(request.requestId());
            serveRead(node, request.op(), node.core.status().commitIndex());
            return;
        }
        handle(node, request.event());
    }

    private void retry(ClientOp op, String hint) {
        if (op.done || (op != finalWrite && (op.attempts >= MAX_ATTEMPTS || !faults))) {
            return;
        }
        int target = hint != null && nodes.get(indexOf(hint)).up ? indexOf(hint) : rng.nextInt(nodes.size());
        submit(op, target);
    }

    private void completeWrite(Attempt attempt, LogEntry entry) {
        ClientOp op = attempt.op();
        if (entry.term() == attempt.term()) {
            if (!java.util.Arrays.equals(entry.data(), op.command)) {
                throw new InvariantViolation("write " + op.id + " was acknowledged at " + entry.index()
                        + " but a different command was applied there");
            }
            if (!op.done) {
                op.done = true;
                writesAcknowledged++;
            }
        } else if (!op.done && op.attempts == attempt.number()) {
            // Its entry was overwritten by a later leader: it never committed, so retrying is safe.
            writesFailed++;
            schedule(5 + rng.nextLong(20), new Retry(op, null));
        }
    }

    private void serveRead(SimNode node, ClientOp op, long readIndex) {
        if (readIndex < op.maxAppliedAtStart) {
            throw new InvariantViolation("stale read: " + node.id + " served read " + op.id + " at index " + readIndex
                    + " but index " + op.maxAppliedAtStart + " was applied before the read began");
        }
        if (node.machine.appliedIndex() < readIndex) {
            throw new InvariantViolation(node.id + " was told to serve a read at " + readIndex + " but has applied only "
                    + node.machine.appliedIndex());
        }
        node.machine.get(op.key);
        op.done = true;
        readsServed++;
    }

    private void checkLiveness() {
        for (SimNode node : nodes) {
            if (!node.up) {
                throw new InvariantViolation("liveness: " + node.id + " is down at the end");
            }
        }
        long leaders = nodes.stream().filter(node -> node.core.role() == Role.LEADER).count();
        if (leaders == 0) {
            throw new InvariantViolation("liveness: no leader after " + options.quietMillis() + " ms of calm");
        }
        if (finalWrite == null || !finalWrite.done) {
            throw new InvariantViolation("liveness: the final write did not commit after " + options.quietMillis()
                    + " ms of calm");
        }
        long applied = nodes.getFirst().machine.appliedIndex();
        for (SimNode node : nodes) {
            if (node.machine.appliedIndex() != applied || applied < finalWrite.index) {
                throw new InvariantViolation("liveness: nodes did not converge: " + nodes.stream()
                        .map(n -> n.id + " applied " + n.machine.appliedIndex()).toList()
                        + ", final write at " + finalWrite.index);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Plumbing

    private void schedule(long delay, SimEvent event) {
        queue.add(new Scheduled(now + delay, sequence++, event));
    }

    private long exponential(double mean) {
        return 1 + (long) (-Math.log(1 - rng.nextDouble()) * mean);
    }

    private int indexOf(String id) {
        return Integer.parseInt(id, 1, id.length(), 10) - 1;
    }

    private static byte[] read(SnapshotPayload payload) {
        try (InputStream in = payload.open()) {
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void record(SimEvent event) {
        long h = traceHash * 31 + now;
        h = h * 31 + event.getClass().getSimpleName().hashCode();
        if (event instanceof Deliver deliver) {
            RaftMessage message = deliver.message();
            h = h * 31 + message.getClass().getSimpleName().hashCode();
            h = h * 31 + message.from().hashCode();
            h = h * 31 + message.to().hashCode();
            h = h * 31 + message.term();
        } else if (event instanceof Tick tick) {
            h = h * 31 + tick.node();
        }
        traceHash = h;
        if (trace != null) {
            if (trace.size() == TRACE_LINES) {
                trace.removeFirst();
            }
            trace.addLast(now + " " + describe(event) + "   " + nodes.stream()
                    .map(node -> node.up ? node.core.status().toString() : node.id + " DOWN").toList());
        }
    }

    private static String describe(SimEvent event) {
        return switch (event) {
            case Deliver deliver -> "deliver " + deliver.message();
            case Tick tick -> "tick n" + (tick.node() + 1);
            case ClientRequest request -> "client " + request.event() + " -> n" + (request.node() + 1);
            default -> event.toString();
        };
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Simulation state

    private record Scheduled(long time, long seq, SimEvent event) {
    }

    private sealed interface SimEvent {
    }

    private record Deliver(RaftMessage message) implements SimEvent {
    }

    private record Tick(int node, long incarnation) implements SimEvent {
    }

    private record Internal(int node, long incarnation, RaftEvent event) implements SimEvent {
    }

    private record ClientRequest(int node, long requestId, ClientOp op, RaftEvent event) implements SimEvent {
    }

    private record ClientWrite() implements SimEvent {
    }

    private record ClientRead() implements SimEvent {
    }

    private record Retry(ClientOp op, String hint) implements SimEvent {
    }

    private record WriteTimeout(ClientOp op, int attempt) implements SimEvent {
    }

    /** One attempt of a client operation; {@code term} is known once a leader accepted it. */
    private record Attempt(ClientOp op, int number, long term) {
    }

    private record Crash() implements SimEvent {
    }

    private record Restart(int node) implements SimEvent {
    }

    private record PartitionStart() implements SimEvent {
    }

    private record PartitionHeal() implements SimEvent {
    }

    private record EndFaults() implements SimEvent {
    }

    private final class SimNode {
        final int index;
        final String id;
        final SimDisk disk = new SimDisk();
        final Clock clock;
        /** Writes accepted by this node as leader, by log index. */
        final Map<Long, Attempt> pendingWrites = new HashMap<>();
        RaftNode core;
        KvMachine machine;
        boolean up;
        long incarnation;

        SimNode(int index, String id, double clockRate, long clockOffset) {
            this.index = index;
            this.id = id;
            // Each node's clock runs at its own rate; only intervals matter to Raft.
            this.clock = () -> clockOffset + (long) (now * clockRate);
        }
    }

    private static final class ClientOp {
        final long id;
        final boolean write;
        final String key;
        final byte[] command;
        /** The highest index applied anywhere when the operation began; a read must reflect at least that. */
        final long maxAppliedAtStart;
        int attempts;
        /** Log index of the latest accepted attempt. */
        long index;
        boolean done;

        ClientOp(long id, boolean write, String key, byte[] command, long maxAppliedAtStart) {
            this.id = id;
            this.write = write;
            this.key = key;
            this.command = command;
            this.maxAppliedAtStart = maxAppliedAtStart;
        }

        @Override
        public String toString() {
            return (write ? "write " : "read ") + id;
        }
    }
}
