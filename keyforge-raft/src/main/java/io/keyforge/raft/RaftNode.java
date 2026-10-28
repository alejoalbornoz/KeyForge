package io.keyforge.raft;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.random.RandomGenerator;

import io.keyforge.raft.RaftAction.AppendLog;
import io.keyforge.raft.RaftAction.Apply;
import io.keyforge.raft.RaftAction.CompactLog;
import io.keyforge.raft.RaftAction.PersistState;
import io.keyforge.raft.RaftAction.ProposalAccepted;
import io.keyforge.raft.RaftAction.ProposalRejected;
import io.keyforge.raft.RaftAction.ReadReady;
import io.keyforge.raft.RaftAction.ReadRejected;
import io.keyforge.raft.RaftAction.RestoreSnapshot;
import io.keyforge.raft.RaftAction.RoleChanged;
import io.keyforge.raft.RaftAction.Send;
import io.keyforge.raft.RaftAction.SendSnapshot;
import io.keyforge.raft.RaftAction.TruncateLog;
import io.keyforge.raft.RaftEvent.Propose;
import io.keyforge.raft.RaftEvent.ReadIndex;
import io.keyforge.raft.RaftEvent.SnapshotTaken;
import io.keyforge.raft.RaftEvent.Tick;
import io.keyforge.raft.RaftMessage.AppendEntries;
import io.keyforge.raft.RaftMessage.AppendEntriesResponse;
import io.keyforge.raft.RaftMessage.InstallSnapshot;
import io.keyforge.raft.RaftMessage.InstallSnapshotResponse;
import io.keyforge.raft.RaftMessage.VoteRequest;
import io.keyforge.raft.RaftMessage.VoteResponse;

/**
 * Raft consensus as a pure, single-threaded state machine: {@link #handle(RaftEvent)} takes one event (a timer tick, a
 * message, a client request) and returns the {@link RaftAction}s the driver must carry out (persist, send, apply,
 * reply). The core performs no I/O, reads time only from the injected {@link Clock} and draws randomness only from the
 * injected generator, so the same events produce the same actions: the deterministic simulator relies on that.
 *
 * <p>Implemented: leader election with randomized timeouts; PreVote; leader stickiness and CheckQuorum; log replication
 * with the consistency check and conflict hints; commitment of entries of the leader's own term only (Raft paper,
 * figure 8), via a no-op each new leader appends; persistence of the term, vote and log before any message that depends
 * on them (see {@link RaftAction}); snapshots, log compaction and InstallSnapshot; and linearizable reads through
 * ReadIndex. Membership is fixed.
 *
 * <p>Not thread-safe: one thread owns a node.
 */
public final class RaftNode {

    private final RaftConfig config;
    private final String id;
    private final List<String> peers;
    private final int quorum;
    private final Clock clock;
    private final RandomGenerator random;

    // Persistent state (mirrored on disk through Persist actions).
    private long currentTerm;
    private String votedFor;
    private final RaftLog log;

    // Volatile state.
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadline;
    private long lastLeaderContact;
    private boolean heardFromLeader;

    // Candidate and pre-candidate state.
    private final Set<String> votes = new HashSet<>();

    // Leader state.
    private final Map<String, Progress> progress = new LinkedHashMap<>();
    private long heartbeatDue;
    private long quorumCheckDue;
    /** The latest leadership-confirmation round this leader has started; never reset. */
    private long sentRound;
    private final ArrayDeque<PendingRead> pendingReads = new ArrayDeque<>();
    /** Reads that arrived before this leader committed an entry of its own term. */
    private final List<Long> readsAwaitingTermCommit = new ArrayList<>();

    /** Actions produced by the event being handled. */
    private List<RaftAction> out = new ArrayList<>();

    /** Replication progress of one follower, as the leader sees it. */
    private static final class Progress {
        /** Next entry to send; advanced optimistically as batches are sent. */
        long next;
        /** Highest entry known to be replicated. */
        long match;
        /**
         * Probing: the follower's log position is unknown (new leader, rejection, lost message), so one batch at a time
         * until a success. Otherwise up to {@link RaftConfig#maxInflightAppends()} batches are pipelined.
         */
        boolean probing = true;
        /** Batches with entries awaiting acknowledgement, oldest first. */
        final ArrayDeque<Inflight> inflight = new ArrayDeque<>();
        boolean snapshotting;
        long snapshotSentAt;
        /** Heard from since the last quorum check. */
        boolean recentActive;
        /** Highest leadership-confirmation round the follower acknowledged. */
        long ackedRound;

        Progress(long next) {
            this.next = next;
        }
    }

    /** An AppendEntries batch ending at {@code lastIndex}, sent at {@code sentAt}. */
    private record Inflight(long lastIndex, long sentAt) {
    }

    private record PendingRead(long requestId, long readIndex, long round) {
    }

    /**
     * Restores a node from its durable state.
     *
     * @param appliedIndex the state machine's applied index: at least the snapshot index, at most the last log index.
     *                     Applied entries were committed, so the commit index starts there too
     */
    public RaftNode(RaftConfig config, Clock clock, RandomGenerator random, PersistentState state,
                    long appliedIndex) {
        this.config = Objects.requireNonNull(config, "config");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.random = Objects.requireNonNull(random, "random");
        this.id = config.nodeId();
        this.peers = config.peers();
        this.quorum = config.quorum();
        this.currentTerm = state.term();
        this.votedFor = state.votedFor();
        this.log = new RaftLog(state.snapshotIndex(), state.snapshotTerm(), state.entries());
        if (appliedIndex < state.snapshotIndex() || appliedIndex > log.lastIndex()) {
            throw new IllegalArgumentException("applied index " + appliedIndex + " is outside [" + state.snapshotIndex()
                    + ", " + log.lastIndex() + "]");
        }
        if (votedFor != null && !config.members().contains(votedFor)) {
            throw new IllegalArgumentException("voted for " + votedFor + ", who is not a member");
        }
        this.commitIndex = appliedIndex;
        this.lastApplied = appliedIndex;
        resetElectionDeadline();
    }

    /** A brand-new node with an empty log. */
    public static RaftNode create(RaftConfig config, Clock clock, RandomGenerator random) {
        return new RaftNode(config, clock, random, PersistentState.EMPTY, 0);
    }

    /** Handles one event and returns what the driver must do, in order (see {@link RaftAction}). */
    public List<RaftAction> handle(RaftEvent event) {
        out = new ArrayList<>();
        switch (event) {
            case Tick _ -> onTick();
            case Propose propose -> onPropose(propose);
            case ReadIndex read -> onReadIndex(read.requestId());
            case SnapshotTaken taken -> onSnapshotTaken(taken);
            case RaftMessage message -> onMessage(message);
        }
        return out;
    }

    public RaftStatus status() {
        return new RaftStatus(id, role, currentTerm, leaderId, commitIndex, lastApplied, log.lastIndex(),
                log.lastTerm(), log.snapshotIndex());
    }

    public Role role() {
        return role;
    }

    public long term() {
        return currentTerm;
    }

    public String leaderId() {
        return leaderId;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Timers

    private void onTick() {
        long now = clock.millis();
        if (role == Role.LEADER) {
            if (now >= quorumCheckDue) {
                quorumCheckDue = now + config.electionTimeoutMinMillis();
                if (config.checkQuorum() && !quorumActive()) {
                    // Partitioned away from the majority: stop accepting requests that can never commit.
                    becomeFollower(currentTerm, null);
                    return;
                }
            }
            if (now >= heartbeatDue) {
                heartbeatDue = now + config.heartbeatIntervalMillis();
                broadcastAppend(true);
            }
        } else if (now >= electionDeadline) {
            if (config.preVote()) {
                startPreVote();
            } else {
                startElection();
            }
        }
    }

    private boolean quorumActive() {
        int active = 1;
        for (Progress p : progress.values()) {
            if (p.recentActive) {
                active++;
            }
            p.recentActive = false;
        }
        return active >= quorum;
    }

    private void resetElectionDeadline() {
        long min = config.electionTimeoutMinMillis();
        long max = config.electionTimeoutMaxMillis();
        electionDeadline = clock.millis() + min + (max == min ? 0 : random.nextLong(max - min + 1));
    }

    /** Whether this node believes a leader is alive: it is the leader, or heard from one within the minimum timeout. */
    private boolean heardFromLeaderRecently() {
        if (role == Role.LEADER) {
            return true;
        }
        return leaderId != null && heardFromLeader
                && clock.millis() - lastLeaderContact < config.electionTimeoutMinMillis();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Elections

    private void startPreVote() {
        Role previous = role;
        role = Role.PRE_CANDIDATE;
        leaderId = null;
        leaveLeadership();
        votes.clear();
        votes.add(id);
        resetElectionDeadline();
        if (previous != Role.PRE_CANDIDATE) {
            out.add(new RoleChanged(role, currentTerm, null));
        }
        if (votes.size() >= quorum) {
            startElection();
            return;
        }
        for (String peer : peers) {
            send(new VoteRequest(id, peer, currentTerm + 1, log.lastIndex(), log.lastTerm(), true));
        }
    }

    private void startElection() {
        role = Role.CANDIDATE;
        currentTerm++;
        votedFor = id;
        persistState();
        leaderId = null;
        leaveLeadership();
        votes.clear();
        votes.add(id);
        resetElectionDeadline();
        out.add(new RoleChanged(role, currentTerm, null));
        if (votes.size() >= quorum) {
            becomeLeader();
            return;
        }
        for (String peer : peers) {
            send(new VoteRequest(id, peer, currentTerm, log.lastIndex(), log.lastTerm(), false));
        }
    }

    private void onVoteRequest(VoteRequest request) {
        if (request.preVote()) {
            // A pre-vote changes nothing here: it only asks whether a real election could succeed.
            boolean grant = request.term() > currentTerm && !heardFromLeaderRecently() && logUpToDate(request);
            send(new VoteResponse(id, request.from(), grant ? request.term() : currentTerm, grant, true));
            return;
        }
        if (request.term() > currentTerm) {
            if (config.checkQuorum() && heardFromLeaderRecently()) {
                // Leader stickiness: a live leader exists, so this candidate is disruptive. Keep the term.
                send(new VoteResponse(id, request.from(), currentTerm, false, false));
                return;
            }
            becomeFollower(request.term(), null);
        }
        boolean grant = request.term() == currentTerm
                && (votedFor == null || votedFor.equals(request.from()))
                && logUpToDate(request);
        if (grant) {
            votedFor = request.from();
            persistState();
            resetElectionDeadline();
        }
        send(new VoteResponse(id, request.from(), currentTerm, grant, false));
    }

    private boolean logUpToDate(VoteRequest request) {
        return request.lastLogTerm() > log.lastTerm()
                || (request.lastLogTerm() == log.lastTerm() && request.lastLogIndex() >= log.lastIndex());
    }

    private void onPreVoteResponse(VoteResponse response) {
        if (!response.granted()) {
            // A rejection carries the voter's term; a higher one means the cluster has moved on.
            if (response.term() > currentTerm) {
                becomeFollower(response.term(), null);
            }
            return;
        }
        if (role == Role.PRE_CANDIDATE && response.term() == currentTerm + 1) {
            votes.add(response.from());
            if (votes.size() >= quorum) {
                startElection();
            }
        }
    }

    private void onVoteResponse(VoteResponse response) {
        if (role != Role.CANDIDATE || !response.granted()) {
            return;
        }
        votes.add(response.from());
        if (votes.size() >= quorum) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        votes.clear();
        progress.clear();
        for (String peer : peers) {
            progress.put(peer, new Progress(log.lastIndex() + 1));
        }
        long now = clock.millis();
        heartbeatDue = now + config.heartbeatIntervalMillis();
        quorumCheckDue = now + config.electionTimeoutMinMillis();
        out.add(new RoleChanged(role, currentTerm, id));
        // Entries of earlier terms can only be committed through an entry of this term (figure 8); the no-op is that
        // entry, and committing it is also what lets ReadIndex serve reads.
        LogEntry noop = LogEntry.noop(log.lastIndex() + 1, currentTerm);
        log.append(noop);
        out.add(new AppendLog(List.of(noop)));
        broadcastAppend(false);
        maybeCommit();
    }

    private void becomeFollower(long term, String leader) {
        boolean changed = role != Role.FOLLOWER || term != currentTerm || !Objects.equals(leader, leaderId);
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            persistState();
        }
        role = Role.FOLLOWER;
        leaderId = leader;
        heardFromLeader = false;
        votes.clear();
        leaveLeadership();
        resetElectionDeadline();
        if (changed) {
            out.add(new RoleChanged(role, currentTerm, leader));
        }
    }

    /** Drops leader-only state; pending reads fail, since a stale leader may not serve them. */
    private void leaveLeadership() {
        progress.clear();
        for (PendingRead read : pendingReads) {
            out.add(new ReadRejected(read.requestId(), leaderId));
        }
        pendingReads.clear();
        for (long requestId : readsAwaitingTermCommit) {
            out.add(new ReadRejected(requestId, leaderId));
        }
        readsAwaitingTermCommit.clear();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Messages

    private void onMessage(RaftMessage message) {
        if (!message.to().equals(id) || !config.members().contains(message.from()) || message.from().equals(id)) {
            return;
        }
        if (message instanceof VoteRequest request) {
            onVoteRequest(request);
            return;
        }
        if (message instanceof VoteResponse response && response.preVote()) {
            onPreVoteResponse(response);
            return;
        }
        if (message.term() > currentTerm) {
            boolean fromLeader = message instanceof AppendEntries || message instanceof InstallSnapshot;
            becomeFollower(message.term(), fromLeader ? message.from() : null);
        } else if (message.term() < currentTerm) {
            // A stale leader learns the new term from the rejection and steps down.
            switch (message) {
                case AppendEntries append ->
                        send(new AppendEntriesResponse(id, append.from(), currentTerm, false, 0, 0, append.round()));
                case InstallSnapshot install ->
                        send(new InstallSnapshotResponse(id, install.from(), currentTerm, 0, install.round()));
                default -> {
                }
            }
            return;
        }
        switch (message) {
            case AppendEntries append -> onAppendEntries(append);
            case AppendEntriesResponse response -> onAppendEntriesResponse(response);
            case InstallSnapshot install -> onInstallSnapshot(install);
            case InstallSnapshotResponse response -> onInstallSnapshotResponse(response);
            case VoteResponse response -> onVoteResponse(response);
            case VoteRequest _ -> throw new AssertionError("handled above");
        }
    }

    /** Records contact with the leader of the current term, which sent an AppendEntries or InstallSnapshot. */
    private void acceptLeader(String leader) {
        if (role == Role.LEADER) {
            throw new IllegalStateException(id + " is leader of term " + currentTerm + " but " + leader
                    + " sent it entries for the same term");
        }
        if (role != Role.FOLLOWER) {
            becomeFollower(currentTerm, leader);
        } else if (!leader.equals(leaderId)) {
            leaderId = leader;
            out.add(new RoleChanged(role, currentTerm, leader));
        }
        heardFromLeader = true;
        lastLeaderContact = clock.millis();
        resetElectionDeadline();
    }

    private void onAppendEntries(AppendEntries request) {
        acceptLeader(request.from());
        long prevIndex = request.prevLogIndex();
        long prevTerm = request.prevLogTerm();
        List<LogEntry> entries = request.entries();
        if (prevIndex < log.snapshotIndex()) {
            // Everything up to the snapshot is committed, so it matches the leader's log: skip that part.
            int skip = (int) Math.min(entries.size(), log.snapshotIndex() - prevIndex);
            entries = entries.subList(skip, entries.size());
            if (skip < request.entries().size() || prevIndex + skip == log.snapshotIndex()) {
                prevIndex = log.snapshotIndex();
                prevTerm = log.snapshotTerm();
            } else {
                replyAppend(request, true, log.snapshotIndex(), 0);
                return;
            }
        }
        if (prevIndex > log.lastIndex()) {
            replyAppend(request, false, 0, log.lastIndex() + 1);
            return;
        }
        if (log.term(prevIndex) != prevTerm) {
            long hint = Math.max(log.firstIndexOfTermAt(prevIndex), commitIndex + 1);
            replyAppend(request, false, 0, hint);
            return;
        }
        List<LogEntry> appended = new ArrayList<>();
        long index = prevIndex + 1;
        for (LogEntry entry : entries) {
            if (entry.index() != index) {
                throw new IllegalStateException("non-contiguous entries from " + request.from() + ": expected index "
                        + index + ", got " + entry);
            }
            if (index <= log.lastIndex()) {
                if (log.term(index) == entry.term()) {
                    index++;
                    continue; // already have it (a duplicate or reordered message)
                }
                if (index <= commitIndex) {
                    throw new IllegalStateException(id + " would truncate committed entry " + index + " (commit "
                            + commitIndex + ") for " + entry + " from " + request.from());
                }
                log.truncateFrom(index);
                out.add(new TruncateLog(index));
            }
            log.append(entry);
            appended.add(entry);
            index++;
        }
        if (!appended.isEmpty()) {
            out.add(new AppendLog(appended));
        }
        long match = prevIndex + entries.size();
        // Only entries verified to match the leader may be committed, so cap at match, not at the end of our log.
        long newCommit = Math.min(request.leaderCommit(), match);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            applyCommitted();
        }
        replyAppend(request, true, match, 0);
    }

    private void replyAppend(AppendEntries request, boolean success, long matchIndex, long hintIndex) {
        send(new AppendEntriesResponse(id, request.from(), currentTerm, success, matchIndex, hintIndex,
                request.round()));
    }

    private void onAppendEntriesResponse(AppendEntriesResponse response) {
        Progress p = progress.get(response.from());
        if (role != Role.LEADER || p == null) {
            return;
        }
        p.recentActive = true;
        p.ackedRound = Math.max(p.ackedRound, response.round());
        if (response.success()) {
            if (response.matchIndex() > log.lastIndex()) {
                throw new IllegalStateException(response.from() + " claims to match index " + response.matchIndex()
                        + " but the leader's log ends at " + log.lastIndex());
            }
            p.match = Math.max(p.match, response.matchIndex());
            p.next = Math.max(p.next, p.match + 1);
            while (!p.inflight.isEmpty() && p.inflight.peekFirst().lastIndex() <= p.match) {
                p.inflight.removeFirst();
            }
            p.probing = false;
            if (p.match >= log.snapshotIndex()) {
                p.snapshotting = false;
            }
            maybeCommit();
        } else if (response.hintIndex() < p.next) {
            // The follower lacks or disagrees with the entry before something we sent: rewind and probe from its hint,
            // never below what is known to match. A stale rejection only costs a resend.
            p.next = Math.max(p.match + 1, response.hintIndex());
            p.inflight.clear();
            p.probing = true;
        }
        releaseReads();
        if (role == Role.LEADER) {
            sendAppend(response.from(), p, false);
        }
    }

    private void onInstallSnapshot(InstallSnapshot request) {
        acceptLeader(request.from());
        long index = request.lastIncludedIndex();
        long term = request.lastIncludedTerm();
        if (index <= commitIndex) {
            // Everything it holds is committed here already.
            replySnapshot(request, commitIndex);
            return;
        }
        if (index > log.snapshotIndex() && index <= log.lastIndex() && log.term(index) == term) {
            // The log already holds the snapshot's last entry, so it matches the leader's log up to there.
            commitIndex = index;
            applyCommitted();
            replySnapshot(request, index);
            return;
        }
        log.reset(index, term);
        commitIndex = index;
        lastApplied = index;
        out.add(new RestoreSnapshot(index, term, request.payload()));
        replySnapshot(request, index);
    }

    private void replySnapshot(InstallSnapshot request, long matchIndex) {
        send(new InstallSnapshotResponse(id, request.from(), currentTerm, matchIndex, request.round()));
    }

    private void onInstallSnapshotResponse(InstallSnapshotResponse response) {
        Progress p = progress.get(response.from());
        if (role != Role.LEADER || p == null) {
            return;
        }
        if (response.matchIndex() > log.lastIndex()) {
            throw new IllegalStateException(response.from() + " claims to match index " + response.matchIndex()
                    + " but the leader's log ends at " + log.lastIndex());
        }
        p.recentActive = true;
        p.ackedRound = Math.max(p.ackedRound, response.round());
        p.snapshotting = false;
        p.inflight.clear();
        p.match = Math.max(p.match, response.matchIndex());
        p.next = p.match + 1;
        p.probing = false;
        maybeCommit();
        releaseReads();
        if (role == Role.LEADER) {
            sendAppend(response.from(), p, false);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Replication (leader)

    private void broadcastAppend(boolean heartbeat) {
        for (Map.Entry<String, Progress> follower : progress.entrySet()) {
            sendAppend(follower.getKey(), follower.getValue(), heartbeat);
        }
    }

    /**
     * Sends the follower its next entries, as many batches as its window allows: one while probing, up to
     * {@link RaftConfig#maxInflightAppends()} once it is keeping up. Only acknowledgements of batches free the window,
     * so duplicated or stale responses never multiply the traffic. A batch unanswered for two heartbeat intervals is
     * presumed lost and replication restarts from the match index. A heartbeat that has no batch to send carries no
     * entries and points at the match index, which the follower is known to hold. A follower whose next entry was
     * compacted away gets the snapshot instead.
     */
    private void sendAppend(String peer, Progress p, boolean heartbeat) {
        long now = clock.millis();
        if (p.snapshotting && now - p.snapshotSentAt >= config.installSnapshotTimeoutMillis()) {
            p.snapshotting = false; // lost or failed: send it again
        }
        if (!p.inflight.isEmpty() && now - p.inflight.peekFirst().sentAt() >= 2 * config.heartbeatIntervalMillis()) {
            p.inflight.clear();
            p.next = p.match + 1;
            p.probing = true;
        }
        if (p.next <= log.snapshotIndex()) {
            if (!p.snapshotting) {
                out.add(new SendSnapshot(peer, currentTerm, log.snapshotIndex(), sentRound));
                p.snapshotting = true;
                p.snapshotSentAt = now;
            } else if (heartbeat) {
                // Keep the follower from timing out while the snapshot is in transit.
                send(new AppendEntries(id, peer, currentTerm, log.snapshotIndex(), log.snapshotTerm(), List.of(),
                        commitIndex, sentRound));
            }
            return;
        }
        boolean sent = false;
        int window = p.probing ? 1 : config.maxInflightAppends();
        while (p.next <= log.lastIndex() && p.inflight.size() < window) {
            long prevIndex = p.next - 1;
            List<LogEntry> entries =
                    log.sliceForAppend(p.next, config.maxEntriesPerAppend(), config.maxBytesPerAppend());
            send(new AppendEntries(id, peer, currentTerm, prevIndex, log.term(prevIndex), entries, commitIndex,
                    sentRound));
            long lastIndex = prevIndex + entries.size();
            p.inflight.addLast(new Inflight(lastIndex, now));
            p.next = lastIndex + 1;
            sent = true;
        }
        if (heartbeat && !sent) {
            long prevIndex = Math.max(p.match, log.snapshotIndex());
            send(new AppendEntries(id, peer, currentTerm, prevIndex, log.term(prevIndex), List.of(), commitIndex,
                    sentRound));
        }
    }

    /** Commits the highest index replicated on a majority, if it belongs to this leader's term (figure 8). */
    private void maybeCommit() {
        if (role != Role.LEADER) {
            return;
        }
        long[] matches = new long[peers.size() + 1];
        matches[0] = log.lastIndex();
        int i = 1;
        for (Progress p : progress.values()) {
            matches[i++] = p.match;
        }
        Arrays.sort(matches);
        long candidate = matches[matches.length - quorum];
        if (candidate > commitIndex && log.term(candidate) == currentTerm) {
            commitIndex = candidate;
            applyCommitted();
            if (!readsAwaitingTermCommit.isEmpty()) {
                List<Long> waiting = List.copyOf(readsAwaitingTermCommit);
                readsAwaitingTermCommit.clear();
                waiting.forEach(this::registerRead);
            }
        }
    }

    private void applyCommitted() {
        if (commitIndex > lastApplied) {
            out.add(new Apply(log.slice(lastApplied + 1, commitIndex + 1)));
            lastApplied = commitIndex;
        }
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Client requests

    private void onPropose(Propose propose) {
        if (role != Role.LEADER) {
            out.add(new ProposalRejected(propose.requestId(), leaderId));
            return;
        }
        LogEntry entry = LogEntry.command(log.lastIndex() + 1, currentTerm, propose.command());
        log.append(entry);
        out.add(new AppendLog(List.of(entry)));
        out.add(new ProposalAccepted(propose.requestId(), entry.index(), entry.term()));
        broadcastAppend(false);
        maybeCommit();
    }

    /**
     * ReadIndex (Raft thesis, section 6.4): record the commit index, then confirm with a heartbeat round that a
     * majority still follows this leader. Only then can no newer leader have committed anything this read would miss.
     */
    private void onReadIndex(long requestId) {
        if (role != Role.LEADER) {
            out.add(new ReadRejected(requestId, leaderId));
            return;
        }
        if (log.term(commitIndex) != currentTerm) {
            // Until its no-op commits, this leader may not know the latest commit index.
            readsAwaitingTermCommit.add(requestId);
            return;
        }
        registerRead(requestId);
    }

    private void registerRead(long requestId) {
        if (quorum == 1) {
            out.add(new ReadReady(requestId, commitIndex));
            return;
        }
        // The confirming round must start after this read arrived. Reads that arrive while a round is in flight wait
        // for the next one, so one round confirms a whole batch of reads.
        pendingReads.add(new PendingRead(requestId, commitIndex, sentRound + 1));
        if (quorumAckedRound() >= sentRound) {
            startReadRound();
        }
    }

    private void startReadRound() {
        sentRound++;
        broadcastAppend(true);
    }

    private void releaseReads() {
        if (pendingReads.isEmpty()) {
            return;
        }
        long acked = quorumAckedRound();
        while (!pendingReads.isEmpty() && pendingReads.peek().round() <= acked) {
            PendingRead read = pendingReads.poll();
            out.add(new ReadReady(read.requestId(), read.readIndex()));
        }
        if (!pendingReads.isEmpty() && acked >= sentRound) {
            startReadRound();
        }
    }

    /** The highest round a majority (this leader included) has acknowledged. */
    private long quorumAckedRound() {
        long[] rounds = new long[peers.size() + 1];
        rounds[0] = sentRound;
        int i = 1;
        for (Progress p : progress.values()) {
            rounds[i++] = p.ackedRound;
        }
        Arrays.sort(rounds);
        return rounds[rounds.length - quorum];
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Snapshots

    private void onSnapshotTaken(SnapshotTaken taken) {
        long index = taken.index();
        if (index <= log.snapshotIndex()) {
            return; // an older snapshot, or one superseded by an installed snapshot
        }
        if (index > lastApplied) {
            throw new IllegalArgumentException("snapshot at " + index + " is ahead of the applied index "
                    + lastApplied);
        }
        if (log.term(index) != taken.term()) {
            throw new IllegalArgumentException("snapshot at " + index + " has term " + taken.term()
                    + " but the log has term " + log.term(index));
        }
        log.compactThrough(index);
        out.add(new CompactLog(index, taken.term()));
    }

    // ---------------------------------------------------------------------------------------------------------------

    private void persistState() {
        out.add(new PersistState(currentTerm, votedFor));
    }

    private void send(RaftMessage message) {
        out.add(new Send(message));
    }
}
