package io.keyforge.raft;

import java.util.List;
import java.util.Objects;

/**
 * Messages between Raft nodes. Every message carries the sender's current term. The transport may lose, delay,
 * reorder or duplicate them; the protocol tolerates all four.
 *
 * <p>AppendEntries and InstallSnapshot carry a {@code round}, echoed in their responses. It is the leader's
 * leadership-confirmation round for ReadIndex: a response echoing round {@code r} proves the follower still accepted
 * this leader after round {@code r} began.
 */
public sealed interface RaftMessage extends RaftEvent {

    String from();

    String to();

    long term();

    /**
     * RequestVote, or a pre-vote when {@code preVote} is set. A pre-vote proposes {@code term} (the sender's term plus
     * one) without anyone adopting it.
     */
    record VoteRequest(String from, String to, long term, long lastLogIndex, long lastLogTerm, boolean preVote)
            implements RaftMessage {
    }

    /**
     * Answer to a {@link VoteRequest}. A granted pre-vote carries the proposed term; any other answer carries the
     * voter's current term.
     */
    record VoteResponse(String from, String to, long term, boolean granted, boolean preVote) implements RaftMessage {
    }

    /** Log replication and heartbeat. */
    record AppendEntries(String from, String to, long term, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit, long round) implements RaftMessage {
        public AppendEntries {
            entries = List.copyOf(entries);
        }
    }

    /**
     * Answer to {@link AppendEntries}.
     *
     * @param matchIndex on success, the last index known to match the leader's log
     * @param hintIndex  on failure, where the leader should retry: the start of the conflicting term or the end of the
     *                   follower's log
     */
    record AppendEntriesResponse(String from, String to, long term, boolean success, long matchIndex, long hintIndex,
                                 long round) implements RaftMessage {
    }

    /** The leader's snapshot, for a follower whose missing entries were compacted away. */
    record InstallSnapshot(String from, String to, long term, long lastIncludedIndex, long lastIncludedTerm,
                           long round, SnapshotPayload payload) implements RaftMessage {
        public InstallSnapshot {
            Objects.requireNonNull(payload, "payload");
        }
    }

    /** Answer to {@link InstallSnapshot}: the follower's log now matches the leader's up to {@code matchIndex}. */
    record InstallSnapshotResponse(String from, String to, long term, long matchIndex, long round)
            implements RaftMessage {
    }
}
