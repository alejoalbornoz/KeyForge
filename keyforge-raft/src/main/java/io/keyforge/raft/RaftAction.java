package io.keyforge.raft;

import java.util.List;

/**
 * An output of {@link RaftNode#handle}: something the driver must do. The core performs no I/O itself.
 *
 * <p><b>Ordering contract.</b> Actions come in the order they must take effect. In particular, every {@link Persist}
 * action must be durable before any action that follows it is executed: a vote is only sent once the vote is on disk,
 * and an AppendEntries success only once the entries are. A driver may batch the persistence of several events and
 * sync once, as long as no later action (a send, an apply, a reply) runs before that sync.
 */
public sealed interface RaftAction {

    // ---------------------------------------------------------------------------------------------------------------
    // Persistence

    /** Actions that change durable state. */
    sealed interface Persist extends RaftAction permits PersistState, AppendLog, TruncateLog, CompactLog,
            RestoreSnapshot {
    }

    /** Store the current term and vote. */
    record PersistState(long term, String votedFor) implements Persist {
    }

    /** Append entries to the end of the log; the first follows the current last entry. */
    record AppendLog(List<LogEntry> entries) implements Persist {
        public AppendLog {
            entries = List.copyOf(entries);
        }
    }

    /** Remove every entry at or after {@code fromIndex}. Never removes a committed entry. */
    record TruncateLog(long fromIndex) implements Persist {
    }

    /**
     * Discard the log up to and including {@code snapshotIndex}, which the driver's latest snapshot (reported with
     * {@link RaftEvent.SnapshotTaken}) covers.
     */
    record CompactLog(long snapshotIndex, long snapshotTerm) implements Persist {
    }

    /**
     * Install a snapshot received from the leader: store it durably, discard the whole log (it now starts after
     * {@code index}), and reset the state machine from it. The state machine's applied index becomes {@code index}.
     */
    record RestoreSnapshot(long index, long term, SnapshotPayload payload) implements Persist {
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Network

    /** Send a message; delivery is best effort. */
    record Send(RaftMessage message) implements RaftAction {
    }

    /**
     * Send the driver's latest snapshot to {@code to} as an {@link RaftMessage.InstallSnapshot} with the given term and
     * round. The core knows the snapshot only by index; the driver holds the data.
     */
    record SendSnapshot(String to, long term, long snapshotIndex, long round) implements RaftAction {
    }

    // ---------------------------------------------------------------------------------------------------------------
    // State machine and replies

    /** Apply committed entries to the state machine, in order. */
    record Apply(List<LogEntry> entries) implements RaftAction {
        public Apply {
            entries = List.copyOf(entries);
        }
    }

    /** The command of {@link RaftEvent.Propose} was appended at {@code index} in {@code term}. */
    record ProposalAccepted(long requestId, long index, long term) implements RaftAction {
    }

    /** This node is not the leader; {@code leaderHint} is the leader it knows of, or {@code null}. */
    record ProposalRejected(long requestId, String leaderHint) implements RaftAction {
    }

    /**
     * The read of {@link RaftEvent.ReadIndex} may be served once the state machine has applied {@code readIndex}.
     */
    record ReadReady(long requestId, long readIndex) implements RaftAction {
    }

    /** The read cannot be served here: this node is not, or is no longer, the leader. */
    record ReadRejected(long requestId, String leaderHint) implements RaftAction {
    }

    /** Informational: the node's role, term or known leader changed. */
    record RoleChanged(Role role, long term, String leaderId) implements RaftAction {
    }
}
