package io.keyforge.raft;

import java.util.List;
import java.util.Objects;

/**
 * Static configuration of one Raft node. Membership is fixed: changes are out of scope.
 *
 * @param nodeId                       this node's id; must be one of {@code members}
 * @param members                      every voting member, this node included, sorted and without duplicates
 * @param electionTimeoutMinMillis     shortest election timeout; also the leader-stickiness window (see
 *                                     {@link RaftNode})
 * @param electionTimeoutMaxMillis     longest election timeout; each timeout is drawn uniformly from [min, max]
 * @param heartbeatIntervalMillis      how often a leader sends AppendEntries to every follower; well below the
 *                                     election timeout. An AppendEntries unanswered for two intervals is resent
 * @param maxEntriesPerAppend          most log entries in one AppendEntries message
 * @param maxBytesPerAppend            AppendEntries stops adding entries once their data passes this size (at least one
 *                                     entry is always sent)
 * @param maxInflightAppends           AppendEntries messages with entries a leader keeps in flight to a follower that is
 *                                     keeping up (a follower being probed gets one at a time)
 * @param installSnapshotTimeoutMillis a leader resends a snapshot that has not been acknowledged after this long; the
 *                                     transport never runs two transfers to one follower at once, so a long transfer
 *                                     is not duplicated
 * @param preVote                      run a pre-vote round before incrementing the term (Raft thesis, section 9.6)
 * @param checkQuorum                  a leader steps down when it has not heard from a majority for an election timeout,
 *                                     and followers that heard from a leader recently ignore vote requests
 */
public record RaftConfig(
        String nodeId,
        List<String> members,
        long electionTimeoutMinMillis,
        long electionTimeoutMaxMillis,
        long heartbeatIntervalMillis,
        int maxEntriesPerAppend,
        long maxBytesPerAppend,
        int maxInflightAppends,
        long installSnapshotTimeoutMillis,
        boolean preVote,
        boolean checkQuorum) {

    public static final long DEFAULT_ELECTION_TIMEOUT_MIN_MILLIS = 1000;
    public static final long DEFAULT_ELECTION_TIMEOUT_MAX_MILLIS = 2000;
    public static final long DEFAULT_HEARTBEAT_INTERVAL_MILLIS = 100;
    public static final int DEFAULT_MAX_ENTRIES_PER_APPEND = 1024;
    public static final long DEFAULT_MAX_BYTES_PER_APPEND = 1L << 20;
    public static final int DEFAULT_MAX_INFLIGHT_APPENDS = 8;
    public static final long DEFAULT_INSTALL_SNAPSHOT_TIMEOUT_MILLIS = 5_000;

    public RaftConfig {
        Objects.requireNonNull(nodeId, "nodeId");
        members = List.copyOf(members);
        if (!members.contains(nodeId)) {
            throw new IllegalArgumentException("members " + members + " do not include this node, " + nodeId);
        }
        if (!members.equals(members.stream().sorted().distinct().toList())) {
            throw new IllegalArgumentException("members must be sorted and distinct: " + members);
        }
        if (electionTimeoutMinMillis < 1 || electionTimeoutMaxMillis < electionTimeoutMinMillis) {
            throw new IllegalArgumentException("election timeout must satisfy 1 <= min <= max, got ["
                    + electionTimeoutMinMillis + ", " + electionTimeoutMaxMillis + "]");
        }
        if (heartbeatIntervalMillis < 1 || heartbeatIntervalMillis >= electionTimeoutMinMillis) {
            throw new IllegalArgumentException("heartbeat interval must be positive and below the election timeout");
        }
        if (maxEntriesPerAppend < 1 || maxBytesPerAppend < 1 || maxInflightAppends < 1) {
            throw new IllegalArgumentException("AppendEntries limits must be positive");
        }
        if (installSnapshotTimeoutMillis < 1) {
            throw new IllegalArgumentException("installSnapshotTimeoutMillis must be positive");
        }
    }

    /** A configuration with the default timeouts and limits, PreVote and CheckQuorum on. */
    public static RaftConfig of(String nodeId, List<String> members) {
        return new RaftConfig(nodeId, members.stream().sorted().toList(), DEFAULT_ELECTION_TIMEOUT_MIN_MILLIS,
                DEFAULT_ELECTION_TIMEOUT_MAX_MILLIS, DEFAULT_HEARTBEAT_INTERVAL_MILLIS, DEFAULT_MAX_ENTRIES_PER_APPEND,
                DEFAULT_MAX_BYTES_PER_APPEND, DEFAULT_MAX_INFLIGHT_APPENDS, DEFAULT_INSTALL_SNAPSHOT_TIMEOUT_MILLIS,
                true, true);
    }

    public RaftConfig withTimeouts(long newElectionMin, long newElectionMax, long newHeartbeat) {
        return new RaftConfig(nodeId, members, newElectionMin, newElectionMax, newHeartbeat, maxEntriesPerAppend,
                maxBytesPerAppend, maxInflightAppends, installSnapshotTimeoutMillis, preVote, checkQuorum);
    }

    public RaftConfig withAppendLimits(int newMaxEntries, long newMaxBytes, int newMaxInflight) {
        return new RaftConfig(nodeId, members, electionTimeoutMinMillis, electionTimeoutMaxMillis,
                heartbeatIntervalMillis, newMaxEntries, newMaxBytes, newMaxInflight, installSnapshotTimeoutMillis,
                preVote, checkQuorum);
    }

    public RaftConfig withInstallSnapshotTimeout(long newTimeout) {
        return new RaftConfig(nodeId, members, electionTimeoutMinMillis, electionTimeoutMaxMillis,
                heartbeatIntervalMillis, maxEntriesPerAppend, maxBytesPerAppend, maxInflightAppends, newTimeout,
                preVote, checkQuorum);
    }

    public RaftConfig withPreVote(boolean enabled) {
        return new RaftConfig(nodeId, members, electionTimeoutMinMillis, electionTimeoutMaxMillis,
                heartbeatIntervalMillis, maxEntriesPerAppend, maxBytesPerAppend, maxInflightAppends,
                installSnapshotTimeoutMillis, enabled, checkQuorum);
    }

    public RaftConfig withCheckQuorum(boolean enabled) {
        return new RaftConfig(nodeId, members, electionTimeoutMinMillis, electionTimeoutMaxMillis,
                heartbeatIntervalMillis, maxEntriesPerAppend, maxBytesPerAppend, maxInflightAppends,
                installSnapshotTimeoutMillis, preVote, enabled);
    }

    /** The other members, in member order. */
    public List<String> peers() {
        return members.stream().filter(member -> !member.equals(nodeId)).toList();
    }

    /** Votes or acknowledgements needed for a majority. */
    public int quorum() {
        return members.size() / 2 + 1;
    }
}
