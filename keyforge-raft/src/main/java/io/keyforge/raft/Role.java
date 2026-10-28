package io.keyforge.raft;

/** The role a Raft node currently plays. */
public enum Role {
    FOLLOWER,
    /** Asking for pre-votes without having incremented its term (PreVote). */
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
