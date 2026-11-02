package io.keyforge.chaos;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;

/** What the nemesis needs to know about leadership: who leads now, and when some nodes have elected a leader. */
interface Leadership {

    /** The current leader, if a node claims to lead. */
    Optional<String> leader();

    /** Waits until one of {@code nodes} leads; returns whether one did within {@code patience}. */
    boolean awaitLeader(Collection<String> nodes, Duration patience) throws InterruptedException;
}
