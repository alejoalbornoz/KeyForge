package io.keyforge.chaos;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The nemesis's hands: what it can do to the cluster's nodes, named by their service names. Implementations apply each
 * call to all the named nodes before returning, and throw if they could not.
 */
interface ClusterControl {

    /** Each node drops every packet from the peers listed for it. */
    void partition(Map<String, Set<String>> grudges);

    /** Delays every inbound Raft connection of {@code nodes}, in both directions. */
    void delay(Collection<String> nodes, int delayMillis, int jitterMillis);

    /** {@code nodes} drop {@code percent} % of the packets they receive from their peers. */
    void drop(Collection<String> nodes, int percent);

    /** Removes every partition, delay and packet loss. */
    void healNetwork();

    /** Kills the nodes' server processes with SIGKILL. */
    void kill(Collection<String> nodes);

    /** Starts the nodes' server processes again; a node that is running is left alone. */
    void start(Collection<String> nodes);

    /** Stops the nodes' server processes with SIGSTOP. */
    void pause(Collection<String> nodes);

    /** Resumes paused server processes with SIGCONT. */
    void resume(Collection<String> nodes);

    /** Deletes the data (LSM store and Raft log) of nodes whose server processes are not running. */
    void wipe(Collection<String> nodes);

    /**
     * Undoes everything, on a best-effort basis: resumes paused servers, starts stopped ones and heals the network.
     * Never throws.
     */
    void healEverything(Collection<String> nodes);
}
