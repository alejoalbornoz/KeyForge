package io.keyforge.raft.sim;

/**
 * Outcome of one simulation run.
 *
 * @param traceHash a hash of every processed event; equal for two runs of the same seed
 */
public record SimulationResult(long seed, int nodes, long steps, long traceHash, int leadersElected,
                               int writesAcknowledged, int writesFailed, int readsServed, int snapshotsInstalled,
                               int crashes, long entriesApplied) {
}
