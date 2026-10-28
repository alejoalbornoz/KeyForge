package io.keyforge.raft.sim;

/** A simulation run failed; the message names the seed that reproduces it. */
public final class SimulationFailure extends RuntimeException {

    private final long seed;

    SimulationFailure(long seed, Object options, long time, long steps, Throwable cause) {
        super("seed " + seed + " failed at " + time + " ms (step " + steps + "): " + cause.getMessage()
                + "\n  options: " + options
                + "\n  reproduce: ./mvnw -pl keyforge-raft verify -Dit.test=RaftSimulationIT -Dkeyforge.sim.seed=" + seed,
                cause);
        this.seed = seed;
    }

    public long seed() {
        return seed;
    }
}
