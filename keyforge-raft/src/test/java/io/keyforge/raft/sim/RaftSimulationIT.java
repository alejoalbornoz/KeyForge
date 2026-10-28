package io.keyforge.raft.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.junit.jupiter.api.Test;

/**
 * The Phase 4 acceptance run: 10,000 seeds of the deterministic simulation without a single invariant violation.
 *
 * <p>{@code -Dkeyforge.sim.seeds=N} changes the count; {@code -Dkeyforge.sim.seed=S} replays one seed (a failure
 * message prints the exact command), with the trace of its last events.
 */
class RaftSimulationIT {

    @Test
    void tenThousandSeedsKeepEveryInvariant() {
        long count = Long.getLong("keyforge.sim.seeds", 10_000);
        long started = System.nanoTime();
        SeedRunner.Batch batch = SeedRunner.run(1, count);
        System.out.printf("Simulation: %s in %.1f s%n", batch.summary(), (System.nanoTime() - started) / 1e9);
        batch.firstFailure().ifPresent(failure ->
                fail(failure.getMessage() + "\n" + Simulation.traceOf(failure.seed(), Simulation.Mutation.NONE),
                        failure));
        assertThat(batch.runs()).isEqualTo(System.getProperty("keyforge.sim.seed") != null ? 1 : count);
    }
}
