package io.keyforge.raft.sim;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A quick slice of the deterministic simulation (the full 10,000-seed run is {@link RaftSimulationIT}), plus checks
 * that runs are reproducible and that the invariants catch real bugs.
 */
class RaftSimulationTest {

    @Test
    void fiveHundredSeedsKeepEveryInvariant() {
        SeedRunner.Batch batch = SeedRunner.run(1_000_000, 500);
        System.out.println("Simulation: " + batch.summary());
        batch.firstFailure().ifPresent(failure ->
                fail(failure.getMessage() + "\n" + Simulation.traceOf(failure.seed(), Simulation.Mutation.NONE),
                        failure));
        assertThat(batch.writes()).as("the workload makes progress").isPositive();
        assertThat(batch.reads()).isPositive();
        assertThat(batch.snapshotsInstalled()).as("InstallSnapshot is exercised").isPositive();
    }

    @Test
    void aSeedReplaysExactly() {
        for (long seed : new long[] {1, 42, 4711}) {
            SimulationResult first = Simulation.run(seed);
            SimulationResult second = Simulation.run(seed);
            assertThat(second).isEqualTo(first);
        }
        assertThat(Simulation.run(1).traceHash()).isNotEqualTo(Simulation.run(2).traceHash());
    }

    /** {@code -Dkeyforge.sim.trace=SEED} prints the last events of that seed's run. */
    @Test
    @EnabledIfSystemProperty(named = "keyforge.sim.trace", matches = "[0-9]+")
    void printTrace() {
        System.out.println(Simulation.traceOf(Long.getLong("keyforge.sim.trace"), Simulation.Mutation.NONE));
    }

    /** Each deliberate bug must break an invariant on some seed, or the checks would prove nothing. */
    @ParameterizedTest
    @EnumSource(value = Simulation.Mutation.class, names = "NONE", mode = EnumSource.Mode.EXCLUDE)
    void invariantsCatchDeliberateBugs(Simulation.Mutation mutation) {
        long caught = LongStream.range(1, 3_000).parallel()
                .filter(seed -> {
                    try {
                        Simulation.runMutated(seed, mutation);
                        return false;
                    } catch (SimulationFailure failure) {
                        return failure.getCause() instanceof InvariantViolation
                                || failure.getCause() instanceof IllegalStateException;
                    }
                })
                .count();
        System.out.println(mutation + " caught by " + caught + " of 2999 seeds");
        assertThat(caught).as("seeds on which %s violates an invariant", mutation).isPositive();
    }
}
