package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;

import org.junit.jupiter.api.Test;

class FaultPlannerTest {

    private static final List<String> NODES = List.of("kf1", "kf2", "kf3", "kf4", "kf5");

    private static List<Fault> plan(Set<FaultKind> kinds, long seed, int count, Optional<String> leader) {
        FaultPlanner planner = new FaultPlanner(NODES, kinds, new SplittableRandom(seed));
        List<Fault> faults = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            faults.add(planner.next(leader));
        }
        return faults;
    }

    @Test
    void theSameSeedPlansTheSameFaults() {
        assertThat(plan(FaultKind.ALL, 7, 50, Optional.of("kf2")))
                .isEqualTo(plan(FaultKind.ALL, 7, 50, Optional.of("kf2")))
                .isNotEqualTo(plan(FaultKind.ALL, 8, 50, Optional.of("kf2")));
    }

    @Test
    void onlyEnabledKindsAreDrawnAndEveryEnabledKindIs() {
        List<Fault> faults = plan(EnumSet.of(FaultKind.KILL, FaultKind.LATENCY), 1, 200, Optional.empty());

        assertThat(faults).extracting(Fault::kind).containsOnly(FaultKind.KILL, FaultKind.LATENCY);
        assertThatThrownBy(() -> new FaultPlanner(NODES, Set.of(), new SplittableRandom(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void partitionsTakeEveryShapeAndAimAtTheLeaderWhenAsked() {
        List<Fault> faults = plan(EnumSet.of(FaultKind.PARTITION), 3, 300, Optional.of("kf4"));
        Set<String> shapes = new HashSet<>();

        for (Fault fault : faults) {
            Fault.Partition partition = (Fault.Partition) fault;
            shapes.add(partition.shape());
            assertThat(partition.grudges()).containsOnlyKeys(NODES);
            if (partition.shape().equals("isolate-leader")) {
                assertThat(partition.grudges().get("kf4")).hasSize(4);
                assertThat(partition.description()).startsWith("isolate-leader {kf4} | {");
            }
        }
        assertThat(shapes).containsExactlyInAnyOrder("isolate-node", "isolate-leader", "halves", "ring", "bridge");
    }

    @Test
    void processFaultsHitOneNodeTheLeaderAMinorityOrAMajority() {
        for (Fault fault : plan(EnumSet.of(FaultKind.KILL, FaultKind.PAUSE), 5, 300, Optional.of("kf1"))) {
            String target = fault instanceof Fault.Kill kill ? kill.target() : ((Fault.Pause) fault).target();
            List<String> nodes = fault instanceof Fault.Kill kill ? kill.nodes() : ((Fault.Pause) fault).nodes();
            assertThat(nodes).doesNotHaveDuplicates().isSubsetOf(NODES);
            switch (target) {
                case "one" -> assertThat(nodes).hasSize(1);
                case "leader" -> assertThat(nodes).containsExactly("kf1");
                case "minority" -> assertThat(nodes).hasSize(2);
                case "majority" -> assertThat(nodes).hasSize(3);
                default -> throw new AssertionError(target);
            }
        }
    }

    @Test
    void networkFaultsStayWithinTheirRanges() {
        for (Fault fault : plan(EnumSet.of(FaultKind.LATENCY, FaultKind.LOSS), 9, 300, Optional.empty())) {
            switch (fault) {
                case Fault.Latency latency -> {
                    assertThat(latency.delayMillis()).isBetween(20, 300);
                    assertThat(latency.jitterMillis()).isEqualTo(latency.delayMillis() / 2);
                    assertThat(latency.nodes()).isNotEmpty().isSubsetOf(NODES);
                }
                case Fault.Loss loss -> {
                    assertThat(loss.percent()).isBetween(5, 25);
                    assertThat(loss.nodes()).isNotEmpty().isSubsetOf(NODES);
                }
                default -> throw new AssertionError(fault);
            }
        }
    }

    @Test
    void aWipeTakesAMajority() {
        Fault.Wipe wipe = (Fault.Wipe) plan(EnumSet.of(FaultKind.WIPE), 1, 1, Optional.empty()).getFirst();

        assertThat(wipe.wiped()).hasSize(3).doesNotHaveDuplicates().isSubsetOf(NODES);
    }
}
