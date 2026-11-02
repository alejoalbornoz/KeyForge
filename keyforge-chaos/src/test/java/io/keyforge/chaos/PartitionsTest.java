package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import net.jqwik.api.constraints.UniqueElements;

class PartitionsTest {

    private static final List<String> FIVE = List.of("kf1", "kf2", "kf3", "kf4", "kf5");

    @Property
    void everyShapeIsSymmetricAndCoversEveryNode(
            @ForAll @Size(min = 1, max = 9) @UniqueElements
            List<@AlphaChars @StringLength(min = 1, max = 4) String> nodes) {
        for (Map<String, Set<String>> grudges : List.of(Partitions.halves(nodes), Partitions.ring(nodes),
                Partitions.bridge(nodes), Partitions.isolate(nodes.getFirst(), nodes))) {
            assertThat(grudges).containsOnlyKeys(nodes);
            grudges.forEach((node, dropped) -> {
                assertThat(dropped).doesNotContain(node).isSubsetOf(nodes);
                dropped.forEach(peer -> assertThat(grudges.get(peer)).contains(node));
            });
        }
    }

    @Property
    void halvesPutTheSmallerHalfFirst(
            @ForAll @Size(min = 2, max = 9) @UniqueElements
            List<@AlphaChars @StringLength(min = 1, max = 4) String> nodes) {
        Map<String, Set<String>> grudges = Partitions.halves(nodes);
        int minority = nodes.size() / 2;

        for (String node : nodes.subList(0, minority)) {
            assertThat(grudges.get(node)).containsExactlyInAnyOrderElementsOf(nodes.subList(minority, nodes.size()));
        }
        for (String node : nodes.subList(minority, nodes.size())) {
            assertThat(grudges.get(node)).containsExactlyInAnyOrderElementsOf(nodes.subList(0, minority));
        }
        // With an odd number of nodes the second half is a strict majority: 2 | 3 in the five-node cluster.
        assertThat(nodes.size() - minority).isEqualTo((nodes.size() + 1) / 2);
    }

    @Property
    void aRingLetsEveryNodeReachOnlyItsNeighbours(
            @ForAll @Size(min = 3, max = 9) @UniqueElements
            List<@AlphaChars @StringLength(min = 1, max = 4) String> nodes) {
        Map<String, Set<String>> grudges = Partitions.ring(nodes);

        for (int i = 0; i < nodes.size(); i++) {
            String left = nodes.get(Math.floorMod(i - 1, nodes.size()));
            String right = nodes.get((i + 1) % nodes.size());
            assertThat(grudges.get(nodes.get(i))).doesNotContain(left, right).hasSize(nodes.size() - 3);
        }
    }

    @Test
    void inAFiveNodeRingEveryNodeSeesADifferentMajority() {
        Map<String, Set<String>> grudges = Partitions.ring(FIVE);

        assertThat(grudges.get("kf1")).containsExactlyInAnyOrder("kf3", "kf4");
        assertThat(grudges.get("kf3")).containsExactlyInAnyOrder("kf1", "kf5");
        assertThat(grudges.values()).allMatch(dropped -> dropped.size() == 2);
    }

    @Test
    void aBridgeNodeJoinsTwoHalvesThatCannotReachEachOther() {
        Map<String, Set<String>> grudges = Partitions.bridge(FIVE);

        assertThat(grudges.get("kf3")).as("the bridge").isEmpty();
        assertThat(grudges.get("kf1")).containsExactlyInAnyOrder("kf4", "kf5");
        assertThat(grudges.get("kf2")).containsExactlyInAnyOrder("kf4", "kf5");
        assertThat(grudges.get("kf4")).containsExactlyInAnyOrder("kf1", "kf2");
    }

    @Test
    void isolatingANodeCutsItOffFromEveryone() {
        Map<String, Set<String>> grudges = Partitions.isolate("kf2", FIVE);

        assertThat(grudges.get("kf2")).containsExactlyInAnyOrder("kf1", "kf3", "kf4", "kf5");
        assertThat(grudges.get("kf4")).containsExactly("kf2");
        assertThat(Partitions.describeComponents(List.of(List.of("kf2"), List.of("kf1", "kf3"))))
                .isEqualTo("{kf2} | {kf1,kf3}");
    }
}
