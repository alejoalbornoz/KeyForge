package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class NemesisTest {

    private static final List<String> NODES = List.of("kf1", "kf2", "kf3");

    private final List<String> calls = Collections.synchronizedList(new ArrayList<>());

    /** Records what the nemesis asks for in {@link #calls}; the calls starting with {@code failing} fail. */
    private final class FakeCluster implements ClusterControl, Leadership {

        private final String failing;

        FakeCluster(String failing) {
            this.failing = failing;
        }

        private void call(String what) {
            calls.add(what);
            if (!failing.isEmpty() && what.startsWith(failing)) {
                throw new IllegalStateException("docker is unhappy");
            }
        }

        @Override
        public void partition(Map<String, Set<String>> grudges) {
            call("partition");
        }

        @Override
        public void delay(Collection<String> nodes, int delayMillis, int jitterMillis) {
            call("delay " + nodes);
        }

        @Override
        public void drop(Collection<String> nodes, int percent) {
            call("drop " + nodes);
        }

        @Override
        public void healNetwork() {
            call("heal-network");
        }

        @Override
        public void kill(Collection<String> nodes) {
            call("kill " + nodes);
        }

        @Override
        public void start(Collection<String> nodes) {
            call("start " + nodes);
        }

        @Override
        public void pause(Collection<String> nodes) {
            call("pause " + nodes);
        }

        @Override
        public void resume(Collection<String> nodes) {
            call("resume " + nodes);
        }

        @Override
        public void wipe(Collection<String> nodes) {
            call("wipe " + nodes);
        }

        @Override
        public void healEverything(Collection<String> nodes) {
            calls.add("heal-everything " + nodes);
        }

        @Override
        public Optional<String> leader() {
            return Optional.of("kf1");
        }

        @Override
        public boolean awaitLeader(Collection<String> nodes, Duration patience) {
            calls.add("await-leader " + nodes);
            return true;
        }
    }

    private Nemesis nemesis(FakeCluster cluster, History history, Set<FaultKind> kinds, Duration fault) {
        return new Nemesis(cluster, cluster, new FaultPlanner(NODES, kinds, new SplittableRandom(1)), history, NODES,
                fault, Duration.ofMillis(20), new SplittableRandom(2));
    }

    @Test
    void injectsAndHealsFaultsOneAtATimeAndRecordsEveryAction() throws InterruptedException {
        History history = new History();
        Nemesis nemesis = nemesis(new FakeCluster(""), history, EnumSet.of(FaultKind.PARTITION, FaultKind.KILL),
                Duration.ofMillis(40));

        nemesis.start();
        Thread.sleep(1_000);
        nemesis.stop();

        assertThat(nemesis.faults()).isGreaterThan(3);
        List<HistoryEvent.Nemesis> actions = history.events().stream()
                .map(event -> (HistoryEvent.Nemesis) event).toList();
        // Invoke and complete the start, then the stop: the last fault is healed before the nemesis stops.
        assertThat(actions).hasSize(4 * nemesis.faults());
        for (int i = 0; i < actions.size(); i += 4) {
            assertThat(actions.subList(i, i + 4)).extracting(HistoryEvent.Nemesis::type, HistoryEvent.Nemesis::function)
                    .containsExactly(tuple(EventType.INVOKE, "start"), tuple(EventType.INFO, "start"),
                            tuple(EventType.INVOKE, "stop"), tuple(EventType.INFO, "stop"));
            assertThat(actions.subList(i, i + 4)).extracting(HistoryEvent.Nemesis::fault).containsOnly(
                    actions.get(i).fault());
        }
        // Each injection is followed by its own healing, and everything is healed at the end.
        List<String> recorded = List.copyOf(calls);
        assertThat(recorded.getLast()).isEqualTo("heal-everything " + NODES);
        for (int i = 0; i + 1 < recorded.size() - 1; i += 2) {
            String injected = recorded.get(i);
            String healed = recorded.get(i + 1);
            if (injected.equals("partition")) {
                assertThat(healed).isEqualTo("heal-network");
            } else {
                assertThat(injected).startsWith("kill ");
                assertThat(healed).isEqualTo("start " + injected.substring("kill ".length()));
            }
        }
    }

    @Test
    void aFaultThatCannotBeInjectedIsRecordedAndStillHealed() throws InterruptedException {
        History history = new History();
        Nemesis nemesis = nemesis(new FakeCluster("pause"), history, EnumSet.of(FaultKind.PAUSE),
                Duration.ofMillis(40));

        nemesis.start();
        Thread.sleep(400);
        nemesis.stop();

        assertThat(nemesis.faults()).isGreaterThan(1);
        assertThat(history.events()).map(event -> (HistoryEvent.Nemesis) event)
                .filteredOn(action -> action.type() == EventType.INFO && action.function().equals("start"))
                .allMatch(action -> action.description().endsWith("(failed: docker is unhappy)"));
        assertThat(calls).anyMatch(call -> call.startsWith("resume "));
    }

    @Test
    void aWipeKillsEveryNodeAndRestartsTheWipedMajorityFirst() throws InterruptedException {
        Nemesis nemesis = nemesis(new FakeCluster(""), new History(), EnumSet.of(FaultKind.WIPE),
                Duration.ofSeconds(10));

        nemesis.start();
        Thread.sleep(300);
        nemesis.stop();

        List<String> recorded = List.copyOf(calls);
        assertThat(recorded.getFirst()).isEqualTo("kill " + NODES);
        String wiped = recorded.get(1).substring("wipe ".length());
        assertThat(wiped).matches("\\[kf\\d, kf\\d]");
        assertThat(recorded.subList(1, 6)).containsExactly("wipe " + wiped, "start " + wiped, "await-leader " + wiped,
                "start " + NODES.stream().filter(node -> !wiped.contains(node)).toList(),
                // Healing a wipe makes sure everything runs.
                "start " + NODES);
        assertThat(recorded.getLast()).isEqualTo("heal-everything " + NODES);
    }
}
