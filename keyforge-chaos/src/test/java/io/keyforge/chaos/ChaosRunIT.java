package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.raft.grpc.PeerAddress;
import io.keyforge.server.KeyForgeConfig;
import io.keyforge.server.KeyForgeNode;
import io.keyforge.server.ServerType;

/**
 * The whole run, workload and nemesis, against a three-node cluster of real KeyForge nodes in this JVM. The nemesis
 * kills nodes by closing them and restarts them on the same ports and directories; the Docker faults themselves are
 * exercised by {@code keyforge-chaos/scripts/linearizability.sh}, outside {@code mvn verify}.
 */
@Timeout(180)
class ChaosRunIT {

    private static final List<String> IDS = List.of("n1", "n2", "n3");

    @TempDir
    Path root;

    private final Map<String, PeerAddress> members = new LinkedHashMap<>();
    private final Map<String, Integer> respPorts = new LinkedHashMap<>();
    private final Map<String, KeyForgeNode> running = new HashMap<>();

    @AfterEach
    void stop() {
        running.values().forEach(KeyForgeNode::close);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private synchronized void startNode(String id) {
        if (running.containsKey(id)) {
            return;
        }
        KeyForgeConfig config = KeyForgeConfig.defaults()
                .withNodeId(id)
                .withPort(respPorts.get(id))
                .withServerType(ServerType.VIRTUAL_THREADS)
                .withDataDirectory(root.resolve(id).resolve("store"))
                .withCluster(members, root.resolve(id).resolve("raft"))
                .withRaftTimings(Duration.ofMillis(300), Duration.ofMillis(50), 200, Duration.ofSeconds(2));
        try {
            running.put(id, KeyForgeNode.start(config));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private synchronized void stopNode(String id) {
        KeyForgeNode node = running.remove(id);
        if (node != null) {
            node.close();
        }
    }

    /** Kills by closing a node and starts it again; the network faults have no in-process equivalent. */
    private final class InProcessCluster implements ClusterControl {

        @Override
        public void kill(Collection<String> nodes) {
            nodes.forEach(ChaosRunIT.this::stopNode);
        }

        @Override
        public void start(Collection<String> nodes) {
            nodes.forEach(ChaosRunIT.this::startNode);
        }

        @Override
        public void healEverything(Collection<String> nodes) {
            start(nodes);
        }

        @Override
        public void partition(Map<String, Set<String>> grudges) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delay(Collection<String> nodes, int delayMillis, int jitterMillis) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void drop(Collection<String> nodes, int percent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void healNetwork() {
        }

        @Override
        public void pause(Collection<String> nodes) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void resume(Collection<String> nodes) {
        }

        @Override
        public void wipe(Collection<String> nodes) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void everyOperationIsRecordedAndCompletedWhileNodesAreKilled() throws Exception {
        StringBuilder nodeList = new StringBuilder();
        for (String id : IDS) {
            members.put(id, new PeerAddress("127.0.0.1", freePort()));
            respPorts.put(id, freePort());
            nodeList.append(nodeList.isEmpty() ? "" : ",").append(id).append("=127.0.0.1:").append(respPorts.get(id));
        }
        IDS.forEach(this::startNode);
        Path output = root.resolve("results");
        ChaosOptions options = ChaosOptions.parse("--nodes=" + nodeList, "--duration-seconds=8", "--clients=6",
                "--rate=150", "--keys=3", "--ops-per-key=60", "--op-timeout-ms=4000", "--connect-timeout-ms=500",
                "--nemesis=kill", "--fault-seconds=2", "--calm-seconds=1", "--recovery-seconds=60", "--seed=11",
                "--output=" + output);

        ChaosRun.Result result = new ChaosRun(options, new InProcessCluster(),
                new ClusterProbe(options.nodes(), Duration.ofSeconds(1))).run();
        System.out.print(result.summary());

        assertThat(result.recovered()).isTrue();
        assertThat(output.resolve("history.json")).exists();
        assertThat(Files.readString(output.resolve("run.txt"))).contains("nemesis:     ").contains("kill");
        List<HistoryEvent> events = result.history().events();
        List<HistoryEvent.Op> ops = events.stream().filter(HistoryEvent.Op.class::isInstance)
                .map(HistoryEvent.Op.class::cast).toList();
        List<HistoryEvent.Nemesis> actions = events.stream().filter(HistoryEvent.Nemesis.class::isInstance)
                .map(HistoryEvent.Nemesis.class::cast).toList();

        // Faults were injected, and each was healed.
        assertThat(actions).isNotEmpty().hasSize(actions.size() / 4 * 4);
        assertThat(actions).extracting(HistoryEvent.Nemesis::fault).containsOnly(FaultKind.KILL);

        // Every invocation has exactly one completion, by the same process, before its next invocation; a process
        // never invokes again after an indeterminate completion.
        Map<Integer, HistoryEvent.Op> inFlight = new HashMap<>();
        Map<Integer, EventType> lastOutcome = new HashMap<>();
        for (HistoryEvent.Op op : ops) {
            if (op.type() == EventType.INVOKE) {
                assertThat(inFlight).doesNotContainKey(op.process());
                assertThat(lastOutcome.get(op.process())).isNotEqualTo(EventType.INFO);
                inFlight.put(op.process(), op);
            } else {
                HistoryEvent.Op invocation = inFlight.remove(op.process());
                assertThat(invocation).isNotNull();
                assertThat(op.key()).isEqualTo(invocation.key());
                assertThat(op.function()).isEqualTo(invocation.function());
                lastOutcome.put(op.process(), op.type());
            }
        }
        assertThat(inFlight).isEmpty();
        // Killing two of three nodes stops the cluster for a while; between kills it serves.
        assertThat(ops.stream().filter(op -> op.type() == EventType.OK).count()).isGreaterThan(50);

        // A light linearizability check: two successful increments of a key never return the same value.
        Map<String, List<Long>> increments = new HashMap<>();
        ops.stream().filter(op -> op.function() == OpFunction.INCR && op.type() == EventType.OK)
                .forEach(op -> increments.computeIfAbsent(op.key(), key -> new ArrayList<>()).add(op.value()));
        increments.values().forEach(values -> assertThat(values).doesNotHaveDuplicates());

        // The final reads, after every fault was healed, read every key successfully.
        long healed = actions.getLast().timeNanos();
        Set<String> finalReads = new java.util.HashSet<>();
        ops.stream().filter(op -> op.timeNanos() > healed && op.function() == OpFunction.GET
                && op.type() == EventType.OK).forEach(op -> finalReads.add(op.key()));
        Set<String> used = new java.util.HashSet<>();
        ops.forEach(op -> used.add(op.key()));
        assertThat(finalReads).containsAll(used);
    }
}
