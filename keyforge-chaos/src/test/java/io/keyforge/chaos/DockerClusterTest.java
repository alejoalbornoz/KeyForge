package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

class DockerClusterTest {

    /** Records every command and answers with {@code responder}. */
    private static final class RecordingShell implements Shell {

        final List<String> commands = Collections.synchronizedList(new ArrayList<>());
        private final Function<List<String>, Result> responder;

        RecordingShell(Function<List<String>, Result> responder) {
            this.responder = responder;
        }

        @Override
        public Result run(List<String> command, Duration timeout) {
            commands.add(String.join(" ", command));
            return responder.apply(command);
        }
    }

    private final RecordingShell shell = new RecordingShell(command -> new Shell.Result(0, ""));
    private final DockerCluster cluster = new DockerCluster(shell, nodes(), "keyforge-chaos-net:latest");

    private static Map<String, DockerCluster.Node> nodes() {
        Map<String, DockerCluster.Node> nodes = new LinkedHashMap<>();
        for (int i = 1; i <= 3; i++) {
            nodes.put("kf" + i, new DockerCluster.Node("kf" + i, "server" + i, "net" + i, "10.0.0." + i));
        }
        return nodes;
    }

    @Test
    void partitionsDropPacketsFromThePeersEachNodeHolds() {
        Map<String, Set<String>> grudges = new LinkedHashMap<>();
        grudges.put("kf1", Set.of("kf2"));
        grudges.put("kf2", Set.of("kf1"));
        grudges.put("kf3", Set.of());

        cluster.partition(grudges);

        assertThat(shell.commands).containsExactlyInAnyOrder(
                "docker exec net1 sh -c iptables -A INPUT -s 10.0.0.2 -j DROP",
                "docker exec net2 sh -c iptables -A INPUT -s 10.0.0.1 -j DROP");
    }

    @Test
    void latencyAddsAToxicInBothDirections() {
        cluster.delay(List.of("kf2"), 120, 60);

        assertThat(shell.commands).containsExactly("docker exec net2 sh -c "
                + "toxiproxy-cli toxic add -t latency -n latency-up -u -a latency=120 -a jitter=60 raft >/dev/null"
                + " && toxiproxy-cli toxic add -t latency -n latency-down -d -a latency=120 -a jitter=60 raft"
                + " >/dev/null");
    }

    @Test
    void packetLossDropsARandomShareFromEveryPeer() {
        cluster.drop(List.of("kf3"), 15);

        assertThat(shell.commands).containsExactly("docker exec net3 sh -c "
                + "iptables -A INPUT -s 10.0.0.1 -m statistic --mode random --probability 0.1500 -j DROP"
                + " && iptables -A INPUT -s 10.0.0.2 -m statistic --mode random --probability 0.1500 -j DROP");
    }

    @Test
    void healingTheNetworkFlushesRulesAndRemovesToxicsEverywhere() {
        cluster.healNetwork();

        assertThat(shell.commands).hasSize(3).allMatch(command -> command.contains("iptables -F INPUT")
                && command.contains("toxic remove -n latency-up raft")
                && command.contains("toxic remove -n latency-down raft"));
    }

    @Test
    void processFaultsSignalTheServerContainers() {
        cluster.kill(List.of("kf1", "kf3"));
        cluster.pause(List.of("kf2"));
        cluster.resume(List.of("kf2"));
        cluster.start(List.of("kf1", "kf3"));
        cluster.start(List.of());

        assertThat(shell.commands).containsExactly("docker kill -s KILL server1 server3",
                "docker kill -s STOP server2", "docker kill -s CONT server2", "docker start server1 server3");
    }

    @Test
    void wipingDeletesTheDataThroughTheServersVolumes() {
        cluster.wipe(List.of("kf2"));

        assertThat(shell.commands).containsExactly("docker run --rm --volumes-from server2 --entrypoint sh "
                + "keyforge-chaos-net:latest -c find /data -mindepth 1 -delete");
    }

    @Test
    void failuresAreReportedWithTheCommandAndItsOutput() {
        RecordingShell broken = new RecordingShell(command -> new Shell.Result(1, "no such container"));
        DockerCluster failing = new DockerCluster(broken, nodes(), "image");

        assertThatThrownBy(() -> failing.kill(List.of("kf1")))
                .hasMessage("docker kill -s KILL server1 failed (exit 1): no such container");
        assertThatThrownBy(() -> failing.healNetwork()).hasMessageContaining("docker exec net");
        assertThatThrownBy(() -> failing.kill(List.of("kf9"))).hasMessageStartingWith("unknown node kf9");
    }

    @Test
    void healingEverythingResumesOnlyRunningServersAndNeverThrows() {
        RecordingShell partlyDown = new RecordingShell(command -> {
            if (command.contains("{{.State.Running}}")) {
                return new Shell.Result(0, command.getLast().equals("server2") ? "false" : "true");
            }
            return new Shell.Result(command.get(1).equals("exec") ? 1 : 0, "");
        });

        new DockerCluster(partlyDown, nodes(), "image").healEverything(List.of("kf1", "kf2", "kf3"));

        assertThat(partlyDown.commands).contains("docker kill -s CONT server1 server3",
                "docker start server1 server2 server3");
        assertThat(partlyDown.commands).anyMatch(command -> command.startsWith("docker exec net1 sh -c iptables -F"));
    }

    @Test
    void discoversContainersAndAddressesByComposeLabels() {
        RecordingShell docker = new RecordingShell(command -> {
            String joined = String.join(" ", command);
            if (joined.startsWith("docker ps")) {
                String service = joined.substring(joined.lastIndexOf('=') + 1);
                return new Shell.Result(0, "id-" + service + "\n");
            }
            if (joined.contains("{{.Config.Image}}")) {
                return new Shell.Result(0, "keyforge-chaos-net:latest\n");
            }
            return new Shell.Result(0, "172.18.0." + command.getLast().length() + "\n");
        });

        DockerCluster discovered = DockerCluster.discover(docker, "keyforge", List.of("kf1"));

        assertThat(discovered.nodes()).containsExactly(Map.entry("kf1",
                new DockerCluster.Node("kf1", "id-kf1", "id-kf1-net", "172.18.0.10")));
        assertThat(docker.commands).contains("docker ps -a -q --filter label=com.docker.compose.project=keyforge "
                + "--filter label=com.docker.compose.service=kf1-net");
    }

    @Test
    void discoveryNeedsExactlyOneContainerPerService() {
        RecordingShell none = new RecordingShell(command -> new Shell.Result(0, ""));

        assertThatThrownBy(() -> DockerCluster.discover(none, "keyforge", List.of("kf1")))
                .hasMessage("expected one container for service kf1 in project keyforge, found none");
    }
}
