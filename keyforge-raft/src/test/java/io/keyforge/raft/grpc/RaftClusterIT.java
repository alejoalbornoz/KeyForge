package io.keyforge.raft.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.raft.Clock;
import io.keyforge.raft.RaftConfig;
import io.keyforge.raft.runtime.NotLeaderException;
import io.keyforge.raft.runtime.RaftRuntime;
import io.keyforge.raft.storage.FileRaftStorage;

/** Three {@link RaftRuntime}s talking real gRPC on localhost, each with its own durable storage. */
class RaftClusterIT {

    private static final List<String> MEMBERS = List.of("n1", "n2", "n3");

    @TempDir
    Path root;

    private final Map<String, Integer> ports = new LinkedHashMap<>();
    private final Map<String, Node> nodes = new LinkedHashMap<>();

    private record Node(RaftRuntime runtime, GrpcTransport transport, RecordingStateMachine machine) {
    }

    @AfterEach
    void stopAll() {
        nodes.values().forEach(node -> node.runtime().close());
    }

    private void startAll() throws IOException {
        for (String id : MEMBERS) {
            try (ServerSocket socket = new ServerSocket(0)) {
                ports.put(id, socket.getLocalPort());
            }
        }
        for (String id : MEMBERS) {
            start(id);
        }
    }

    private Node start(String id) throws IOException {
        Map<String, PeerAddress> peers = new LinkedHashMap<>();
        ports.forEach((peer, port) -> {
            if (!peer.equals(id)) {
                peers.put(peer, new PeerAddress("127.0.0.1", port));
            }
        });
        Path directory = root.resolve(id);
        GrpcTransport transport = new GrpcTransport(id, "127.0.0.1", ports.get(id), peers, directory.resolve("spool"));
        RecordingStateMachine machine = new RecordingStateMachine();
        RaftConfig config = RaftConfig.of(id, MEMBERS).withTimeouts(300, 600, 50);
        RaftRuntime runtime = RaftRuntime.start(config, Clock.system(), transport, FileRaftStorage.open(directory),
                machine, new RaftRuntime.Options(5, 50));
        transport.setForwardHandler((from, request) -> {
            try {
                Object result = runtime.propose(request).get(5, TimeUnit.SECONDS);
                return new ForwardResult.Reply(result.toString().getBytes(StandardCharsets.UTF_8));
            } catch (ExecutionException e) {
                if (e.getCause() instanceof NotLeaderException notLeader) {
                    return new ForwardResult.NotLeader(notLeader.leaderHint());
                }
                throw new IllegalStateException(e.getCause());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        Node node = new Node(runtime, transport, machine);
        nodes.put(id, node);
        return node;
    }

    private Node leader() {
        await("a leader is elected", () -> nodes.values().stream().filter(node -> node.runtime().isLeader()).count()
                == 1);
        return nodes.values().stream().filter(node -> node.runtime().isLeader()).findFirst().orElseThrow();
    }

    private static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting until " + what);
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
    }

    private static List<String> commands(String prefix, int count) {
        List<String> commands = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            commands.add(prefix + i);
        }
        return commands;
    }

    private static void proposeAll(RaftRuntime leader, List<String> commands) throws Exception {
        List<CompletableFuture<Object>> futures = new ArrayList<>();
        for (String command : commands) {
            futures.add(leader.propose(command.getBytes(StandardCharsets.UTF_8)));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
    }

    @Test
    void replicatesServesReadsForwardsAndSurvivesALeaderCrash() throws Exception {
        startAll();
        Node leader = leader();
        List<String> expected = new ArrayList<>(commands("a", 200));
        proposeAll(leader.runtime(), expected);
        await("every node applied the first batch", () -> nodes.values().stream()
                .allMatch(node -> node.machine().applied().equals(expected)));

        // Linearizable reads on the leader; followers refuse them and name the leader.
        long readIndex = leader.runtime().readBarrier().get(5, TimeUnit.SECONDS);
        assertThat(readIndex).isGreaterThanOrEqualTo(leader.runtime().status().commitIndex() - 1);
        Node follower = nodes.values().stream().filter(node -> node != leader).findFirst().orElseThrow();
        String leaderId = leader.runtime().nodeId();
        assertThatThrownBy(() -> follower.runtime().readBarrier().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(NotLeaderException.class)
                .cause().extracting(cause -> ((NotLeaderException) cause).leaderHint()).isEqualTo(leaderId);

        // A follower forwards a request to the leader.
        ForwardResult forwarded = follower.transport()
                .forward(leaderId, "forwarded".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(5))
                .get(10, TimeUnit.SECONDS);
        assertThat(forwarded).isInstanceOf(ForwardResult.Reply.class);
        expected.add("forwarded");
        // Forwarding to a follower is refused with a hint.
        String other = MEMBERS.stream().filter(id -> !id.equals(leaderId) && !id.equals(follower.runtime().nodeId()))
                .findFirst().orElseThrow();
        assertThat(follower.transport().forward(other, new byte[] {1}, Duration.ofSeconds(5)).get(10,
                TimeUnit.SECONDS)).isEqualTo(new ForwardResult.NotLeader(leaderId));

        // Kill the leader: the others elect a new one and keep committing; snapshots compact their logs meanwhile.
        nodes.remove(leaderId).runtime().close();
        Node newLeader = leader();
        assertThat(newLeader.runtime().nodeId()).isNotEqualTo(leaderId);
        List<String> more = commands("b", 300);
        proposeAll(newLeader.runtime(), more);
        expected.addAll(more);
        await("the survivors applied everything", () -> nodes.values().stream()
                .allMatch(node -> node.machine().applied().equals(expected)));
        assertThat(newLeader.runtime().status().snapshotIndex()).as("the log was compacted").isPositive();

        // The old leader restarts from its directory and catches up, through InstallSnapshot.
        Node restarted = start(leaderId);
        await("the restarted node caught up", () -> restarted.machine().applied().equals(expected));
        assertThat(restarted.runtime().status().snapshotIndex()).isPositive();
    }

    @Test
    void aNodeRestartsFromItsLogAndSnapshot() throws Exception {
        startAll();
        proposeAll(leader().runtime(), commands("c", 120));
        await("every node applied", () -> nodes.values().stream()
                .allMatch(node -> node.machine().applied().size() == 120));
        for (String id : MEMBERS) {
            nodes.remove(id).runtime().close();
        }
        for (String id : MEMBERS) {
            start(id);
        }
        Node leader = leader();
        proposeAll(leader.runtime(), List.of("after-restart"));
        List<String> expected = new ArrayList<>(commands("c", 120));
        expected.add("after-restart");
        await("every node recovered the same history", () -> nodes.values().stream()
                .allMatch(node -> node.machine().applied().equals(expected)));
    }
}
