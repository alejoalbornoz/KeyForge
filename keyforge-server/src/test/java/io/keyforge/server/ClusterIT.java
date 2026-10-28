package io.keyforge.server;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.keyforge.raft.grpc.PeerAddress;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

/**
 * A three-node cluster in one JVM: real RESP servers, real gRPC between the nodes, LSM stores in replicated mode, driven
 * with Jedis. Data commands may go to any node; followers forward them to the leader.
 */
@Timeout(120)
class ClusterIT {

    private static final List<String> IDS = List.of("node-1", "node-2", "node-3");

    @TempDir
    Path root;

    private final Map<String, PeerAddress> members = new LinkedHashMap<>();
    private final Map<String, KeyForgeNode> nodes = new LinkedHashMap<>();
    private final List<Jedis> clients = new ArrayList<>();
    private ServerType serverType = ServerType.VIRTUAL_THREADS;

    @AfterEach
    void stop() {
        clients.forEach(Jedis::close);
        nodes.values().forEach(KeyForgeNode::close);
    }

    private void startCluster() throws Exception {
        for (String id : IDS) {
            try (ServerSocket socket = new ServerSocket(0)) {
                members.put(id, new PeerAddress("127.0.0.1", socket.getLocalPort()));
            }
        }
        for (String id : IDS) {
            start(id);
        }
    }

    private KeyForgeNode start(String id) throws InterruptedException {
        KeyForgeConfig config = KeyForgeConfig.defaults()
                .withNodeId(id)
                .withPort(0)
                .withServerType(serverType)
                .withDataDirectory(root.resolve(id).resolve("store"))
                .withCluster(members, root.resolve(id).resolve("raft"))
                .withRaftTimings(Duration.ofMillis(300), Duration.ofMillis(50), 50, Duration.ofSeconds(10));
        KeyForgeNode node = KeyForgeNode.start(config);
        nodes.put(id, node);
        return node;
    }

    private Jedis client(KeyForgeNode node) {
        Jedis jedis = new Jedis("127.0.0.1", node.port(), 15_000);
        clients.add(jedis);
        return jedis;
    }

    private String leaderId() {
        await("a leader is elected", () -> nodes.values().stream().filter(node -> node.raft().isLeader()).count() == 1);
        return nodes.entrySet().stream().filter(entry -> entry.getValue().raft().isLeader()).findFirst().orElseThrow()
                .getKey();
    }

    private KeyForgeNode follower() {
        String leader = leaderId();
        return nodes.entrySet().stream().filter(entry -> !entry.getKey().equals(leader)).findFirst().orElseThrow()
                .getValue();
    }

    private static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
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

    @ParameterizedTest
    @EnumSource(ServerType.class)
    void everyNodeServesEveryCommand(ServerType type) throws Exception {
        serverType = type;
        startCluster();
        KeyForgeNode follower = follower();
        Jedis viaFollower = client(follower);
        Jedis viaLeader = client(nodes.get(leaderId()));

        assertThat(viaFollower.set("greeting", "hello")).isEqualTo("OK");
        for (KeyForgeNode node : nodes.values()) {
            assertThat(client(node).get("greeting")).isEqualTo("hello");
        }
        for (int i = 0; i < 10; i++) {
            (i % 2 == 0 ? viaFollower : viaLeader).incr("counter");
        }
        assertThat(viaFollower.get("counter")).isEqualTo("10");
        assertThat(viaFollower.set("greeting", "again", SetParams.setParams().nx())).isNull();
        assertThat(viaFollower.mset("a", "1", "b", "2")).isEqualTo("OK");
        assertThat(viaFollower.mget("a", "b", "missing")).containsExactly("1", "2", null);
        assertThat(viaFollower.exists("a", "b", "missing")).isEqualTo(2);
        assertThat(viaFollower.del("a", "b", "missing")).isEqualTo(2);
        assertThat(viaFollower.dbSize()).isEqualTo(2);

        // The leader turns PX into an absolute instant; the key disappears everywhere.
        assertThat(viaFollower.set("short", "lived", SetParams.setParams().px(300))).isEqualTo("OK");
        assertThat(viaLeader.get("short")).isEqualTo("lived");
        Thread.sleep(500);
        assertThat(viaFollower.get("short")).isNull();
    }

    @Test
    void integersErrorsAndInfoBehaveLikeStandalone() throws Exception {
        startCluster();
        Jedis viaFollower = client(follower());
        viaFollower.set("text", "abc");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> viaFollower.incr("text"))
                .hasMessageContaining("value is not an integer or out of range");
        String info = viaFollower.info("raft");
        assertThat(info).contains("role:follower").contains("leader_id:" + leaderId());
        assertThat(viaFollower.flushDB()).isEqualTo("OK");
        assertThat(viaFollower.dbSize()).isZero();
    }

    @Test
    void writesContinueAfterTheLeaderDiesAndItCatchesUpOnRestart() throws Exception {
        startCluster();
        String oldLeader = leaderId();
        Jedis viaFollower = client(follower());
        for (int i = 0; i < 300; i++) {
            viaFollower.set("before-" + i, Integer.toString(i));
        }

        nodes.remove(oldLeader).close();
        String newLeader = leaderId();
        assertThat(newLeader).isNotEqualTo(oldLeader);
        Jedis survivor = client(nodes.values().iterator().next());
        for (int i = 0; i < 300; i++) {
            survivor.set("after-" + i, Integer.toString(i));
        }
        assertThat(survivor.get("before-299")).isEqualTo("299");

        KeyForgeNode restarted = start(oldLeader);
        KeyForgeNode leader = nodes.get(leaderId());
        await("the old leader caught up", () ->
                restarted.raft().appliedIndex() >= leader.raft().status().commitIndex());
        Jedis viaRestarted = client(restarted);
        assertThat(viaRestarted.get("after-299")).isEqualTo("299");
        assertThat(viaRestarted.dbSize()).isEqualTo(600);
    }

    @Test
    void theRaftLogIsTheOnlyWriteAheadLog() throws Exception {
        startCluster();
        Jedis client = client(follower());
        for (int i = 0; i < 200; i++) {
            client.set("key-" + i, "value-" + i);
        }
        for (String id : IDS) {
            nodes.remove(id).close();
        }
        try (Stream<Path> files = Files.walk(root)) {
            assertThat(files.filter(path -> path.toString().endsWith(".wal"))).isEmpty();
        }
        for (String id : IDS) {
            start(id);
        }
        Jedis after = client(nodes.get(leaderId()));
        assertThat(after.dbSize()).isEqualTo(200);
        assertThat(after.get("key-199")).isEqualTo("value-199");
        assertThat(after.set("key-0", "rewritten")).isEqualTo("OK");
    }
}
