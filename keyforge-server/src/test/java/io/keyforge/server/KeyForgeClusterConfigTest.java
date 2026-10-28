package io.keyforge.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.keyforge.raft.RaftConfig;
import io.keyforge.raft.grpc.PeerAddress;

class KeyForgeClusterConfigTest {

    private static final String MEMBERS = "node-2=kf2:7000, node-1=kf1:7000,node-3=kf3:7001";

    @Test
    void aNodeIsStandaloneByDefault() {
        KeyForgeConfig config = KeyForgeConfig.defaults();
        assertThat(config.clustered()).isFalse();
        assertThat(config.clusterMembers()).isEmpty();
        assertThat(config.raftDataDirectory()).isEqualTo(Path.of("raft"));
        assertThat(config.electionTimeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.heartbeatInterval()).isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void clusterMembersComeFromTheEnvironmentLikeEveryProperty() {
        KeyForgeConfig config = KeyForgeConfig.load(null,
                Map.of("KEYFORGE_CLUSTER_MEMBERS", MEMBERS, "KEYFORGE_RAFT_DATA_DIR", "/data/raft"),
                Map.of("node.id", "node-2", "raft.election-timeout-ms", "500"));
        assertThat(config.clustered()).isTrue();
        assertThat(config.clusterMembers()).containsExactly(
                Map.entry("node-1", new PeerAddress("kf1", 7000)),
                Map.entry("node-2", new PeerAddress("kf2", 7000)),
                Map.entry("node-3", new PeerAddress("kf3", 7001)));
        assertThat(config.raftDataDirectory()).isEqualTo(Path.of("/data/raft"));
        assertThat(config.toProperties()).containsEntry(KeyForgeConfig.CLUSTER_MEMBERS,
                "node-1=kf1:7000,node-2=kf2:7000,node-3=kf3:7001");
        assertThat(KeyForgeConfig.load(null, Map.of(), config.toProperties())).isEqualTo(config);

        RaftConfig raft = KeyForgeNode.raftConfig(config);
        assertThat(raft.members()).containsExactly("node-1", "node-2", "node-3");
        assertThat(raft.electionTimeoutMinMillis()).isEqualTo(500);
        assertThat(raft.electionTimeoutMaxMillis()).isEqualTo(1000);
    }

    @Test
    void invalidClusterSettingsAreRejected() {
        assertThatThrownBy(() -> load(Map.of("node.id", "node-9", "cluster.members", MEMBERS)))
                .hasMessageContaining("must include this node, 'node-9'");
        assertThatThrownBy(() -> load(Map.of("node.id", "node-1", "cluster.members", MEMBERS,
                "store.engine", "memory"))).hasMessageContaining("must be 'lsm' in a cluster");
        assertThatThrownBy(() -> load(Map.of("cluster.members", "node-1=kf1")))
                .hasMessageContaining("expected host:port");
        assertThatThrownBy(() -> load(Map.of("cluster.members", "node-1=a:1,node-1=b:2")))
                .hasMessageContaining("lists 'node-1' twice");
        assertThatThrownBy(() -> load(Map.of("raft.election-timeout-ms", "100", "raft.heartbeat-interval-ms", "100")))
                .hasMessageContaining("must be longer than");
    }

    private static KeyForgeConfig load(Map<String, String> overrides) {
        return KeyForgeConfig.load(null, Map.of(), overrides);
    }
}
