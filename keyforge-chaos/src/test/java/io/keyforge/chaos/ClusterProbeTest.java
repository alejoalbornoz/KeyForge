package io.keyforge.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ClusterProbeTest {

    @Test
    void parsesRoleAndTermFromInfoRaft() {
        assertThat(ClusterProbe.parse("# Raft\r\nrole:leader\r\nterm:7\r\nleader_id:node-2\r\ncommit_index:3\r\n"))
                .isEqualTo(new ClusterProbe.Status("leader", 7));
        assertThat(ClusterProbe.parse("role:follower\nterm:2\n")).isEqualTo(new ClusterProbe.Status("follower", 2));
        assertThat(ClusterProbe.parse("# Server\r\nkeyforge_mode:standalone\r\n"))
                .isEqualTo(new ClusterProbe.Status("unknown", -1));
    }

    @Test
    void aNodeThatDoesNotAnswerIsDown() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        ClusterProbe probe = new ClusterProbe(Map.of("kf1", InetSocketAddress.createUnresolved("127.0.0.1", port)),
                Duration.ofMillis(200));

        assertThat(probe.statuses(List.of("kf1"))).containsEntry("kf1", ClusterProbe.Status.DOWN);
        assertThat(probe.leader()).isEmpty();
        assertThat(probe.awaitServing(Duration.ofMillis(300))).isFalse();
    }
}
