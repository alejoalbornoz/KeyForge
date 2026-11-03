package io.keyforge.server.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.keyforge.server.KeyForgeConfig;
import io.keyforge.server.KeyForgeNode;
import io.keyforge.server.TestDirectories;
import redis.clients.jedis.Jedis;

/** A real node with {@code metrics.port} set: commands go in over RESP, their meters come out of {@code /metrics}. */
class MetricsEndpointIT {

    private final HttpClient http = HttpClient.newHttpClient();
    private Path dataDirectory;
    private KeyForgeNode node;

    @AfterEach
    void stop() throws IOException {
        if (node != null) {
            node.close();
        }
        if (dataDirectory != null) {
            TestDirectories.deleteRecursively(dataDirectory);
        }
    }

    @Test
    void servesCommandLsmAndJvmMetricsTaggedWithTheNodeId() throws Exception {
        node = startNode();
        try (Jedis jedis = new Jedis("127.0.0.1", node.port())) {
            jedis.set("k", "1");
            jedis.get("k");
            jedis.incr("k");
            jedis.set("k", "x");
            assertThatThrownBy(() -> jedis.incr("k")).hasMessageContaining("not an integer");
        }
        node.compact();

        HttpResponse<String> response = get("/metrics");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.headers().firstValue("Content-Type")).hasValue(MetricsEndpoint.CONTENT_TYPE);
        String body = response.body();
        assertThat(sample(body, "keyforge_commands_seconds_count{command=\"set\",node=\"metrics-node\"}")).isEqualTo(2);
        assertThat(sample(body, "keyforge_commands_seconds_count{command=\"incr\",node=\"metrics-node\"}"))
                .isEqualTo(2);
        assertThat(sample(body, "keyforge_command_errors_total{command=\"incr\",node=\"metrics-node\"}")).isEqualTo(1);
        assertThat(sample(body, "keyforge_commands_seconds_bucket{command=\"get\",node=\"metrics-node\",le=\"+Inf\"}"))
                .isEqualTo(1);
        assertThat(sample(body, "keyforge_lsm_flushes_total{node=\"metrics-node\"}")).isPositive();
        assertThat(sample(body, "keyforge_lsm_sstables{node=\"metrics-node\"}")).isEqualTo(1);
        assertThat(sample(body, "keyforge_lsm_sstable_bytes{node=\"metrics-node\"}")).isPositive();
        assertThat(body).contains("keyforge_lsm_compactions_total{node=\"metrics-node\"}")
                .contains("jvm_memory_used_bytes{area=\"heap\"")
                .contains("jvm_threads_live_threads{node=\"metrics-node\"}");
        assertThat(body).as("no Raft meters on a standalone node").doesNotContain("keyforge_raft_");
    }

    @Test
    void answersOnlyGetAndHeadOnTheMetricsPath() throws Exception {
        node = startNode();

        assertThat(get("/").statusCode()).isEqualTo(404);
        assertThat(get("/metrics/x").statusCode()).isEqualTo(404);
        HttpResponse<String> post = http.send(HttpRequest.newBuilder(uri("/metrics"))
                .POST(HttpRequest.BodyPublishers.ofString("")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(post.statusCode()).isEqualTo(405);
        assertThat(post.headers().firstValue("Allow")).hasValue("GET, HEAD");
        HttpResponse<String> head = http.send(HttpRequest.newBuilder(uri("/metrics"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(head.statusCode()).isEqualTo(200);
    }

    @Test
    void closingTheNodeStopsTheEndpoint() throws Exception {
        node = startNode();
        int port = node.metricsPort();
        assertThat(get("/metrics").statusCode()).isEqualTo(200);

        node.close();
        node = null;

        assertThatThrownBy(() -> http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics"))
                .build(), HttpResponse.BodyHandlers.ofString())).isInstanceOf(IOException.class);
    }

    @Test
    void aNodeWithoutAMetricsPortServesNothing() throws Exception {
        dataDirectory = Files.createTempDirectory("keyforge-metrics");
        node = KeyForgeNode.start(KeyForgeConfig.load(null, Map.of(), Map.of(
                KeyForgeConfig.SERVER_BIND, "127.0.0.1",
                KeyForgeConfig.SERVER_PORT, "0",
                KeyForgeConfig.STORE_DATA_DIR, dataDirectory.toString())));

        assertThat(node.metricsPort()).isZero();
    }

    private KeyForgeNode startNode() throws Exception {
        dataDirectory = Files.createTempDirectory("keyforge-metrics");
        return KeyForgeNode.start(KeyForgeConfig.load(null, Map.of(), Map.of(
                KeyForgeConfig.NODE_ID, "metrics-node",
                KeyForgeConfig.SERVER_BIND, "127.0.0.1",
                KeyForgeConfig.SERVER_PORT, "0",
                KeyForgeConfig.STORE_DATA_DIR, dataDirectory.toString(),
                KeyForgeConfig.METRICS_BIND, "127.0.0.1",
                KeyForgeConfig.METRICS_PORT, Integer.toString(freePort()))));
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + node.metricsPort() + path);
    }

    /** The value of one series in a Prometheus text-format scrape, such as {@code name{label="x"}}. */
    static double sample(String scrape, String series) {
        return scrape.lines().filter(line -> line.startsWith(series + " ")).findFirst()
                .map(line -> Double.parseDouble(line.substring(series.length() + 1).split(" ")[0]))
                .orElseThrow(() -> new AssertionError("No series " + series + " in:\n" + scrape));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
