package io.keyforge.bench;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.keyforge.server.KeyForgeConfig;
import io.keyforge.server.KeyForgeNode;
import io.keyforge.server.ServerType;

/** Short runs of the load generator against each real server, in-process. */
@Timeout(120)
class LoadGeneratorIT {

    @TempDir
    Path tempDir;

    private KeyForgeNode node;
    private ServerType serverType;
    private final ByteArrayOutputStream progress = new ByteArrayOutputStream();

    @AfterEach
    void stopNode() {
        if (node != null) {
            node.close();
        }
    }

    @ParameterizedTest
    @EnumSource(ServerType.class)
    void closedLoopRunMeasuresThroughputWithoutErrors(ServerType serverType) throws Exception {
        start(serverType);

        RunResult result = run("--connections=20", "--rate=0").result();

        assertThat(result.connected()).isEqualTo(20);
        assertThat(result.openLoop()).isFalse();
        assertThat(result.requests()).isPositive();
        assertThat(result.achievedRate()).isPositive();
        assertThat(result.errors()).isZero();
        assertThat(result.disconnects()).isZero();
        assertThat(result.p50()).isPositive().isLessThanOrEqualTo(result.p99());
        assertThat(result.p99()).isLessThanOrEqualTo(result.max());
        assertThat(result.label()).isEqualTo(serverType.configName());
        assertThat(progress.toString(StandardCharsets.UTF_8)).contains("measure");
    }

    @ParameterizedTest
    @EnumSource(ServerType.class)
    void openLoopRunDeliversTheOfferedRate(ServerType serverType) throws Exception {
        start(serverType);

        RunResult result = run("--connections=20", "--rate=2000").result();

        assertThat(result.openLoop()).isTrue();
        assertThat(result.targetRate()).isEqualTo(2000);
        assertThat(result.achievedRate()).isBetween(1500.0, 2500.0);
        assertThat(result.errors()).isZero();
    }

    @ParameterizedTest
    @EnumSource(value = ServerType.class, names = "VIRTUAL_THREADS")
    void resultsAreAppendedToACsvFileWithPercentilesAlongside(ServerType serverType) throws Exception {
        start(serverType);
        Path csv = tempDir.resolve("results").resolve("runs.csv");

        LoadGenerator.main(new String[] {"--port=" + node.port(), "--connections=5", "--rate=500",
                "--warmup-seconds=0", "--duration-seconds=1", "--settle-seconds=0", "--keyspace=100",
                "--label=vt", "--output=" + csv});
        LoadGenerator.main(new String[] {"--port=" + node.port(), "--connections=5", "--rate=0",
                "--warmup-seconds=0", "--duration-seconds=1", "--settle-seconds=0", "--keyspace=100",
                "--label=vt", "--output=" + csv});

        List<String> lines = Files.readAllLines(csv);
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0)).isEqualTo(RunResult.CSV_HEADER);
        assertThat(lines.get(1)).startsWith("vt,").contains(",open,500,");
        assertThat(lines.get(2)).startsWith("vt,").contains(",closed,0,");
        assertThat(Files.readString(csv.resolveSibling("vt-5c-500rps.hgrm"))).contains("Percentile");
        assertThat(csv.resolveSibling("vt-5c-max.hgrm")).exists();
    }

    private void start(ServerType type) throws InterruptedException {
        serverType = type;
        node = KeyForgeNode.start(KeyForgeConfig.load(null, Map.of(), Map.of(
                KeyForgeConfig.SERVER_BIND, "127.0.0.1",
                KeyForgeConfig.SERVER_PORT, "0",
                KeyForgeConfig.SERVER_TYPE, type.configName(),
                KeyForgeConfig.STORE_DATA_DIR, tempDir.resolve("data-" + type.configName()).toString())));
    }

    private LoadGenerator.Measurement run(String... extra) throws IOException, InterruptedException {
        String[] common = {"--port=" + node.port(), "--warmup-seconds=1", "--duration-seconds=2",
                "--settle-seconds=0", "--keyspace=1000", "--threads=2",
                "--label=" + serverType.configName()};
        String[] args = new String[common.length + extra.length];
        System.arraycopy(common, 0, args, 0, common.length);
        System.arraycopy(extra, 0, args, common.length, extra.length);
        return new LoadGenerator(BenchOptions.parse(args), new PrintStream(progress, true, StandardCharsets.UTF_8))
                .run();
    }
}
