package io.keyforge.bench.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.keyforge.bench.RunResult;

/** Reading back what {@code compare-with-redis.sh}, redis-benchmark and the load generator write. */
class ResultFilesTest {

    @TempDir
    Path directory;

    @Test
    void redisBenchmarkRowsAreReadAfterTheHeader() throws IOException {
        Path csv = directory.resolve("redis-benchmark.csv");
        Files.writeString(csv, RedisBenchmarkRow.CSV_HEADER + "\n"
                + "redis,16,50,1000000,SET,1234567.89,0.583,0.104,0.551,0.831,1.031,3.207,1.64\n"
                + "\n"
                + "keyforge-lsm,1,50,1000000,get,98039.22,0.287,0.088,0.271,0.415,0.551,2.215,\n");

        List<RedisBenchmarkRow> rows = RedisBenchmarkRow.read(csv);

        assertThat(rows).hasSize(2);
        RedisBenchmarkRow first = rows.getFirst();
        assertThat(first.target()).isEqualTo("redis");
        assertThat(first.pipeline()).isEqualTo(16);
        assertThat(first.clients()).isEqualTo(50);
        assertThat(first.requests()).isEqualTo(1_000_000);
        assertThat(first.test()).isEqualTo("SET");
        assertThat(first.rps()).isEqualTo(1234567.89);
        assertThat(first.p50Ms()).isEqualTo(0.551);
        assertThat(first.p99Ms()).isEqualTo(1.031);
        assertThat(first.serverCpuUsPerRequest()).isEqualTo(1.64);
        assertThat(rows.get(1).test()).as("command names are upper-cased").isEqualTo("GET");
        assertThat(rows.get(1).serverCpuUsPerRequest()).as("an empty field is NaN").isNaN();
    }

    @Test
    void aRedisBenchmarkFileWithoutTheHeaderOrWithMissingFieldsIsRejected() throws IOException {
        Path csv = directory.resolve("redis-benchmark.csv");
        Files.writeString(csv, "redis,1,50,100,SET,1,1,1,1,1,1,1,1\n");
        assertThatThrownBy(() -> RedisBenchmarkRow.read(csv)).isInstanceOf(IOException.class)
                .hasMessageContaining("header");

        assertThatThrownBy(() -> RedisBenchmarkRow.parse("redis,1,50,100,SET,1"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("13 fields");
    }

    @Test
    void latencyRunsRoundTripThroughTheLoadGeneratorsCsvAndSkipClosedLoopRows() throws IOException {
        Path csv = directory.resolve("latency.csv");
        Files.writeString(csv, RunResult.CSV_HEADER + "\n"
                + result("redis", true, 20_000).toCsv() + "\n"
                + result("redis", false, 0).toCsv() + "\n");

        List<LatencyRun> runs = LatencyRun.read(csv);

        assertThat(runs).hasSize(1);
        LatencyRun run = runs.getFirst();
        assertThat(run.target()).isEqualTo("redis");
        assertThat(run.connections()).as("requested, not established").isEqualTo(50);
        assertThat(run.targetRate()).isEqualTo(20_000);
        assertThat(run.achievedRate()).isEqualTo(19_999.5);
        assertThat(run.errors()).isEqualTo(3);
        assertThat(run.p50Us()).isEqualTo(150.5);
        assertThat(run.p99Us()).isEqualTo(990.0);
        assertThat(run.p999Us()).isEqualTo(2_000.0);
        assertThat(run.p9999Us()).isEqualTo(5_000.0);
        assertThat(run.maxUs()).isEqualTo(9_000.0);
        assertThat(run.serverCpuCores()).isEqualTo(0.75);
        assertThat(run.histogramFileName()).isEqualTo("redis-50c-20000rps.hgrm");
        assertThat(run.logFileName()).isEqualTo("redis-50c-20000rps.log");
    }

    @Test
    void theMedianPerSecondP99ComesFromTheMeasuredSecondsOnly() {
        List<String> output = List.of(
                "[redis] warmup    1s      9000 req/s  p50     169 us  p99    99999 us  p99.9     5415 us",
                "[redis] measure   1s     10036 req/s  p50     169 us  p99      623 us  p99.9     5415 us  | x",
                "[redis] measure   2s      9999 req/s  p50     176 us  p99    10789 us  p99.9    19644 us",
                "[redis] measure   3s     10001 req/s  p50     181 us  p99      500 us  p99.9    36078 us",
                "[redis] cannot measure the server heap: jcmd 1 GC.run failed",
                "[redis] measure   4s     10001 req/s  p50     181 us  p99      700 us  p99.9    36078 us");

        assertThat(LatencyRun.medianSecondP99(output)).isEqualTo(661.5);
        assertThat(LatencyRun.medianSecondP99(output.subList(0, 4))).isEqualTo(623);
        assertThat(LatencyRun.medianSecondP99(List.of("nothing measured"))).isNaN();
    }

    @Test
    void anEmptyLatencyFileHasNoRuns() throws IOException {
        Path csv = directory.resolve("latency.csv");
        Files.writeString(csv, "");

        assertThat(LatencyRun.read(csv)).isEmpty();
    }

    @Test
    void percentileDistributionsAreReadFromHdrHistogramsOwnOutput() {
        Histogram histogram = new Histogram(3);
        for (int value = 1; value <= 10_000; value++) {
            histogram.recordValue(value);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        histogram.outputPercentileDistribution(new PrintStream(bytes, true, StandardCharsets.UTF_8), 1.0);
        List<String> lines = bytes.toString(StandardCharsets.UTF_8).lines().toList();

        PercentileDistribution distribution = PercentileDistribution.parse(lines);

        assertThat(distribution.points()).hasSizeGreaterThan(50);
        assertThat(distribution.points().getFirst().percentile()).isZero();
        assertThat(distribution.points().getLast().percentile()).as("the last line is the highest value")
                .isGreaterThan(0.999);
        assertThat(distribution.max()).isCloseTo(10_000, within(10.0));
        assertThat(PercentileChart.valueAt(distribution, 0.5)).isCloseTo(5_000, within(10.0));
        assertThat(distribution.points()).isSortedAccordingTo(
                (a, b) -> Double.compare(a.percentile(), b.percentile()));
        assertThat(distribution.scaled(0.001).max()).isCloseTo(10, within(0.01));
    }

    @Test
    void aFileWithoutPercentileLinesIsRejected() {
        assertThatThrownBy(() -> PercentileDistribution.parse(List.of("       Value     Percentile", "#[Mean = 1]")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static RunResult result(String label, boolean openLoop, int rate) {
        return new RunResult(label, "epoll", 50, 48, 0.5, openLoop, rate, 19_999.5, 600_000, 3, 0, 30,
                150.5, 400, 990, 2_000, 5_000, 9_000, 200, 50, 900, 10, 11, 12, -1, -1, 5, 6, 0.75, 1.5);
    }
}
