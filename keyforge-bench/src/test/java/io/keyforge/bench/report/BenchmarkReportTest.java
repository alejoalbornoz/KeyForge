package io.keyforge.bench.report;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import io.keyforge.bench.RunResult;

class BenchmarkReportTest {

    @TempDir
    Path directory;

    @Test
    void writesAThroughputChartPerPipelineDepthALatencyChartPerRateAndASummary() throws Exception {
        Files.writeString(directory.resolve("redis-benchmark.csv"), RedisBenchmarkRow.CSV_HEADER + "\n"
                + "keyforge-lsm,1,50,1000,SET,50000,1,1,1,1,2,3,12.5\n"
                + "redis,1,50,1000,SET,150000,1,1,0.3,1,0.9,3,4.2\n"
                + "redis,1,50,1000,GET,160000,1,1,0.3,1,0.8,3,4.0\n"
                + "redis,16,50,1000,SET,900000,1,1,1,1,2,3,1.1\n"
                + "my-experiment,16,50,1000,SET,1000,1,1,1,1,2,3,99\n");
        Files.writeString(directory.resolve("latency.csv"), RunResult.CSV_HEADER + "\n"
                + run("redis", 20_000).toCsv() + "\n"
                + run("keyforge-lsm", 20_000).toCsv() + "\n"
                + run("cluster-leader", 1_000).toCsv() + "\n");
        histogram("redis-50c-20000rps.hgrm", 100);
        histogram("keyforge-lsm-50c-20000rps.hgrm", 300);
        // cluster-leader has no histogram file: its run is in the summary but not in a chart.

        List<Path> written = BenchmarkReport.write(directory);

        Path charts = directory.resolve("charts");
        assertThat(written).containsExactly(charts.resolve("throughput-pipeline-1.svg"),
                charts.resolve("throughput-pipeline-16.svg"), charts.resolve("latency-20000rps.svg"),
                charts.resolve("summary.md"));

        Document p1 = ChartsTest.parse(Files.readString(charts.resolve("throughput-pipeline-1.svg")));
        assertThat(texts(p1)).as("known targets in their fixed order, not in file order")
                .containsSubsequence("Redis", "KeyForge, LSM");
        assertThat(p1.getElementsByTagName("path").getLength()).as("redis SET, GET; LSM SET").isEqualTo(3);
        Document p16 = ChartsTest.parse(Files.readString(charts.resolve("throughput-pipeline-16.svg")));
        assertThat(texts(p16)).contains("redis-benchmark throughput, pipeline of 16", "my-experiment");

        Document latency = ChartsTest.parse(Files.readString(charts.resolve("latency-20000rps.svg")));
        assertThat(latency.getElementsByTagName("polyline").getLength()).isEqualTo(2);
        assertThat(texts(latency)).contains("Open-loop latency at 20,000 requests/s");

        String summary = Files.readString(charts.resolve("summary.md"));
        assertThat(summary)
                .contains("| Redis | 1 | 150,000 | 160,000 | 4.2 / 4.0 µs |")
                .contains("| KeyForge, LSM | 1 | 50,000 | – | 12.5 / – µs |")
                .contains("| my-experiment | 16 | 1,000 | – | 99.0 / – µs |")
                .contains("| Redis | 1 | 0.300 / 0.900 | 0.300 / 0.800 |")
                .contains("| KeyForge 3 nodes, leader | 1,000 | 19,999 | 0.150 | 0.990 | 2.00 | 5.00 | 9.00 | – | 0.75 "
                        + "| 3 |");
    }

    @Test
    void aDirectoryWithoutResultsIsAnError() {
        assertThatThrownBy(() -> BenchmarkReport.write(directory)).isInstanceOf(IOException.class)
                .hasMessageContaining("No results");
    }

    @Test
    void knownTargetsKeepTheirColorWhateverElseIsInTheReport() {
        BenchmarkReport.Targets targets = new BenchmarkReport.Targets();
        targets.add("other");
        targets.add("keyforge-lsm");
        targets.add("redis");

        assertThat(targets.names()).containsExactly("redis", "keyforge-lsm", "other");
        assertThat(targets.slot("redis")).isEqualTo(1);
        assertThat(targets.slot("keyforge-lsm")).isEqualTo(4);
        assertThat(targets.slot("other")).isEqualTo(8);
        assertThat(targets.displayName("cluster-follower")).isEqualTo("KeyForge 3 nodes, follower");
        assertThat(targets.displayName("other")).isEqualTo("other");
    }

    @Test
    void millisecondsKeepThreeSignificantDigits() {
        assertThat(BenchmarkReport.ms(123.4)).isEqualTo("0.123");
        assertThat(BenchmarkReport.ms(1_234)).isEqualTo("1.23");
        assertThat(BenchmarkReport.ms(12_345)).isEqualTo("12.3");
        assertThat(BenchmarkReport.ms(1_234_567)).isEqualTo("1,235");
    }

    private void histogram(String name, long baseMicros) throws IOException {
        Histogram histogram = new Histogram(3);
        for (long value = 1; value <= 1_000; value++) {
            histogram.recordValue(baseMicros * 1_000 + value * 1_000); // nanoseconds, like the load generator
        }
        try (PrintStream out = new PrintStream(Files.newOutputStream(directory.resolve(name)), false,
                StandardCharsets.UTF_8)) {
            histogram.outputPercentileDistribution(out, 1_000.0);
        }
    }

    private static RunResult run(String label, int rate) {
        return new RunResult(label, "epoll", 50, 50, 0.5, true, rate, 19_999.4, 600_000, 3, 0, 30,
                150, 400, 990, 2_000, 5_000, 9_000, 200, 50, 900, 10, 11, 12, -1, -1, 5, 6, 0.75, 1.5);
    }

    private static List<String> texts(Document svg) {
        NodeList nodes = svg.getElementsByTagName("text");
        return java.util.stream.IntStream.range(0, nodes.getLength())
                .mapToObj(i -> nodes.item(i).getTextContent()).toList();
    }
}
